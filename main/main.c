#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdbool.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "esp_system.h"
#include "esp_task_wdt.h"

/* ===============================
 * Identificação (obrigatória nos prints)
 * =============================== */
#define TAG_PREFIX "{Arthur Sales Guilherme-RM:86786} "
#define LOGF(fmt, ...) printf(TAG_PREFIX fmt, ##__VA_ARGS__)

/* ===============================
 * Parâmetros de sistema
 * =============================== */
enum {
    kPrioGen  = 6,
    kPrioRx   = 5,
    kPrioSup  = 4,
    kPrioLog  = 2
};

enum {
    kStkGen  = 4096,
    kStkRx   = 4096,
    kStkSup  = 4096,
    kStkLog  = 3072
};

#define Q_LEN                 10
#define Q_ITEM_SZ             (sizeof(int))

#define PERIOD_GEN_MS         150
#define PERIOD_SUP_MS         1500
#define RX_WAIT_MS            1000

#define RX_WARN_N             2
#define RX_RECOVER_SOFT_N     3
#define RX_RESET_Q_N          4
#define RX_GIVEUP_N           5

#define WDT_SECS              5

/* ===============================
 * Objetos globais
 * =============================== */
static QueueHandle_t qData = NULL;

static TaskHandle_t thGen = NULL;
static TaskHandle_t thRx  = NULL;
static TaskHandle_t thSup = NULL;
static TaskHandle_t thLog = NULL;

/* Heartbeats e flags de saúde */
static volatile TickType_t hbGen = 0;
static volatile TickType_t hbRx  = 0;
static volatile TickType_t hbSup = 0;

static volatile bool okGen = false;
static volatile bool okRx  = false;

/* ===============================
 * Utilidades
 * =============================== */
static inline TickType_t MS2T(uint32_t ms) { return pdMS_TO_TICKS(ms); }

static void feed_wdt_here(void) {
    /* Todas as tarefas críticas participam do WDT */
    esp_task_wdt_reset();
}

/* ===============================
 * Módulo 1 – Geração de Dados
 *  - Inteiros sequenciais; tenta enfileirar sem bloquear
 *  - Se cheio: descarta e segue
 * =============================== */
static void vProducerTask(void *arg) {
    (void)arg;
    esp_task_wdt_add(NULL);

    int seq = 0;
    for (;;) {
        if (xQueueSend(qData, &seq, 0) == pdTRUE) {
            hbGen = xTaskGetTickCount();
            okGen = true;
            LOGF("[GERADOR] Enfileirado: %d\n", seq);
        } else {
            LOGF("[GERADOR] Fila cheia; descartado: %d\n", seq);
        }
        seq++;

        /* Telemetria simples de stack */
        if (uxTaskGetStackHighWaterMark(NULL) < 100) {
            LOGF("[GERADOR] Alerta: baixa pilha disponível.\n");
        }

        feed_wdt_here();
        vTaskDelay(MS2T(PERIOD_GEN_MS));
    }
}

/* ===============================
 * Módulo 2 – Recepção/“Transmissão”
 *  - xQueueReceive com timeout
 *  - malloc/free por item recebido
 *  - Escalonamento por contagem de timeouts
 *  - Em falha persistente: encerra a própria tarefa
 * =============================== */
static void vConsumerTask(void *arg) {
    (void)arg;
    esp_task_wdt_add(NULL);

    int timeouts = 0;

    for (;;) {
        int v = 0;
        if (xQueueReceive(qData, &v, MS2T(RX_WAIT_MS)) == pdTRUE) {
            timeouts = 0;
            hbRx = xTaskGetTickCount();
            okRx = true;

            int *tmp = (int*)malloc(sizeof(int));
            if (!tmp) {
                LOGF("[RX] ERRO CRÍTICO: malloc falhou.\n");
                okRx = false;
                break; /* permite que o supervisor trate */
            }
            *tmp = v;
            LOGF("[RX] Transmitindo valor: %d\n", *tmp);
            free(tmp);

        } else {
            timeouts++;
            LOGF("[RX] Timeout (%d ms). cont=%d\n", RX_WAIT_MS, timeouts);

            switch (timeouts) {
                case RX_WARN_N:
                    LOGF("[RX] Aviso: sem dados; verificando fluxo.\n");
                    break;
                case RX_RECOVER_SOFT_N:
                    LOGF("[RX] Recuperação leve: limpando estados locais.\n");
                    /* (limpezas locais poderiam ser feitas aqui) */
                    break;
                case RX_RESET_Q_N:
                    LOGF("[RX] Recuperação moderada: resetando fila.\n");
                    xQueueReset(qData);
                    break;
                default:
                    if (timeouts >= RX_GIVEUP_N) {
                        LOGF("[RX] Falha persistente: finalizando para recriação.\n");
                        okRx = false;
                        vTaskDelete(NULL); /* encerra a tarefa */
                    }
            }
        }

        /* Telemetria de heap */
        size_t free_heap = xPortGetFreeHeapSize();
        size_t min_heap  = xPortGetMinimumEverFreeHeapSize();
        if (free_heap < (20 * 1024)) {
            LOGF("[RX] Heap livre baixo: %u (mínimo %u)\n",
                 (unsigned)free_heap, (unsigned)min_heap);
        }

        feed_wdt_here();
        vTaskDelay(MS2T(50));
    }

    LOGF("[RX] Encerrando.\n");
    vTaskDelete(NULL);
}

/* ===============================
 * Módulo 3 – Supervisão
 *  - Monitora heartbeats/flags
 *  - Recria tarefas paradas
 *  - Heurística de memória e reinício do chip
 * =============================== */
static void vGuardianTask(void *arg) {
    (void)arg;
    esp_task_wdt_add(NULL);

    uint8_t rx_restarts = 0;

    for (;;) {
        vTaskDelay(MS2T(PERIOD_SUP_MS));
        hbSup = xTaskGetTickCount();

        LOGF("[SUP] GEN:%s(hb=%u) | RX:%s(hb=%u)\n",
             okGen ? "OK" : "ERRO", (unsigned)hbGen,
             okRx  ? "OK" : "ERRO", (unsigned)hbRx);

        TickType_t now = xTaskGetTickCount();

        /* Gerador travado? (sem HB recente) */
        if ((now - hbGen) > MS2T(3 * PERIOD_SUP_MS)) {
            LOGF("[SUP] GERADOR inativo – recriando.\n");
            if (thGen) {
                vTaskDelete(thGen);
                thGen = NULL;
            }
            xTaskCreatePinnedToCore(vProducerTask, "producer",
                                    kStkGen, NULL, kPrioGen, &thGen, 1);
            hbGen = xTaskGetTickCount();
            okGen = false;
        }

        /* Consumidor ausente? (handle nulo ou sem HB) */
        if (thRx == NULL || (now - hbRx) > MS2T(5 * PERIOD_SUP_MS)) {
            LOGF("[SUP] RX inativa – recriando.\n");
            if (thRx) {
                vTaskDelete(thRx);
                thRx = NULL;
            }
            xTaskCreatePinnedToCore(vConsumerTask, "consumer",
                                    kStkRx, NULL, kPrioRx, &thRx, 1);
            hbRx = xTaskGetTickCount();
            okRx = false;

            /* Heurística de stress + memória */
            rx_restarts++;
            if (rx_restarts >= 3) {
                size_t free_heap = xPortGetFreeHeapSize();
                if (free_heap < (16 * 1024)) {
                    LOGF("[SUP] Memória crítica (%u). Reiniciando...\n", (unsigned)free_heap);
                    esp_restart();
                }
            }
        }

        /* Telemetria global de heap */
        size_t free_heap = xPortGetFreeHeapSize();
        size_t min_heap  = xPortGetMinimumEverFreeHeapSize();
        LOGF("[SUP] Heap livre=%u (mín=%u)\n", (unsigned)free_heap, (unsigned)min_heap);
        if (min_heap < (8 * 1024)) {
            LOGF("[SUP] Heap mínimo crítico – reiniciando...\n");
            esp_restart();
        }

        esp_task_wdt_reset();
    }
}

/* ===============================
 * Log periódico (opcional)
 * =============================== */
static void vTickerTask(void *arg) {
    (void)arg;
    for (;;) {
        LOGF("[LOG] HB_GEN=%u | HB_RX=%u | HB_SUP=%u\n",
             (unsigned)hbGen, (unsigned)hbRx, (unsigned)hbSup);
        vTaskDelay(MS2T(1000));
    }
}

/* ===============================
 * app_main – init, WDT, fila e tarefas
 * =============================== */
void app_main(void) {
    LOGF("[BOOT] FreeRTOS + WDT iniciando...\n");

    /* (Re)configura o Task WDT */
    esp_task_wdt_deinit();
    esp_task_wdt_config_t cfg = {
        .timeout_ms   = WDT_SECS * 1000,
        .trigger_panic = true
    };
    esp_task_wdt_init(&cfg);

    /* Fila de dados */
    qData = xQueueCreate(Q_LEN, Q_ITEM_SZ);
    if (!qData) {
        LOGF("[BOOT] ERRO: falha ao criar fila. Reiniciando...\n");
        esp_restart();
    }

    BaseType_t ok = pdPASS;

    ok &= (xTaskCreatePinnedToCore(vProducerTask, "producer",
                                   kStkGen, NULL, kPrioGen, &thGen, 1) == pdPASS);

    ok &= (xTaskCreatePinnedToCore(vConsumerTask, "consumer",
                                   kStkRx, NULL, kPrioRx, &thRx, 1) == pdPASS);

    ok &= (xTaskCreatePinnedToCore(vGuardianTask, "supervisor",
                                   kStkSup, NULL, kPrioSup, &thSup, 1) == pdPASS);

    /* Logger opcional */
    (void)xTaskCreatePinnedToCore(vTickerTask, "logger",
                                  kStkLog, NULL, kPrioLog, &thLog, 1);

    if (!ok) {
        LOGF("[BOOT] ERRO: criação de tarefas falhou. Reiniciando...\n");
        esp_restart();
    }

    LOGF("[BOOT] Sistema em execução.\n");
}
