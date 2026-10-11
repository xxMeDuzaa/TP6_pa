"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaToolkitModule = void 0;
exports.processWithIdempotencyAndDlq = processWithIdempotencyAndDlq;
const common_1 = require("@nestjs/common");
let KafkaToolkitModule = class KafkaToolkitModule {
};
exports.KafkaToolkitModule = KafkaToolkitModule;
exports.KafkaToolkitModule = KafkaToolkitModule = __decorate([
    (0, common_1.Global)(),
    (0, common_1.Module)({
        providers: [],
        exports: [],
    })
], KafkaToolkitModule);
/**
 * Executes an event handler with:
 * 1. Idempotency check via eventModel (processed_events).
 * 2. MongoDB transaction support if connection is provided.
 * 3. Exponential backoff retry (1s, 2s, 4s).
 * 4. Routing to <topic>.dlq upon 3 consecutive failures without blocking Kafka partition.
 */
async function processWithIdempotencyAndDlq(options) {
    const { event, eventModel, topic, connection, kafkaClient, maxRetries = 3, backoffMs = [1000, 2000, 4000], handler, } = options;
    if (!event || !event.eventId) {
        console.warn(`[kafka-toolkit] Received invalid event without eventId on topic ${topic}`);
        return;
    }
    // Idempotency check
    try {
        const alreadyProcessed = await eventModel.findOne({ eventId: event.eventId });
        if (alreadyProcessed) {
            console.log(`[kafka-toolkit] [IDEMPOTENCY] Event ${event.eventId} (${event.eventType}) already processed. Skipping.`);
            return;
        }
    }
    catch (err) {
        console.warn(`[kafka-toolkit] Error checking idempotency for ${event.eventId}:`, err.message);
    }
    let attempt = 0;
    let lastError = null;
    while (attempt < maxRetries) {
        attempt++;
        try {
            if (connection) {
                const session = await connection.startSession();
                try {
                    await session.withTransaction(async () => {
                        await handler(event);
                        await eventModel.create([{ eventId: event.eventId, processedAt: new Date() }], { session });
                    });
                }
                finally {
                    await session.endSession();
                }
            }
            else {
                await handler(event);
                await eventModel.create({ eventId: event.eventId, processedAt: new Date() });
            }
            console.log(`[kafka-toolkit] [SUCCESS] Event ${event.eventId} (${event.eventType}) processed successfully (attempt ${attempt}).`);
            return;
        }
        catch (err) {
            lastError = err;
            console.warn(`[kafka-toolkit] [RETRY] Attempt ${attempt}/${maxRetries} failed for event ${event.eventId}: ${err.message}`);
            if (attempt < maxRetries) {
                const delay = backoffMs[attempt - 1] ?? 1000 * Math.pow(2, attempt - 1);
                await new Promise((resolve) => setTimeout(resolve, delay));
            }
        }
    }
    // DLQ routing
    const dlqTopic = `${topic}.dlq`;
    console.error(`[kafka-toolkit] [DLQ] Event ${event.eventId} failed after ${maxRetries} retries. Routing to ${dlqTopic}. Error: ${lastError?.message}`);
    if (kafkaClient) {
        try {
            kafkaClient.emit(dlqTopic, {
                key: event.customerId || 'UNKNOWN',
                value: {
                    originalEvent: event,
                    error: {
                        message: lastError?.message || 'Unknown error',
                        stack: lastError?.stack,
                        failedAt: new Date().toISOString(),
                        attempts: maxRetries,
                    },
                },
            });
            console.log(`[kafka-toolkit] [DLQ] Successfully sent failed event to ${dlqTopic}`);
        }
        catch (dlqErr) {
            console.error(`[kafka-toolkit] [DLQ] Failed to emit to DLQ ${dlqTopic}:`, dlqErr.message);
        }
    }
}
