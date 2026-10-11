import { ClientKafka } from '@nestjs/microservices';
import { Model, Connection } from 'mongoose';
import { ActivationEvent } from '@activation-poc/contracts';
export declare class KafkaToolkitModule {
}
export interface ProcessEventOptions {
    event: ActivationEvent;
    eventModel: Model<any>;
    topic: string;
    connection?: Connection;
    kafkaClient?: ClientKafka;
    maxRetries?: number;
    backoffMs?: number[];
    handler: (event: ActivationEvent) => Promise<void>;
}
/**
 * Executes an event handler with:
 * 1. Idempotency check via eventModel (processed_events).
 * 2. MongoDB transaction support if connection is provided.
 * 3. Exponential backoff retry (1s, 2s, 4s).
 * 4. Routing to <topic>.dlq upon 3 consecutive failures without blocking Kafka partition.
 */
export declare function processWithIdempotencyAndDlq(options: ProcessEventOptions): Promise<void>;
