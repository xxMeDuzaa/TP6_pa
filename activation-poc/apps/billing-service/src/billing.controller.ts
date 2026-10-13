import { Controller, Inject, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload, Ctx } from '@nestjs/microservices';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { ActivationEvent, TOPICS } from '@activation-poc/contracts';
import { HandlerContext, IdempotencyOptions, processIdempotent, RetryWithDlq } from '@activation-poc/kafka-toolkit';
import { BillingAccount } from './schemas/billing-account.schema';
import { ProcessedEvent } from './schemas/processed-event.schema';

@Controller()
export class BillingController implements OnModuleInit {
  private readonly logger = new Logger(BillingController.name);

  constructor(
    @InjectModel(BillingAccount.name) private readonly billingModel: Model<BillingAccount>,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<ProcessedEvent>,
    @InjectConnection() private readonly connection: Connection,
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  private idempotency(topic: string): IdempotencyOptions {
    return { processedModel: this.processedModel, connection: this.connection, emitter: this.kafkaClient, topic };
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  @RetryWithDlq(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() event: ActivationEvent, @Ctx() _context: unknown) {
    await processIdempotent(event, this.idempotency(TOPICS.ACTIVATION_REQUESTED), async (e, ctx) => {
      // Escenario DLQ: fallo técnico que se reintenta 1s/2s/4s y termina en activation.requested.dlq
      if (e.payload?.simulateFailure === 'dlq') {
        throw new Error(`Simulated billing outage for activation ${e.correlationId}`);
      }

      // Caso borde: ActivationFailed llegó antes que ActivationRequested (ya compensado)
      const existing = await this.billingModel.findOne({ activationId: e.correlationId }).session(ctx.session);
      if (existing?.status === 'CANCELLED') return;

      if (e.payload?.simulateFailure === 'billing') {
        this.publish(ctx, 'BillingFailed', e);
        return;
      }

      await this.billingModel.create(
        [{ activationId: e.correlationId, customerId: e.customerId, status: 'CREATED' }],
        { session: ctx.session },
      );
      this.publish(ctx, 'BillingAccountCreated', e);
    });
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  @RetryWithDlq(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() event: ActivationEvent, @Ctx() _context: unknown) {
    if (event?.eventType !== 'ActivationFailed') return;

    await processIdempotent(event, this.idempotency(TOPICS.ACTIVATION_EVENTS), async (e, ctx) => {
      // Compensación (Saga): la cuenta queda CANCELLED aunque todavía no existiera.
      await this.billingModel.updateOne(
        { activationId: e.correlationId },
        { $set: { status: 'CANCELLED', customerId: e.customerId } },
        { upsert: true, session: ctx.session ?? undefined },
      );
      this.publish(ctx, 'BillingAccountCancelled', e);
    });
  }

  private publish(ctx: HandlerContext, eventType: string, source: ActivationEvent) {
    const event: ActivationEvent = {
      eventId: uuidv4(),
      eventType,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: source.correlationId,
      customerId: source.customerId,
      source: 'billing-service',
    };
    ctx.publish(TOPICS.BILLING_EVENTS, { key: event.customerId, value: event });
    this.logger.log(`${eventType} -> ${source.correlationId} (cliente ${source.customerId})`);
  }
}
