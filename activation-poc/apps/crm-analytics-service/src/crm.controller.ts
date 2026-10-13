import { Controller, Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload, Ctx } from '@nestjs/microservices';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import { ActivationEvent, TOPICS } from '@activation-poc/contracts';
import { processIdempotent, RetryWithDlq } from '@activation-poc/kafka-toolkit';
import { EventLog } from './schemas/event-log.schema';
import { ProcessedEvent } from './schemas/processed-event.schema';

@Controller()
export class CrmController implements OnModuleInit {
  constructor(
    @InjectModel(EventLog.name) private readonly eventLogModel: Model<EventLog>,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<ProcessedEvent>,
    @InjectConnection() private readonly connection: Connection,
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka, // sólo para la DLQ
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  @RetryWithDlq(TOPICS.ACTIVATION_REQUESTED)
  async handleRequested(@Payload() event: ActivationEvent, @Ctx() _context: unknown) {
    await this.logEvent(event, TOPICS.ACTIVATION_REQUESTED);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  @RetryWithDlq(TOPICS.BILLING_EVENTS)
  async handleBilling(@Payload() event: ActivationEvent, @Ctx() _context: unknown) {
    await this.logEvent(event, TOPICS.BILLING_EVENTS);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  @RetryWithDlq(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioning(@Payload() event: ActivationEvent, @Ctx() _context: unknown) {
    await this.logEvent(event, TOPICS.PROVISIONING_EVENTS);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  @RetryWithDlq(TOPICS.ACTIVATION_EVENTS)
  async handleFinal(@Payload() event: ActivationEvent, @Ctx() _context: unknown) {
    await this.logEvent(event, TOPICS.ACTIVATION_EVENTS);
  }

  private async logEvent(event: ActivationEvent, topic: string) {
    await processIdempotent(
      event,
      { processedModel: this.processedModel, connection: this.connection, topic },
      async (e, ctx) => {
        await this.eventLogModel.create(
          [{ eventId: e.eventId, correlationId: e.correlationId, topic, payload: e }],
          { session: ctx.session },
        );
      },
    );
  }
}
