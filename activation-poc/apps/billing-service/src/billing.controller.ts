import { Controller, Inject } from '@nestjs/common';
import { EventPattern, Payload, ClientKafka } from '@nestjs/microservices';
import { InjectModel, InjectConnection } from '@nestjs/mongoose';
import { Model, Connection } from 'mongoose';
import { BillingAccount, BillingAccountDocument } from './schemas/billing-account.schema';
import { ProcessedEvent, ProcessedEventDocument } from './schemas/processed-event.schema';
import { TOPICS, ActivationEvent } from '@activation-poc/contracts';
import { processWithIdempotencyAndDlq } from '@activation-poc/kafka-toolkit';
import { v4 as uuidv4 } from 'uuid';

@Controller()
export class BillingController {
  constructor(
    @InjectModel(BillingAccount.name) private billingModel: Model<BillingAccountDocument>,
    @InjectModel(ProcessedEvent.name) private eventModel: Model<ProcessedEventDocument>,
    @InjectConnection() private connection: Connection,
    @Inject('KAFKA_CLIENT') private kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    this.kafkaClient.subscribeToResponseOf(TOPICS.BILLING_EVENTS);
    await this.kafkaClient.connect();
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any) {
    const event = message as ActivationEvent;
    await processWithIdempotencyAndDlq({
      event,
      eventModel: this.eventModel,
      topic: TOPICS.ACTIVATION_REQUESTED,
      connection: this.connection,
      kafkaClient: this.kafkaClient,
      handler: async (e) => {
        // Edge case: if ActivationFailed was received before creation
        const existing = await this.billingModel.findOne({ activationId: e.correlationId });
        if (existing && existing.status === 'CANCELLED') {
          console.log(`[billing-service] Activation ${e.correlationId} was already cancelled prior to creation.`);
          return;
        }

        if (e.payload?.simulateFailure === 'billing') {
          await this.publishEvent('BillingFailed', e);
          return;
        }

        await this.billingModel.create({
          activationId: e.correlationId,
          customerId: e.customerId,
          status: 'CREATED',
        });

        await this.publishEvent('BillingAccountCreated', e);
      },
    });
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    const event = message as ActivationEvent;
    if (event.eventType === 'ActivationFailed') {
      await processWithIdempotencyAndDlq({
        event,
        eventModel: this.eventModel,
        topic: TOPICS.ACTIVATION_EVENTS,
        connection: this.connection,
        kafkaClient: this.kafkaClient,
        handler: async (e) => {
          // Compensate: upsert status as CANCELLED
          await this.billingModel.findOneAndUpdate(
            { activationId: e.correlationId },
            { $set: { status: 'CANCELLED', customerId: e.customerId } },
            { upsert: true, new: true }
          );
          await this.publishEvent('BillingAccountCancelled', e);
        },
      });
    }
  }

  private async publishEvent(eventType: string, sourceEvent: ActivationEvent) {
    const event: ActivationEvent = {
      eventId: uuidv4(),
      eventType,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: sourceEvent.correlationId,
      customerId: sourceEvent.customerId,
      source: 'billing-service',
    };

    this.kafkaClient.emit(TOPICS.BILLING_EVENTS, {
      key: event.customerId,
      value: event,
    });
  }
}
