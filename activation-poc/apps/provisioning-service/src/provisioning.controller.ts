import { Controller, Inject } from '@nestjs/common';
import { EventPattern, Payload, ClientKafka } from '@nestjs/microservices';
import { InjectModel, InjectConnection } from '@nestjs/mongoose';
import { Model, Connection } from 'mongoose';
import { ProvisioningOrder, ProvisioningOrderDocument } from './schemas/provisioning-order.schema';
import { ProcessedEvent, ProcessedEventDocument } from './schemas/processed-event.schema';
import { TOPICS, ActivationEvent } from '@activation-poc/contracts';
import { processWithIdempotencyAndDlq } from '@activation-poc/kafka-toolkit';
import { v4 as uuidv4 } from 'uuid';

@Controller()
export class ProvisioningController {
  constructor(
    @InjectModel(ProvisioningOrder.name) private provisioningModel: Model<ProvisioningOrderDocument>,
    @InjectModel(ProcessedEvent.name) private eventModel: Model<ProcessedEventDocument>,
    @InjectConnection() private connection: Connection,
    @Inject('KAFKA_CLIENT') private kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    this.kafkaClient.subscribeToResponseOf(TOPICS.PROVISIONING_EVENTS);
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
        // Simulate provisioning delay 1-3 seconds
        const delay = Math.floor(Math.random() * 2000) + 1000;
        await new Promise(resolve => setTimeout(resolve, delay));

        if (e.payload?.simulateFailure === 'provisioning') {
          await this.publishEvent('ProvisioningFailed', e);
          return;
        }

        await this.provisioningModel.create({
          activationId: e.correlationId,
          customerId: e.customerId,
          status: 'COMPLETED',
        });

        await this.publishEvent('ProvisioningCompleted', e);
      },
    });
  }

  private async publishEvent(eventType: string, sourceEvent: ActivationEvent) {
    const event: ActivationEvent = {
      eventId: uuidv4(),
      eventType,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: sourceEvent.correlationId,
      customerId: sourceEvent.customerId,
      source: 'provisioning-service',
    };

    this.kafkaClient.emit(TOPICS.PROVISIONING_EVENTS, {
      key: event.customerId,
      value: event,
    });
  }
}
