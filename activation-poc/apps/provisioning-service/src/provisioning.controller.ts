import { Controller, Inject, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload, Ctx } from '@nestjs/microservices';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { ActivationEvent, TOPICS } from '@activation-poc/contracts';
import { HandlerContext, processIdempotent, RetryWithDlq } from '@activation-poc/kafka-toolkit';
import { ProvisioningOrder } from './schemas/provisioning-order.schema';
import { ProcessedEvent } from './schemas/processed-event.schema';

@Controller()
export class ProvisioningController implements OnModuleInit {
  private readonly logger = new Logger(ProvisioningController.name);

  constructor(
    @InjectModel(ProvisioningOrder.name) private readonly provisioningModel: Model<ProvisioningOrder>,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<ProcessedEvent>,
    @InjectConnection() private readonly connection: Connection,
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  @RetryWithDlq(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() event: ActivationEvent, @Ctx() _context: unknown) {
    // La demora simulada (1-3 s) va ANTES de abrir la transacción para no mantenerla abierta.
    const delay = Math.floor(Math.random() * 2000) + 1000;
    await new Promise((resolve) => setTimeout(resolve, delay));

    await processIdempotent(
      event,
      {
        processedModel: this.processedModel,
        connection: this.connection,
        emitter: this.kafkaClient,
        topic: TOPICS.ACTIVATION_REQUESTED,
      },
      async (e, ctx) => {
        if (e.payload?.simulateFailure === 'provisioning') {
          this.publish(ctx, 'ProvisioningFailed', e);
          return;
        }

        await this.provisioningModel.create(
          [{ activationId: e.correlationId, customerId: e.customerId, status: 'COMPLETED' }],
          { session: ctx.session },
        );
        this.publish(ctx, 'ProvisioningCompleted', e);
      },
    );
  }

  private publish(ctx: HandlerContext, eventType: string, source: ActivationEvent) {
    const event: ActivationEvent = {
      eventId: uuidv4(),
      eventType,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: source.correlationId,
      customerId: source.customerId,
      source: 'provisioning-service',
    };
    ctx.publish(TOPICS.PROVISIONING_EVENTS, { key: event.customerId, value: event });
    this.logger.log(`${eventType} -> ${source.correlationId} (cliente ${source.customerId})`);
  }
}
