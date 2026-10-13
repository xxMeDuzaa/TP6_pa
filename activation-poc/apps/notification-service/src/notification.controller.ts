import { Controller, Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload, Ctx } from '@nestjs/microservices';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import * as nodemailer from 'nodemailer';
import { ActivationEvent, TOPICS } from '@activation-poc/contracts';
import { processIdempotent, RetryWithDlq } from '@activation-poc/kafka-toolkit';
import { Notification } from './schemas/notification.schema';
import { ProcessedEvent } from './schemas/processed-event.schema';

@Controller()
export class NotificationController implements OnModuleInit {
  private readonly transporter: nodemailer.Transporter;

  constructor(
    @InjectModel(Notification.name) private readonly notificationModel: Model<Notification>,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<ProcessedEvent>,
    @InjectConnection() private readonly connection: Connection,
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka, // sólo para la DLQ
  ) {
    this.transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST || 'localhost',
      port: parseInt(process.env.SMTP_PORT || '1025', 10),
      ignoreTLS: true,
    });
  }

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  @RetryWithDlq(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() event: ActivationEvent, @Ctx() _context: unknown) {
    if (event?.eventType !== 'ActivationCompleted' && event?.eventType !== 'ActivationFailed') return;

    await processIdempotent(
      event,
      { processedModel: this.processedModel, connection: this.connection, topic: TOPICS.ACTIVATION_EVENTS },
      async (e, ctx) => {
        const recipient = `${e.customerId}@example.com`;
        const subject =
          e.eventType === 'ActivationCompleted' ? '¡Tu plan ha sido activado!' : 'Problemas con la activación de tu plan';

        // Si el mail falla se aborta la transacción y se reintenta; si el commit fallara
        // después de enviarlo, el reintento podría mandar un duplicado (at-least-once).
        await this.transporter.sendMail({
          from: '"POC Activación" <noreply@empresa.com>',
          to: recipient,
          subject,
          text: `Hola cliente ${e.customerId}, te informamos sobre el estado de tu activación ${e.correlationId}: ${e.eventType}.`,
        });

        await this.notificationModel.create(
          [{ activationId: e.correlationId, eventType: e.eventType, recipient, status: 'SENT' }],
          { session: ctx.session },
        );
      },
    );
  }
}
