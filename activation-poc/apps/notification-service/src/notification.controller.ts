import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Notification, NotificationDocument } from './schemas/notification.schema';
import { ProcessedEvent, ProcessedEventDocument } from './schemas/processed-event.schema';
import { TOPICS, ActivationEvent } from '@activation-poc/contracts';
import * as nodemailer from 'nodemailer';

@Controller()
export class NotificationController {
  private transporter: nodemailer.Transporter;

  constructor(
    @InjectModel(Notification.name) private notificationModel: Model<NotificationDocument>,
    @InjectModel(ProcessedEvent.name) private eventModel: Model<ProcessedEventDocument>,
  ) {
    this.transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST || 'localhost',
      port: parseInt(process.env.SMTP_PORT || '1025', 10),
      ignoreTLS: true,
    });
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    const event = message as ActivationEvent;
    
    if (event.eventType === 'ActivationCompleted' || event.eventType === 'ActivationFailed') {
      await this.processIdempotent(event, async (e) => {
        
        const subject = e.eventType === 'ActivationCompleted' 
          ? `¡Tu plan ha sido activado!` 
          : `Problemas con la activación de tu plan`;
          
        const text = `Hola cliente ${e.customerId}, te informamos sobre el estado de tu activación: ${e.eventType}.`;

        await this.transporter.sendMail({
          from: '"POC Activación" <noreply@empresa.com>',
          to: `${e.customerId}@example.com`,
          subject,
          text,
        });

        await this.notificationModel.create({
          activationId: e.correlationId,
          eventType: e.eventType,
          recipient: `${e.customerId}@example.com`,
          status: 'SENT'
        });
      });
    }
  }

  private async processIdempotent(event: ActivationEvent, handler: (event: ActivationEvent) => Promise<void>) {
    try {
      const exists = await this.eventModel.findOne({ eventId: event.eventId });
      if (exists) return; 
      
      await handler(event);
      await this.eventModel.create({ eventId: event.eventId });
    } catch (error) {
      console.error('Error processing event', error);
      throw error;
    }
  }
}
