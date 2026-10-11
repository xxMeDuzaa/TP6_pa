import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { EventLog, EventLogDocument } from './schemas/event-log.schema';
import { ProcessedEvent, ProcessedEventDocument } from './schemas/processed-event.schema';
import { TOPICS, ActivationEvent } from '@activation-poc/contracts';

@Controller()
export class CrmController {
  constructor(
    @InjectModel(EventLog.name) private eventLogModel: Model<EventLogDocument>,
    @InjectModel(ProcessedEvent.name) private eventModel: Model<ProcessedEventDocument>,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleRequested(@Payload() message: any) { await this.logEvent(message); }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBilling(@Payload() message: any) { await this.logEvent(message); }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioning(@Payload() message: any) { await this.logEvent(message); }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleFinal(@Payload() message: any) { await this.logEvent(message); }

  private async logEvent(event: ActivationEvent) {
    try {
      const exists = await this.eventModel.findOne({ eventId: event.eventId });
      if (exists) return; 
      
      await this.eventLogModel.create({
        eventId: event.eventId,
        correlationId: event.correlationId,
        payload: event
      });
      await this.eventModel.create({ eventId: event.eventId });
    } catch (error) {
      console.error('Error processing event', error);
      throw error;
    }
  }
}
