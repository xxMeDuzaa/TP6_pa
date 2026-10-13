import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true, collection: 'event_log' })
export class EventLog {
  @Prop({ required: true, unique: true })
  eventId: string;

  @Prop({ required: true, index: true })
  correlationId: string;

  @Prop()
  topic: string;

  @Prop({ type: Object })
  payload: any;
}

export type EventLogDocument = EventLog & Document;
export const EventLogSchema = SchemaFactory.createForClass(EventLog);
