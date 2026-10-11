import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true })
export class EventLog {
  @Prop({ required: true, unique: true })
  eventId: string;

  @Prop({ required: true })
  correlationId: string;

  @Prop({ type: Object })
  payload: any;
}

export type EventLogDocument = EventLog & Document;
export const EventLogSchema = SchemaFactory.createForClass(EventLog);
