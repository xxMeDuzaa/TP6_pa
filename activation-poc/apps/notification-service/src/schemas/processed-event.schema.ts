import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true })
export class ProcessedEvent {
  @Prop({ required: true, unique: true })
  eventId: string;

  @Prop({ default: Date.now, expires: 604800 })
  createdAt: Date;
}

export type ProcessedEventDocument = ProcessedEvent & Document;
export const ProcessedEventSchema = SchemaFactory.createForClass(ProcessedEvent);
