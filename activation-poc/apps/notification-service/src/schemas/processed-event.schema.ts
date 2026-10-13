import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

/**
 * Registro de idempotencia (RNF-03): un documento por eventId ya procesado.
 * MongoDB borra cada registro a los 7 días (índice TTL sobre createdAt).
 */
@Schema({ collection: 'processed_events' })
export class ProcessedEvent {
  @Prop({ required: true, unique: true })
  eventId: string;

  @Prop()
  eventType?: string;

  @Prop()
  topic?: string;

  @Prop({ default: Date.now, expires: 604800 }) // 7 días
  createdAt: Date;
}

export type ProcessedEventDocument = HydratedDocument<ProcessedEvent>;
export const ProcessedEventSchema = SchemaFactory.createForClass(ProcessedEvent);
