import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true, collection: 'notifications' })
export class Notification {
  @Prop({ required: true })
  activationId: string;

  @Prop({ required: true })
  eventType: string;

  @Prop({ required: true })
  recipient: string;

  @Prop({ required: true })
  status: string;
}

export type NotificationDocument = Notification & Document;
export const NotificationSchema = SchemaFactory.createForClass(Notification);
NotificationSchema.index({ activationId: 1, eventType: 1 });
