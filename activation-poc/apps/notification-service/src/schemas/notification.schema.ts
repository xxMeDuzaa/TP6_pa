import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type NotificationDocument = Notification & Document;

@Schema({ timestamps: true })
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

export const NotificationSchema = SchemaFactory.createForClass(Notification);
NotificationSchema.index({ activationId: 1, eventType: 1 });
