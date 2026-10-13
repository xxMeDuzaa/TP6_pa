import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type ActivationDocument = Activation & Document;

@Schema({ timestamps: true, collection: 'activations' })
export class Activation {
  @Prop({ required: true })
  _id: string; // The activationId

  @Prop({ required: true, index: true })
  customerId: string;

  @Prop({ required: true })
  planId: string;

  @Prop({ required: true })
  status: string;

  @Prop()
  simulateFailure: string;

  @Prop({ type: Object, default: {} })
  steps: {
    billing?: { status: string; at: Date };
    provisioning?: { status: string; at: Date };
  };

  @Prop({ type: Array, default: [] })
  history: Array<{ eventType: string; eventId?: string; at: Date }>;
}

export const ActivationSchema = SchemaFactory.createForClass(Activation);
