import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true, collection: 'provisioning_orders' })
export class ProvisioningOrder {
  @Prop({ required: true, unique: true })
  activationId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  status: string;
}

export type ProvisioningOrderDocument = ProvisioningOrder & Document;
export const ProvisioningOrderSchema = SchemaFactory.createForClass(ProvisioningOrder);
