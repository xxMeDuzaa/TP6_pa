import { Injectable, Inject } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Activation, ActivationDocument } from './schemas/activation.schema';
import { TOPICS, ActivationEvent } from '@activation-poc/contracts';
import { v4 as uuidv4 } from 'uuid';
import { ActivationGateway } from './activation.gateway';

@Injectable()
export class ActivationService {
  constructor(
    @InjectModel(Activation.name) private activationModel: Model<ActivationDocument>,
    @Inject('KAFKA_CLIENT') private kafkaClient: ClientKafka,
    private gateway: ActivationGateway,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  async createActivation(data: any): Promise<string> {
    const activationId = `act-${uuidv4().split('-')[0]}`;
    
    const activation = new this.activationModel({
      _id: activationId,
      customerId: data.customerId || 'C-1234',
      planId: data.planId,
      simulateFailure: data.simulateFailure || 'none',
      status: 'PENDING',
      history: [{ eventType: 'ActivationRequested', at: new Date() }],
    });
    
    await activation.save();
    
    const event: ActivationEvent = {
      eventId: uuidv4(),
      eventType: 'ActivationRequested',
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: activationId,
      customerId: activation.customerId,
      source: 'activation-api',
      payload: {
        planId: activation.planId,
        channel: 'web',
        simulateFailure: activation.simulateFailure as any,
      },
    };

    this.kafkaClient.emit(TOPICS.ACTIVATION_REQUESTED, {
      key: activation.customerId,
      value: event,
    });
    
    this.gateway.notifyActivationUpdate(activation);
    return activationId;
  }

  async getActivation(id: string) {
    return this.activationModel.findById(id).exec();
  }

  async handleEvent(message: any) {
    const event = message as ActivationEvent;
    if (!event.correlationId) return;

    const activation = await this.activationModel.findById(event.correlationId);
    if (!activation) return;

    const { eventType } = event;

    // Idempotencia liviana: un mismo eventId no se aplica dos veces al historial
    if (activation.history.some((h: any) => h.eventId === event.eventId)) return;

    activation.history.push({ eventType, eventId: event.eventId, at: new Date(event.occurredAt) });

    if (eventType === 'BillingAccountCreated') {
      activation.steps = { ...activation.steps, billing: { status: 'OK', at: new Date() } };
    } else if (eventType === 'BillingFailed') {
      activation.steps = { ...activation.steps, billing: { status: 'ERROR', at: new Date() } };
    } else if (eventType === 'ProvisioningCompleted') {
      activation.steps = { ...activation.steps, provisioning: { status: 'OK', at: new Date() } };
    } else if (eventType === 'ProvisioningFailed') {
      activation.steps = { ...activation.steps, provisioning: { status: 'ERROR', at: new Date() } };
    }

    if (activation.status === 'PENDING') {
      activation.status = 'IN_PROGRESS';
    }

    // Evaluate final state
    const billingStatus = activation.steps?.billing?.status;
    const provisioningStatus = activation.steps?.provisioning?.status;

    let finalEvent: ActivationEvent | null = null;

    if (activation.status !== 'ACTIVE' && activation.status !== 'FAILED') {
      if (billingStatus === 'OK' && provisioningStatus === 'OK') {
        activation.status = 'ACTIVE';
        finalEvent = this.createFinalEvent('ActivationCompleted', activation);
        activation.history.push({ eventType: 'ActivationCompleted', at: new Date() });
      } else if (billingStatus === 'ERROR' || provisioningStatus === 'ERROR') {
        activation.status = 'FAILED';
        finalEvent = this.createFinalEvent('ActivationFailed', activation);
        activation.history.push({ eventType: 'ActivationFailed', at: new Date() });
      }
    }

    await activation.save();
    this.gateway.notifyActivationUpdate(activation);

    if (finalEvent) {
      this.kafkaClient.emit(TOPICS.ACTIVATION_EVENTS, {
        key: activation.customerId,
        value: finalEvent,
      });
    }
  }

  private createFinalEvent(eventType: string, activation: ActivationDocument): ActivationEvent {
    return {
      eventId: uuidv4(),
      eventType,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: activation._id,
      customerId: activation.customerId,
      source: 'activation-api',
    };
  }
}
