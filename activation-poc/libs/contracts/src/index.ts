export interface ActivationEventPayload {
  planId?: string;
  channel?: string;
  simulateFailure?: 'none' | 'billing' | 'provisioning';
}

export interface ActivationEvent {
  eventId: string;
  eventType: string;
  version: number;
  occurredAt: string;
  correlationId: string;
  customerId: string;
  source: string;
  payload?: ActivationEventPayload;
}

export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events'
};
