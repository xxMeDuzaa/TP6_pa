/**
 * - 'none'         → camino feliz.
 * - 'billing'      → fallo de NEGOCIO en billing (publica BillingFailed → compensación).
 * - 'provisioning' → fallo de NEGOCIO en provisioning (publica ProvisioningFailed → compensación).
 * - 'dlq'          → fallo TÉCNICO en billing (lanza excepción): agota los reintentos
 *                    (1s, 2s, 4s) y el evento termina en activation.requested.dlq (RNF-04).
 */
export type SimulateFailure = 'none' | 'billing' | 'provisioning' | 'dlq';

export interface ActivationEventPayload {
  planId?: string;
  channel?: string;
  simulateFailure?: SimulateFailure;
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
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];
