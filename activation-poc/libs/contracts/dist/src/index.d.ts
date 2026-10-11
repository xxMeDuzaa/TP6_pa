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
export declare const TOPICS: {
    ACTIVATION_REQUESTED: string;
    BILLING_EVENTS: string;
    PROVISIONING_EVENTS: string;
    ACTIVATION_EVENTS: string;
};
