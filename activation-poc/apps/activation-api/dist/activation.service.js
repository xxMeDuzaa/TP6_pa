"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ActivationService = void 0;
const common_1 = require("@nestjs/common");
const microservices_1 = require("@nestjs/microservices");
const mongoose_1 = require("@nestjs/mongoose");
const mongoose_2 = require("mongoose");
const activation_schema_1 = require("./schemas/activation.schema");
const contracts_1 = require("@activation-poc/contracts");
const uuid_1 = require("uuid");
const activation_gateway_1 = require("./activation.gateway");
let ActivationService = class ActivationService {
    constructor(activationModel, kafkaClient, gateway) {
        this.activationModel = activationModel;
        this.kafkaClient = kafkaClient;
        this.gateway = gateway;
    }
    async onModuleInit() {
        this.kafkaClient.subscribeToResponseOf(contracts_1.TOPICS.ACTIVATION_REQUESTED);
        this.kafkaClient.subscribeToResponseOf(contracts_1.TOPICS.ACTIVATION_EVENTS);
        await this.kafkaClient.connect();
    }
    async createActivation(data) {
        const activationId = `act-${(0, uuid_1.v4)().split('-')[0]}`;
        const activation = new this.activationModel({
            _id: activationId,
            customerId: data.customerId || 'C-1234',
            planId: data.planId,
            simulateFailure: data.simulateFailure || 'none',
            status: 'PENDING',
            history: [{ eventType: 'ActivationRequested', at: new Date() }],
        });
        await activation.save();
        const event = {
            eventId: (0, uuid_1.v4)(),
            eventType: 'ActivationRequested',
            version: 1,
            occurredAt: new Date().toISOString(),
            correlationId: activationId,
            customerId: activation.customerId,
            source: 'activation-api',
            payload: {
                planId: activation.planId,
                channel: 'web',
                simulateFailure: activation.simulateFailure,
            },
        };
        this.kafkaClient.emit(contracts_1.TOPICS.ACTIVATION_REQUESTED, {
            key: activation.customerId,
            value: event,
        });
        this.gateway.notifyActivationUpdate(activation);
        return activationId;
    }
    async getActivation(id) {
        return this.activationModel.findById(id).exec();
    }
    async handleEvent(message) {
        const event = message;
        if (!event.correlationId)
            return;
        const activation = await this.activationModel.findById(event.correlationId);
        if (!activation)
            return;
        const { eventType } = event;
        activation.history.push({ eventType, at: new Date(event.occurredAt) });
        if (eventType === 'BillingAccountCreated') {
            activation.steps = { ...activation.steps, billing: { status: 'OK', at: new Date() } };
        }
        else if (eventType === 'BillingFailed') {
            activation.steps = { ...activation.steps, billing: { status: 'ERROR', at: new Date() } };
        }
        else if (eventType === 'ProvisioningCompleted') {
            activation.steps = { ...activation.steps, provisioning: { status: 'OK', at: new Date() } };
        }
        else if (eventType === 'ProvisioningFailed') {
            activation.steps = { ...activation.steps, provisioning: { status: 'ERROR', at: new Date() } };
        }
        if (activation.status === 'PENDING') {
            activation.status = 'IN_PROGRESS';
        }
        const billingStatus = activation.steps?.billing?.status;
        const provisioningStatus = activation.steps?.provisioning?.status;
        let finalEvent = null;
        if (activation.status !== 'ACTIVE' && activation.status !== 'FAILED') {
            if (billingStatus === 'OK' && provisioningStatus === 'OK') {
                activation.status = 'ACTIVE';
                finalEvent = this.createFinalEvent('ActivationCompleted', activation);
                activation.history.push({ eventType: 'ActivationCompleted', at: new Date() });
            }
            else if (billingStatus === 'ERROR' || provisioningStatus === 'ERROR') {
                activation.status = 'FAILED';
                finalEvent = this.createFinalEvent('ActivationFailed', activation);
                activation.history.push({ eventType: 'ActivationFailed', at: new Date() });
            }
        }
        await activation.save();
        this.gateway.notifyActivationUpdate(activation);
        if (finalEvent) {
            this.kafkaClient.emit(contracts_1.TOPICS.ACTIVATION_EVENTS, {
                key: activation.customerId,
                value: finalEvent,
            });
        }
    }
    createFinalEvent(eventType, activation) {
        return {
            eventId: (0, uuid_1.v4)(),
            eventType,
            version: 1,
            occurredAt: new Date().toISOString(),
            correlationId: activation._id,
            customerId: activation.customerId,
            source: 'activation-api',
        };
    }
};
exports.ActivationService = ActivationService;
exports.ActivationService = ActivationService = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, mongoose_1.InjectModel)(activation_schema_1.Activation.name)),
    __param(1, (0, common_1.Inject)('KAFKA_CLIENT')),
    __metadata("design:paramtypes", [mongoose_2.Model, microservices_1.ClientKafka, activation_gateway_1.ActivationGateway])
], ActivationService);
//# sourceMappingURL=activation.service.js.map