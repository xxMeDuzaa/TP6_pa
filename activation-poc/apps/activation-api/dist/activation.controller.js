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
exports.ActivationController = void 0;
const common_1 = require("@nestjs/common");
const activation_service_1 = require("./activation.service");
const microservices_1 = require("@nestjs/microservices");
const contracts_1 = require("@activation-poc/contracts");
let ActivationController = class ActivationController {
    constructor(activationService) {
        this.activationService = activationService;
    }
    async createActivation(data) {
        const activationId = await this.activationService.createActivation(data);
        return { activationId, status: 'PENDING' };
    }
    async getActivation(id) {
        return this.activationService.getActivation(id);
    }
    async handleBillingEvents(message) {
        await this.activationService.handleEvent(message);
    }
    async handleProvisioningEvents(message) {
        await this.activationService.handleEvent(message);
    }
};
exports.ActivationController = ActivationController;
__decorate([
    (0, common_1.Post)(),
    (0, common_1.HttpCode)(202),
    __param(0, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], ActivationController.prototype, "createActivation", null);
__decorate([
    (0, common_1.Get)(':id'),
    __param(0, (0, common_1.Param)('id')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], ActivationController.prototype, "getActivation", null);
__decorate([
    (0, microservices_1.EventPattern)(contracts_1.TOPICS.BILLING_EVENTS),
    __param(0, (0, microservices_1.Payload)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], ActivationController.prototype, "handleBillingEvents", null);
__decorate([
    (0, microservices_1.EventPattern)(contracts_1.TOPICS.PROVISIONING_EVENTS),
    __param(0, (0, microservices_1.Payload)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], ActivationController.prototype, "handleProvisioningEvents", null);
exports.ActivationController = ActivationController = __decorate([
    (0, common_1.Controller)('activations'),
    __metadata("design:paramtypes", [activation_service_1.ActivationService])
], ActivationController);
//# sourceMappingURL=activation.controller.js.map