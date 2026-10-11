"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AppModule = void 0;
const common_1 = require("@nestjs/common");
const microservices_1 = require("@nestjs/microservices");
const mongoose_1 = require("@nestjs/mongoose");
const activation_controller_1 = require("./activation.controller");
const activation_service_1 = require("./activation.service");
const activation_gateway_1 = require("./activation.gateway");
const activation_schema_1 = require("./schemas/activation.schema");
let AppModule = class AppModule {
};
exports.AppModule = AppModule;
exports.AppModule = AppModule = __decorate([
    (0, common_1.Module)({
        imports: [
            mongoose_1.MongooseModule.forRoot(process.env.MONGO_URI || 'mongodb://localhost:27017/activation_api'),
            mongoose_1.MongooseModule.forFeature([{ name: activation_schema_1.Activation.name, schema: activation_schema_1.ActivationSchema }]),
            microservices_1.ClientsModule.register([
                {
                    name: 'KAFKA_CLIENT',
                    transport: microservices_1.Transport.KAFKA,
                    options: {
                        client: {
                            clientId: process.env.KAFKA_CLIENT_ID || 'activation-api',
                            brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
                        },
                        consumer: {
                            groupId: 'activation-api-producer',
                        },
                    },
                },
            ]),
        ],
        controllers: [activation_controller_1.ActivationController],
        providers: [activation_service_1.ActivationService, activation_gateway_1.ActivationGateway],
    })
], AppModule);
//# sourceMappingURL=app.module.js.map