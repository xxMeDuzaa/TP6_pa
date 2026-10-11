"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const core_1 = require("@nestjs/core");
const app_module_1 = require("./app.module");
const microservices_1 = require("@nestjs/microservices");
async function bootstrap() {
    const app = await core_1.NestFactory.create(app_module_1.AppModule);
    app.enableCors();
    app.connectMicroservice({
        transport: microservices_1.Transport.KAFKA,
        options: {
            client: {
                brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
                clientId: process.env.KAFKA_CLIENT_ID || 'activation-api',
            },
            consumer: {
                groupId: process.env.KAFKA_GROUP_ID || 'activation-api-group',
            },
        },
    });
    await app.startAllMicroservices();
    await app.listen(3000);
    console.log('Activation API listening on port 3000');
}
bootstrap();
//# sourceMappingURL=main.js.map