import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
        clientId: process.env.KAFKA_CLIENT_ID || 'notification-svc',
      },
      consumer: {
        groupId: process.env.KAFKA_GROUP_ID || 'notification-svc',
      },
    },
  });

  await app.listen();
  console.log('Notification Service is listening...');
}
bootstrap();
