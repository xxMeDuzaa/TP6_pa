import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { kafkaConsumerOptions } from '@activation-poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    kafkaConsumerOptions({
      clientId: process.env.KAFKA_CLIENT_ID || 'billing-svc',
      groupId: process.env.KAFKA_GROUP_ID || 'billing-svc',
    }),
  );

  await app.listen();
  console.log('Billing Service is listening...');
}
bootstrap();
