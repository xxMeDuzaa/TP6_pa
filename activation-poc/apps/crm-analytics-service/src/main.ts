import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { kafkaConsumerOptions } from '@activation-poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    kafkaConsumerOptions({
      clientId: process.env.KAFKA_CLIENT_ID || 'crm-analytics-svc',
      groupId: process.env.KAFKA_GROUP_ID || 'crm-analytics',
      // RF-10: un grupo nuevo de crm-analytics reprocesa todo el historial desde el offset 0
      fromBeginning: true,
    }),
  );

  await app.listen();
  console.log('CRM Analytics Service is listening...');
}
bootstrap();
