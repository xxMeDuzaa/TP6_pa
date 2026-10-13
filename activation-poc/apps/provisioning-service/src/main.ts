import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { kafkaConsumerOptions } from '@activation-poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    kafkaConsumerOptions({
      clientId: process.env.KAFKA_CLIENT_ID || 'provisioning-svc',
      groupId: process.env.KAFKA_GROUP_ID || 'provisioning-svc',
    }),
  );

  await app.listen();
  console.log('Provisioning Service is listening...');
}
bootstrap();
