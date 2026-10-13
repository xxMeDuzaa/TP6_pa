import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { kafkaConsumerOptions } from '@activation-poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();

  app.connectMicroservice<MicroserviceOptions>(
    kafkaConsumerOptions({
      clientId: process.env.KAFKA_CLIENT_ID || 'activation-api',
      groupId: process.env.KAFKA_GROUP_ID || 'activation-api',
    }),
  );

  await app.startAllMicroservices();
  await app.listen(3000);
  console.log('Activation API listening on port 3000');
}
bootstrap();
