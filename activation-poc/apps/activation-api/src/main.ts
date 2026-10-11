import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
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
