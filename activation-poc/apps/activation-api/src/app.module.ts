import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { MongooseModule } from '@nestjs/mongoose';
import { ActivationController } from './activation.controller';
import { ActivationService } from './activation.service';
import { ActivationGateway } from './activation.gateway';
import { Activation, ActivationSchema } from './schemas/activation.schema';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI || 'mongodb://localhost:27017/activation_api'),
    MongooseModule.forFeature([{ name: Activation.name, schema: ActivationSchema }]),
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
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
  controllers: [ActivationController],
  providers: [ActivationService, ActivationGateway],
})
export class AppModule {}
