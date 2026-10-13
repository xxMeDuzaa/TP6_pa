import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { MongooseModule } from '@nestjs/mongoose';
import { ProvisioningController } from './provisioning.controller';
import { ProvisioningOrder, ProvisioningOrderSchema } from './schemas/provisioning-order.schema';
import { ProcessedEvent, ProcessedEventSchema } from './schemas/processed-event.schema';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI || 'mongodb://localhost:27017/provisioning_service?directConnection=true'),
    MongooseModule.forFeature([
      { name: ProvisioningOrder.name, schema: ProvisioningOrderSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
    // Productor: eventos de negocio (si corresponde) y mensajes a <topic>.dlq
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: `${process.env.KAFKA_CLIENT_ID || 'provisioning-svc'}-producer`,
            brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [ProvisioningController],
})
export class AppModule {}
