import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { MongooseModule } from '@nestjs/mongoose';
import { CrmController } from './crm.controller';
import { EventLog, EventLogSchema } from './schemas/event-log.schema';
import { ProcessedEvent, ProcessedEventSchema } from './schemas/processed-event.schema';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI || 'mongodb://localhost:27017/crm_analytics_service?directConnection=true'),
    MongooseModule.forFeature([
      { name: EventLog.name, schema: EventLogSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
    // Productor: eventos de negocio (si corresponde) y mensajes a <topic>.dlq
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: `${process.env.KAFKA_CLIENT_ID || 'crm-analytics-svc'}-producer`,
            brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [CrmController],
})
export class AppModule {}
