import { KafkaOptions, Transport } from '@nestjs/microservices';

export interface KafkaConsumerConfig {
  clientId: string;
  groupId: string;
  /** true → lee el topic desde el inicio si el grupo no tiene offsets (auto.offset.reset=earliest). */
  fromBeginning?: boolean;
}

/**
 * Configuración común del consumidor Kafka de cada servicio NestJS:
 *  - `postfixId: ''`  → el consumer group se llama exactamente como KAFKA_GROUP_ID
 *                       (Nest agrega "-server" por defecto).
 *  - `autoCommit: false` → commit MANUAL del offset, siempre después de procesar
 *                       (lo hace @RetryWithDlq al terminar, con éxito o vía DLQ).
 */
export function kafkaConsumerOptions(config: KafkaConsumerConfig): KafkaOptions {
  return {
    transport: Transport.KAFKA,
    options: {
      postfixId: '',
      client: {
        clientId: config.clientId,
        brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
      },
      consumer: {
        groupId: config.groupId,
      },
      subscribe: { fromBeginning: config.fromBeginning ?? false },
      run: { autoCommit: false },
    },
  };
}
