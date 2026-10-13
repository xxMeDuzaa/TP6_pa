import { KafkaOptions, Transport } from '@nestjs/microservices';

export interface KafkaConsumerConfig {
  clientId: string;
  groupId: string;
  /**
   * auto.offset.reset. Default true (earliest): si el grupo no tiene offset commiteado en una
   * partición, la lee desde el inicio. Con "latest", un consumidor que se cae antes de haber
   * procesado algo en una partición se saltearía los mensajes llegados mientras estaba caído
   * (violaría RNF-05). Releer no duplica efectos porque los consumidores son idempotentes.
   */
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
      subscribe: { fromBeginning: config.fromBeginning ?? true },
      run: { autoCommit: false },
    },
  };
}
