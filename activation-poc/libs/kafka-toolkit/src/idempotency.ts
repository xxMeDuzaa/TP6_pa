import { Logger } from '@nestjs/common';
import type { ClientSession, Connection, Model } from 'mongoose';
import { lastValueFrom } from 'rxjs';
import { KafkaEmitter, KafkaOutgoingMessage, NonRetriableError } from './retry-dlq';

export interface IdempotentEvent {
  eventId: string;
  eventType?: string;
}

export interface IdempotencyOptions {
  /** Modelo de la colección processed_events del servicio. */
  processedModel: Model<any>;
  /** Si se pasa, todo corre dentro de una transacción de MongoDB (requiere replica set). */
  connection?: Connection | null;
  /** Productor para los eventos que el handler publique con ctx.publish(). */
  emitter?: KafkaEmitter | null;
  /** Topic de origen; se guarda en processed_events como dato de auditoría. */
  topic?: string;
}

export interface HandlerContext {
  /** Sesión de la transacción: pasala a TODAS las escrituras ({ session }). Null sin transacción. */
  session: ClientSession | null;
  /**
   * Encola un mensaje Kafka. Se publica al final, después de las escrituras y antes del
   * commit: si la publicación falla, la transacción se aborta y el evento se reintenta
   * (at-least-once; los consumidores son idempotentes).
   */
  publish(topic: string, message: KafkaOutgoingMessage): void;
}

export type IdempotencyResult = 'processed' | 'duplicate';

const logger = new Logger('Idempotency');

function isDuplicateEventIdError(err: any): boolean {
  return err?.code === 11000 && (err?.keyPattern?.eventId !== undefined || /eventId/.test(err?.message ?? ''));
}

/**
 * Procesa un evento exactamente una vez por servicio (RNF-03):
 *  1. Abre una sesión/transacción de Mongoose.
 *  2. Si el eventId ya está en processed_events → lo ignora.
 *  3. Ejecuta el handler pasando la sesión.
 *  4. Inserta el eventId en processed_events dentro de la MISMA transacción.
 *  5. Publica los eventos encolados y commitea.
 *
 * Si algo falla, la transacción se aborta (ni el efecto ni la marca quedan escritos) y
 * el error se propaga para que @RetryWithDlq lo reintente.
 */
export async function processIdempotent<E extends IdempotentEvent>(
  event: E,
  options: IdempotencyOptions,
  handler: (event: E, ctx: HandlerContext) => Promise<void>,
): Promise<IdempotencyResult> {
  if (!event || typeof event !== 'object' || !event.eventId) {
    throw new NonRetriableError('Mensaje inválido: falta eventId');
  }

  const { processedModel, connection, emitter, topic } = options;

  const runOnce = async (session: ClientSession | null): Promise<IdempotencyResult> => {
    const already = await processedModel.exists({ eventId: event.eventId }).session(session);
    if (already) return 'duplicate';

    const outbox: Array<{ topic: string; message: KafkaOutgoingMessage }> = [];
    await handler(event, {
      session,
      publish: (t, message) => outbox.push({ topic: t, message }),
    });

    await processedModel.create(
      [{ eventId: event.eventId, eventType: event.eventType, topic }],
      session ? { session } : {},
    );

    if (outbox.length && !emitter) {
      throw new Error('El handler publicó mensajes pero no hay emitter configurado');
    }
    for (const out of outbox) {
      await lastValueFrom(emitter!.emit(out.topic, out.message), { defaultValue: undefined });
    }
    return 'processed';
  };

  try {
    let result: IdempotencyResult = 'processed';
    if (connection) {
      // connection.transaction() reintenta solo los errores transitorios de Mongo
      // (TransientTransactionError / UnknownTransactionCommitResult).
      await connection.transaction(async (session) => {
        result = await runOnce(session);
      });
    } else {
      result = await runOnce(null);
    }
    if (result === 'duplicate') {
      logger.log(`[IDEMPOTENCIA] ${event.eventType ?? 'evento'} ${event.eventId} ya procesado; se ignora`);
    }
    return result;
  } catch (err) {
    // Carrera entre dos consumidores con el mismo eventId: el índice único decide.
    if (isDuplicateEventIdError(err)) {
      logger.log(`[IDEMPOTENCIA] ${event.eventId} insertado en paralelo por otra instancia; se ignora`);
      return 'duplicate';
    }
    throw err;
  }
}
