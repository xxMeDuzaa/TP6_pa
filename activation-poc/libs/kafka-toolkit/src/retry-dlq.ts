import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { lastValueFrom, Observable } from 'rxjs';

/**
 * Lo mínimo que necesitamos de un productor Kafka. `ClientKafka` de Nest lo cumple.
 */
export interface KafkaEmitter {
  emit(pattern: string, data: any): Observable<any>;
}

export interface KafkaOutgoingMessage {
  key?: string | null;
  value: any;
  headers?: Record<string, string>;
}

/** Backoff exponencial pedido por RNF-04: 1s, 2s, 4s. */
export const DEFAULT_BACKOFF_MS: readonly number[] = [1000, 2000, 4000];
/** Cantidad de reintentos DESPUÉS del intento original (1 intento + 3 reintentos). */
export const DEFAULT_MAX_RETRIES = 3;

export const dlqTopicOf = (topic: string): string => `${topic}.dlq`;

/**
 * Error que NO tiene sentido reintentar (mensaje mal formado, validación, etc.).
 * Se manda directo a la DLQ sin esperar el backoff.
 */
export class NonRetriableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NonRetriableError';
  }
}

export interface RetryDlqOptions {
  /** Topic desde el que se consumió el mensaje. La DLQ será `<topic>.dlq`. */
  topic: string;
  /** Reintentos después del intento original. Default: 3. */
  maxRetries?: number;
  /** Espera antes de cada reintento. Default: [1000, 2000, 4000]. */
  backoffMs?: readonly number[];
  /** Nombre del consumidor, sólo para logs y para el payload de la DLQ. */
  consumer?: string;
}

export interface ExecuteWithRetryOptions extends RetryDlqOptions {
  /** Mensaje original (se publica tal cual en la DLQ). */
  message: any;
  /** Key original del mensaje en Kafka (se conserva en la DLQ). */
  originalKey?: string | null;
  /** Productor usado para publicar en la DLQ. */
  emitter?: KafkaEmitter | null;
}

const logger = new Logger('KafkaRetryDlq');

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function delayFor(backoffMs: readonly number[], retryNumber: number): number {
  // retryNumber empieza en 1. Si se piden más reintentos que valores, se sigue duplicando.
  if (retryNumber <= backoffMs.length) return backoffMs[retryNumber - 1];
  const last = backoffMs[backoffMs.length - 1] ?? 1000;
  return last * Math.pow(2, retryNumber - backoffMs.length);
}

function describe(message: any): string {
  if (!message || typeof message !== 'object') return String(message);
  return `${message.eventType ?? '?'}#${message.eventId ?? '?'}`;
}

/**
 * Publica en `<topic>.dlq` el MENSAJE ORIGINAL (mismo value y misma key) y deja el motivo
 * del error en los headers, para inspeccionarlo en Kafka UI o reinyectarlo tal cual.
 */
export async function sendToDlq(
  emitter: KafkaEmitter | null | undefined,
  topic: string,
  message: any,
  error: any,
  attempts: number,
  consumer?: string,
  originalKey?: string | null,
): Promise<void> {
  const dlqTopic = dlqTopicOf(topic);
  if (!emitter) {
    logger.error(`[DLQ] No hay productor configurado; no se pudo enviar ${describe(message)} a ${dlqTopic}`);
    return;
  }

  const errorName = error?.name ?? 'Error';
  const errorMessage = String(error?.message ?? error ?? 'Unknown error');

  const outgoing: KafkaOutgoingMessage = {
    key: originalKey ?? message?.customerId ?? null,
    value: message,
    headers: {
      'x-original-topic': topic,
      'x-exception-type': errorName,
      'x-exception-message': errorMessage.slice(0, 1000),
      'x-attempts': String(attempts),
      'x-consumer': consumer ?? 'unknown',
      'x-failed-at': new Date().toISOString(),
    },
  };

  await lastValueFrom(emitter.emit(dlqTopic, outgoing), { defaultValue: undefined });
  logger.warn(`[DLQ] ${describe(message)} enviado a ${dlqTopic} tras ${attempts} intento(s): ${errorMessage}`);
}

/**
 * Ejecuta `fn` con reintentos exponenciales y, si sigue fallando, publica en la DLQ.
 *
 * Importante: NUNCA relanza el error del handler. Así el consumer de Nest termina
 * el mensaje con éxito, el offset se commitea y la partición sigue avanzando
 * (el mensaje problemático queda aparcado en la DLQ en vez de bloquear la cola).
 *
 * @returns el resultado de `fn`, o `undefined` si terminó en la DLQ.
 */
export async function executeWithRetryAndDlq<T>(
  fn: (attempt: number) => Promise<T>,
  options: ExecuteWithRetryOptions,
): Promise<T | undefined> {
  const maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
  const backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
  const totalAttempts = maxRetries + 1;
  const tag = `[${options.consumer ?? 'consumer'}] ${options.topic} ${describe(options.message)}`;

  let lastError: any;
  for (let attempt = 1; attempt <= totalAttempts; attempt++) {
    try {
      const result = await fn(attempt);
      if (attempt > 1) logger.log(`${tag} procesado en el intento ${attempt}/${totalAttempts}`);
      return result;
    } catch (err: any) {
      lastError = err;
      if (err instanceof NonRetriableError) {
        logger.error(`${tag} error no reintentable: ${err.message}`);
        break;
      }
      if (attempt < totalAttempts) {
        const wait = delayFor(backoffMs, attempt);
        logger.warn(`${tag} intento ${attempt}/${totalAttempts} falló (${err?.message}). Reintento en ${wait} ms`);
        await sleep(wait);
      } else {
        logger.error(`${tag} intento ${attempt}/${totalAttempts} falló (${err?.message}). Sin más reintentos.`);
      }
    }
  }

  const attemptsMade = lastError instanceof NonRetriableError ? 1 : totalAttempts;
  try {
    await sendToDlq(options.emitter, options.topic, options.message, lastError, attemptsMade, options.consumer, options.originalKey);
  } catch (dlqErr: any) {
    // Si ni siquiera se puede escribir en la DLQ, lo dejamos bien visible en los logs.
    logger.error(`${tag} NO se pudo publicar en ${dlqTopicOf(options.topic)}: ${dlqErr?.message}`);
  }
  return undefined;
}

export interface RetryWithDlqDecoratorOptions extends RetryDlqOptions {
  /** Propiedad de la instancia que tiene el ClientKafka. Default: 'kafkaClient'. */
  clientProperty?: string;
  /** Índice del argumento que trae el payload (el del @Payload()). Default: 0. */
  messageArgIndex?: number;
  /** Índice del argumento con el KafkaContext (el del @Ctx()). Default: 1. */
  contextArgIndex?: number;
}

/** Lo que usamos del KafkaContext de Nest (evita acoplar el toolkit a su clase). */
interface KafkaContextLike {
  getTopic(): string;
  getPartition(): number;
  getMessage(): { offset: string; key?: Buffer | string | null };
  getConsumer(): { commitOffsets(offsets: Array<{ topic: string; partition: number; offset: string }>): Promise<void> };
}

function isKafkaContext(value: any): value is KafkaContextLike {
  return !!value && typeof value.getConsumer === 'function' && typeof value.getMessage === 'function';
}

/**
 * Commit MANUAL del offset (requiere `run.autoCommit = false`, ver kafkaConsumerOptions).
 * Se commitea offset + 1 = "el próximo mensaje a leer".
 */
export async function commitOffset(context: KafkaContextLike): Promise<void> {
  const next = (BigInt(context.getMessage().offset) + 1n).toString();
  await context.getConsumer().commitOffsets([
    { topic: context.getTopic(), partition: context.getPartition(), offset: next },
  ]);
}

/**
 * Decorador de método para handlers `@EventPattern`:
 *
 * ```ts
 * @EventPattern(TOPICS.ACTIVATION_REQUESTED)
 * @RetryWithDlq(TOPICS.ACTIVATION_REQUESTED)
 * async handle(@Payload() event: ActivationEvent, @Ctx() _context: unknown) { ... } // KafkaContext
 * ```
 *
 * 1. Reintenta el método completo (1s, 2s, 4s).
 * 2. Si sigue fallando, publica el mensaje original en `<topic>.dlq` usando
 *    `this.kafkaClient` (configurable con `clientProperty`).
 * 3. En ambos casos commitea el offset DESPUÉS de procesar, y la partición sigue.
 *    Si el servicio se cae antes del commit, el mensaje se vuelve a entregar
 *    (la idempotencia evita efectos duplicados).
 *
 * Copia la metadata de Nest, así que funciona encima o debajo de @EventPattern.
 */
export function RetryWithDlq(topicOrOptions: string | RetryWithDlqDecoratorOptions): MethodDecorator {
  const opts: RetryWithDlqDecoratorOptions =
    typeof topicOrOptions === 'string' ? { topic: topicOrOptions } : topicOrOptions;

  return (target: object, propertyKey: string | symbol, descriptor: PropertyDescriptor) => {
    const original = descriptor.value as (...args: any[]) => Promise<any>;
    const clientProperty = opts.clientProperty ?? 'kafkaClient';
    const consumer = opts.consumer ?? `${target.constructor.name}.${String(propertyKey)}`;

    const wrapped = async function (this: any, ...args: any[]) {
      const context = args[opts.contextArgIndex ?? 1];
      const kafkaContext = isKafkaContext(context) ? context : undefined;
      const rawKey = kafkaContext?.getMessage().key;

      const result = await executeWithRetryAndDlq(() => original.apply(this, args), {
        ...opts,
        consumer,
        message: args[opts.messageArgIndex ?? 0],
        originalKey: rawKey == null ? null : rawKey.toString(),
        emitter: this?.[clientProperty],
      });

      if (kafkaContext) {
        try {
          await commitOffset(kafkaContext);
        } catch (err: any) {
          // No es grave: el mensaje se reentrega y la idempotencia lo descarta.
          logger.warn(`[${consumer}] no se pudo commitear el offset: ${err?.message}`);
        }
      } else {
        logger.warn(`[${consumer}] sin @Ctx(): no se puede commitear el offset manualmente`);
      }
      return result;
    };

    for (const key of Reflect.getMetadataKeys(original)) {
      Reflect.defineMetadata(key, Reflect.getMetadata(key, original), wrapped);
    }
    Object.defineProperty(wrapped, 'name', { value: original.name });
    descriptor.value = wrapped;
    return descriptor;
  };
}
