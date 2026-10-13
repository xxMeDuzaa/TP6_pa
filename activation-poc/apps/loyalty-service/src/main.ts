/**
 * loyalty-service — Escenario 5: "un consumidor nuevo se suma tarde y reconstruye
 * su estado leyendo el historial completo del topic".
 *
 * Es deliberadamente liviano: kafkajs puro (sin Nest ni MongoDB) y estado en memoria.
 *
 * auto.offset.reset = earliest:
 *   En kafkajs esa propiedad se expresa como `fromBeginning: true` en subscribe().
 *   Aplica sólo cuando el consumer group NO tiene offsets commiteados: la primera vez
 *   que arranca el grupo `loyalty-svc` lee activation.events desde el offset 0.
 *   Para repetir la demo desde cero, cambiá KAFKA_GROUP_ID (p. ej. loyalty-svc-2).
 */
import * as http from 'node:http';
import { Kafka, logLevel } from 'kafkajs';
import { ActivationEvent, TOPICS } from '@activation-poc/contracts';

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'loyalty-svc';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'loyalty-svc';
const AUTO_OFFSET_RESET = (process.env.AUTO_OFFSET_RESET || 'earliest').toLowerCase();
const POINTS_PER_ACTIVATION = parseInt(process.env.POINTS_PER_ACTIVATION || '100', 10);
const PORT = parseInt(process.env.PORT || '3005', 10);

interface CustomerLoyalty {
  customerId: string;
  points: number;
  activations: string[];
  lastEventAt: string;
}

const customers = new Map<string, CustomerLoyalty>();
const seenEventIds = new Set<string>(); // idempotencia en memoria
const stats = { consumed: 0, awarded: 0, startedAt: new Date().toISOString(), firstOffsetSeen: {} as Record<number, string> };

function award(event: ActivationEvent) {
  const current =
    customers.get(event.customerId) ??
    ({ customerId: event.customerId, points: 0, activations: [], lastEventAt: event.occurredAt } as CustomerLoyalty);
  current.points += POINTS_PER_ACTIVATION;
  current.activations.push(event.correlationId);
  current.lastEventAt = event.occurredAt;
  customers.set(event.customerId, current);
  stats.awarded++;
}

async function startConsumer() {
  const kafka = new Kafka({ clientId: CLIENT_ID, brokers: BROKERS, logLevel: logLevel.WARN });
  const consumer = kafka.consumer({ groupId: GROUP_ID });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.ACTIVATION_EVENTS, fromBeginning: AUTO_OFFSET_RESET === 'earliest' });
  console.log(`[loyalty] grupo=${GROUP_ID} topic=${TOPICS.ACTIVATION_EVENTS} auto.offset.reset=${AUTO_OFFSET_RESET}`);

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      stats.consumed++;
      stats.firstOffsetSeen[partition] ??= message.offset;

      let event: ActivationEvent;
      try {
        event = JSON.parse(message.value?.toString() ?? '');
      } catch {
        console.warn(`[loyalty] p${partition}@${message.offset} mensaje no JSON, se ignora`);
        return;
      }
      if (!event?.eventId || seenEventIds.has(event.eventId)) return;
      seenEventIds.add(event.eventId);

      if (event.eventType === 'ActivationCompleted') {
        award(event);
        console.log(
          `[loyalty] p${partition}@${message.offset} +${POINTS_PER_ACTIVATION} pts a ${event.customerId} ` +
            `(total ${customers.get(event.customerId)!.points}) por ${event.correlationId}`,
        );
      }
    },
  });

  const shutdown = async () => {
    await consumer.disconnect().catch(() => undefined);
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function startHttp() {
  http
    .createServer((req, res) => {
      const url = req.url ?? '/';
      const send = (status: number, body: unknown) => {
        res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify(body, null, 2));
      };
      if (url === '/health') return send(200, { status: 'ok' });
      if (url === '/loyalty') return send(200, { groupId: GROUP_ID, stats, customers: [...customers.values()] });
      const match = url.match(/^\/loyalty\/([^/]+)$/);
      if (match) {
        const c = customers.get(decodeURIComponent(match[1]));
        return c ? send(200, c) : send(404, { error: 'cliente sin puntos' });
      }
      send(404, { error: 'not found' });
    })
    .listen(PORT, () => console.log(`[loyalty] HTTP en :${PORT} (GET /loyalty, /loyalty/:customerId)`));
}

startHttp();
startConsumer().catch((err) => {
  console.error('[loyalty] error fatal', err);
  process.exit(1);
});
