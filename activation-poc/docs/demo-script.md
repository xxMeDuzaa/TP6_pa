# Guion de demo — POC Activación de Servicios con Kafka

Cinco escenarios, cada uno demuestra un concepto de Kafka. Duración objetivo: **15 minutos**.
Pantalla dividida: **Demo UI a la izquierda, Kafka UI a la derecha**.

Todos los comandos se corren desde `activation-poc/`.

> **Windows / PowerShell:** los comandos `docker` son iguales. Para llamar a la API usá
> `Invoke-RestMethod` (ejemplos marcados **PS**) o `curl.exe`, no el alias `curl`.

---

## 0. Preparación (antes de presentar)

```bash
docker compose down -v          # arrancar limpio (borra datos de Mongo y offsets)
docker compose up -d --build    # RNF-01: un solo comando levanta todo
docker compose ps               # todo "running"; kafka-init termina solo
```

| Qué | URL |
|-----|-----|
| Demo UI | http://localhost:5173 |
| Kafka UI (kafbat) | http://localhost:8080 |
| MailHog | http://localhost:8025 |
| API | http://localhost:3000 |

Chequeo rápido de topics (3 particiones y `retention.ms=604800000` = 7 días):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic activation.requested
```

---

## Escenario 1 — Camino feliz · *Fan-out: un evento, varias reacciones*

**Qué se hace:** contratar un plan sin fallo.

1. Demo UI: cliente `C-1001`, *Plan Digital*, **Sin fallo** → *Contratar Plan*.
   Por API:
   ```bash
   curl -X POST http://localhost:3000/activations -H "Content-Type: application/json" \
     -d '{"customerId":"C-1001","planId":"PLAN_DIGITAL","simulateFailure":"none"}'
   ```
   **PS:**
   ```powershell
   Invoke-RestMethod -Method Post -Uri http://localhost:3000/activations -ContentType 'application/json' `
     -Body '{"customerId":"C-1001","planId":"PLAN_DIGITAL","simulateFailure":"none"}'
   ```
   La respuesta es inmediata: `202 { activationId, status: "PENDING" }` (RF-02).

**Qué se ve:**
- Timeline con **4 eventos en verde**: `ActivationRequested → BillingAccountCreated →
  ProvisioningCompleted → ActivationCompleted`; estado **ACTIVE**.
- MailHog: email *"¡Tu plan ha sido activado!"*.
- Kafka UI → `activation.requested`: **un** mensaje con key `C-1001` (RNF-02), consumido por
  tres grupos distintos: `billing-svc`, `provisioning-svc` y `crm-analytics`.
- `GET http://localhost:3000/activations/<activationId>` devuelve el estado y el historial (RF-09).

**Mensaje:** activation-api no llama a nadie; publica un hecho y cada servicio reacciona.

---

## Escenario 2 — Fallo y compensación · *Saga sin llamadas directas*

**Qué se hace:** contratar con **Fallo en Provisioning** (cliente `C-1002`).

**Qué se ve:**
- Timeline: `BillingAccountCreated` → `ProvisioningFailed` → `ActivationFailed` (estado
  **FAILED**) → `BillingAccountCancelled`.
- MailHog: email *"Problemas con la activación de tu plan"*.
- La cuenta de billing quedó anulada:
  ```bash
  docker exec mongo mongosh billing_service --quiet --eval 'db.billing_accounts.find({customerId:"C-1002"}).toArray()'
  ```
  → `status: "CANCELLED"`.

**Mensaje:** la compensación también es un evento: billing escucha `ActivationFailed` y deshace
lo suyo. Nadie le ordenó nada.

> Variante: con **Fallo en Billing** se ve `BillingFailed` → `ActivationFailed` y el
> `ProvisioningCompleted` que llega después sólo se agrega al historial (regla de agregación).

---

## Escenario 3 — Servicio caído · *Offset: nadie pierde mensajes*

> **Antes de la demo:** Kafka sólo puede mostrar el lag de una partición si el grupo ya tiene un
> offset commiteado en ella. Asegurate de que `notification-svc` haya procesado al menos un mensaje
> en **cada una de las 3 particiones** de `activation.events`: creá 5 o 6 activaciones con clientes
> distintos durante el Escenario 1. Para verificarlo, el comando de abajo tiene que listar las
> particiones 0, 1 y 2. Aunque alguna no aparezca, los mensajes **no se pierden**: los consumidores
> usan `auto.offset.reset=earliest` y la leen desde el inicio al volver.

**Qué se hace:**
```bash
docker compose stop notification-service
```
Crear **3 activaciones** sin fallo desde la UI (por ejemplo `C-3001`, `C-3002`, `C-3003`).

**Qué se ve:**
- Las activaciones llegan a **ACTIVE** igual: el resto del sistema no se enteró de la caída.
- MailHog no recibe nada.
- Kafka UI → *Consumers* → `notification-svc` → **lag = 3** en `activation.events`. Por CLI:
  ```bash
  docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notification-svc
  ```

Volver a levantarlo:
```bash
docker compose start notification-service
```
- Llegan **los 3 emails** a MailHog y el lag vuelve a 0 (RNF-05).

**Mensaje:** el offset se commitea a mano **después** de procesar; lo no confirmado se vuelve a
leer al reconectar.

---

## Escenario 4 — Escalar · *Consumer group y particiones*

**Qué se hace:**
```bash
docker compose up -d --scale billing-service=2
```

**Qué se ve:**
- Kafka UI → *Consumers* → `billing-svc`: las **3 particiones** de `activation.requested`
  repartidas entre **2 consumidores** (2 + 1). Por CLI:
  ```bash
  docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group billing-svc
  ```
- Crear varias activaciones con clientes distintos (keys distintas → particiones distintas) y ver
  en `docker compose logs -f billing-service` que trabajan `billing-service-1` y `billing-service-2`.

Durante el rebalanceo puede haber una pausa de unos segundos: es parte de lo que se muestra.

Volver a una instancia: `docker compose up -d --scale billing-service=1`.

**Mensaje:** escalar es agregar instancias al mismo grupo; Kafka reparte las particiones y el
orden por cliente se mantiene (key = `customerId`).

---

## Escenario 5 — Nuevo consumidor · *Replay del log*

**Qué se hace:** levantar `loyalty-service`, un servicio nuevo con un consumer group nuevo
(`loyalty-svc`) y `auto.offset.reset=earliest`:
```bash
docker compose --profile loyalty up -d loyalty-service
docker compose logs -f loyalty-service
```

**Qué se ve:**
- En los logs procesa **todo el historial** de `activation.events` desde el offset 0, incluidas las
  activaciones de los escenarios anteriores, que ocurrieron antes de que el servicio existiera:
  `p0@0 +100 pts a C-1001 ...`.
- Puntos reconstruidos:
  ```bash
  curl http://localhost:3005/loyalty
  ```
  Sólo suman las activaciones `ActivationCompleted` (C-1002 no tiene puntos).
- Ningún otro servicio se tocó ni se reinició.

**Mensaje de cierre:** esto no se puede hacer con APIs directas: un servicio nuevo se suma y
reconstruye su estado leyendo el log, sin que nadie le reenvíe nada.

---

## Anexo — Pruebas de los criterios de aceptación (fuera de los 15 min)

### A. Un mensaje inválido termina en la DLQ y el flujo sigue (RNF-04)

Publicar a mano un mensaje roto (sin `eventId`) en `activation.requested`:
```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
  --topic activation.requested --property parse.key=true --property key.separator='|' <<'EOF'
C-9999|{"eventType":"ActivationRequested","customerId":"C-9999"}
EOF
```
**PS:**
```powershell
'C-9999|{"eventType":"ActivationRequested","customerId":"C-9999"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic activation.requested --property parse.key=true --property key.separator='|'
```
- Logs (`docker compose logs billing-service`): `error no reintentable: Mensaje inválido: falta eventId`
  y `[DLQ] ... enviado a activation.requested.dlq`.
- Kafka UI → `activation.requested.dlq`: el mensaje original con los headers `x-original-topic`,
  `x-exception-type`, `x-exception-message`, `x-attempts` y `x-consumer`. Aparece una copia por
  cada grupo que lo consume (billing, provisioning y crm-analytics).
- Crear otra activación normal: se completa sin problemas (la partición no quedó bloqueada).

Para ver los **reintentos 1s/2s/4s** de un fallo técnico, elegir en la UI **Fallo técnico en
Billing (DLQ)**:
```
... intento 1/4 falló (...). Reintento en 1000 ms
... intento 2/4 falló (...). Reintento en 2000 ms
... intento 3/4 falló (...). Reintento en 4000 ms
... intento 4/4 falló (...). Sin más reintentos.
[DLQ] ActivationRequested#... enviado a activation.requested.dlq tras 4 intento(s)
```
Esa activación queda en `IN_PROGRESS`: el mensaje en la DLQ requiere intervención manual.

### B. Reenviar un evento ya procesado no duplica cuentas ni emails (RNF-03)

Forzar que billing y notification relean todo:
```bash
docker compose stop billing-service notification-service
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group billing-svc \
  --all-topics --reset-offsets --to-earliest --execute
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group notification-svc \
  --all-topics --reset-offsets --to-earliest --execute
docker compose start billing-service notification-service
```
- Logs: `[IDEMPOTENCIA] ... ya procesado; se ignora` para cada evento.
- Ni cuentas ni emails nuevos:
  ```bash
  docker exec mongo mongosh billing_service --quiet --eval 'db.billing_accounts.countDocuments()'
  docker exec mongo mongosh notification_service --quiet --eval 'db.notifications.countDocuments()'
  ```
  (mismos números que antes; MailHog sin mails nuevos).
- Índices de idempotencia (único en `eventId` + TTL de 7 días):
  ```bash
  docker exec mongo mongosh billing_service --quiet --eval 'db.processed_events.getIndexes()'
  ```

### C. Rastrear una activación por correlationId (RNF-06)

```bash
docker exec mongo mongosh crm_analytics_service --quiet --eval 'db.event_log.find({correlationId:"<activationId>"},{_id:0,"payload.eventType":1,"payload.source":1,topic:1}).toArray()'
```
Devuelve todos los eventos de esa activación, de todos los servicios.

### D. crm-analytics reprocesa el historial desde el inicio (RF-10)

```bash
docker compose stop crm-analytics-service
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group crm-analytics \
  --all-topics --reset-offsets --to-earliest --execute
docker compose start crm-analytics-service
```
Relee todos los topics; la idempotencia evita duplicar `event_log`. Un grupo nuevo de
crm-analytics (otro `KAFKA_GROUP_ID`) también arranca desde el inicio (`fromBeginning: true`).

---

## Cierre

```bash
docker compose --profile loyalty down     # agregar -v para borrar también los datos
```
