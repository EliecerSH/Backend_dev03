# ms-pagos (puerto 8087)

Pasarela de pago **WebPay Plus** (SDK oficial Transbank 6.2.1) con **detección de fraude por velocity check sobre Kafka**.
Java 17 · Spring Boot 4.1 · PostgreSQL (schema `pagos`) · Kafka · Azure AD (resource server).

Se despliega junto con Kafka y ms-recomendaciones desde el `docker-compose.yml` de `Backend_dev03` (ver README principal).

## API

| Método | Ruta | Auth | Descripción |
|---|---|---|---|
| POST | `/api/pagos/iniciar` | User/Admin | `{ "orderId": 123 }` → `{ orderId, url, token }` |
| GET/POST | `/api/pagos/retorno` | pública | `return_url` de WebPay. Hace `commit` y redirige a `FRONTEND_URL/pedidos?paymentStatus=…&orderId=…` |
| GET | `/api/pagos/orden/{orderId}` | dueño o Admin | Estado del pago de la orden |
| GET | `/actuator/health` | pública | Salud (liveness/readiness) |

Códigos de `/iniciar`: `429` bloqueo antifraude (incluye `blockedUntil`) · `403` orden de otro usuario · `404` orden inexistente · `409` orden ya pagada / pago en curso · `503` ms-ordenes o Kafka no disponible.

El cliente debe enviar un **formulario POST** a `url` con el campo oculto `token_ws` (ya implementado en `Frontend_dev/src/api/apiService.js → redirigirAWebpay`).

`paymentStatus` posibles: `AUTHORIZED`, `REJECTED`, `CANCELLED` (anulación o timeout en WebPay), `ERROR`.

## Kafka

| Tópico | Rol | Payload |
|---|---|---|
| `pagos.intentos` | produce y consume (group `ms-pagos-antifraude`) | `{clientId, amount, transactionId, timestamp, orderId}` |
| `pagos.resultados` | produce | `{eventId, orderId, clientId, status, amount, authorizationCode, items[], occurredAt}` |
| `pagos.alertas-fraude` | produce | `{eventId, clientId, tipo, intentosEnVentana, ventanaSegundos, orderId, bloqueadoHasta, occurredAt}` |

Además publica `pedidos360.pago.{iniciado|authorized|rejected|cancelled|bloqueado}` en el exchange RabbitMQ existente para que ms-auditoria los registre (best effort; RabbitMQ caído no afecta el pago).

## Antifraude

1. `/iniciar` revisa si el cliente tiene un bloqueo vigente (gate síncrono) → `429`.
2. Publica el intento en `pagos.intentos` con key `clientId` (orden por cliente) y espera el ack de Kafka (`acks=all`, idempotente).
3. `IntentoPagoConsumer` guarda el intento (idempotente por `transactionId`) y cuenta los intentos en los últimos `FRAUD_WINDOW_SECONDS` bajo bloqueo de fila (`SELECT … FOR UPDATE`).
4. Si alcanza `FRAUD_MAX_ATTEMPTS` → bloquea `FRAUD_BLOCK_MINUTES` y publica `BLOQUEO_APLICADO`.
5. Un retorno de WebPay (aprobado, rechazado o anulado) limpia el contador del cliente.

## Tests
`mvn test` — `AntifraudeServiceTest` cubre umbral, bloqueo + alerta, duplicados y expiración del bloqueo.

## Referencias
- [Webpay Plus — documentación Transbank](https://www.transbankdevelopers.cl/documentacion/webpay-plus)
- [SDK Java oficial](https://github.com/TransbankDevelopers/transbank-sdk-java)
