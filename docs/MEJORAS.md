# Mejoras aplicadas y observaciones atendidas

> No se contó con el documento de retroalimentación de las evaluaciones anteriores; esta lista se construyó
> revisando el código entregado (Backend_dev, Backend_dev02, Frontend_dev, ms-pagos previo y el ejemplo
> CloudNative-Recomendaciones). Si hay observaciones puntuales del docente, agregarlas aquí con su estado.

## Requisitos de esta entrega

| Requisito | Cómo se cumple |
|---|---|
| Kafka, mínimo 2 tópicos | 4 tópicos de dominio (`pagos.intentos`, `pagos.resultados`, `pagos.alertas-fraude`, `recomendaciones.interacciones`) + DLT, broker KRaft en `docker-compose.yml` |
| ms-pagos con detección de fraude | Velocity check sobre Kafka: 3 intentos / 10 s → bloqueo 30 min, idempotente y seguro ante concurrencia (bloqueo de fila). Alertas en `pagos.alertas-fraude` |
| WebPay en el botón de pago (+5) | SDK oficial Transbank 6.2.1. Botón **"Pagar con WebPay"** en el carrito y en "Mis pedidos" (reintento), retorno con commit en backend, manejo de anulación y timeout |
| ms-recomendaciones simple | Motor explicable (afinidad por categoría con decaimiento, popularidad, co-ocurrencia) alimentado por Kafka y persistido en **Amazon DynamoDB** (tabla única, transacciones idempotentes, TTL). Bloques "Recomendado para ti" / "Lo más popular" en el home y "También te puede interesar" en la ficha de producto |
| ¿Colas → Kafka? | Explicado en [RABBITMQ_VS_KAFKA.md](RABBITMQ_VS_KAFKA.md): stock y notificaciones siguen en RabbitMQ; **la auditoría se migró a Kafka** (`auditoria.eventos`); lo nuevo va en Kafka |

## Correcciones de negocio

1. **Se descontaba stock y se enviaba el correo antes de pagar.** Ahora, con `PAGO_REQUERIDO=true`, la orden nace `PENDIENTE_PAGO` y `orden.creada` se publica solo cuando llega `AUTHORIZED` por Kafka. Un pago rechazado deja la orden en `PAGO_RECHAZADO` y se puede reintentar.
2. **El monto lo decide el backend:** ms-pagos obtiene el total y el dueño de la orden desde ms-ordenes con el token del usuario. El navegador solo envía el `orderId`.
3. **El resultado del pago se confirma en el backend** (`commit`), nunca se acepta un estado enviado por el navegador. Doble refresco del retorno no repite el commit.
4. **El carrito ya no se vacía antes de pagar:** se vacía al volver con `AUTHORIZED`.

## Calidad y arquitectura

| Antes | Ahora |
|---|---|
| ms-pagos en el puerto 8083, el mismo de ms-carrito | ms-pagos 8087, ms-recomendaciones 8088 |
| ms-pagos era un repositorio git anidado con su propio compose (PostgreSQL, RabbitMQ y Kafka locales) | Un solo stack `Backend_dev03` con compose, `.env.example` y workflow CI/CD iguales al resto del proyecto |
| Sin manejo de mensajes envenenados | `ErrorHandlingDeserializer` + 2 reintentos + **Dead Letter Topic** (`*.DLT`) |
| Si Kafka fallaba al publicar el resultado, el evento se perdía | **Outbox simplificado:** el pago queda `resultado_publicado=false` y un job lo republica cada 30 s; `eventId` determinista para que los consumidores descarten duplicados |
| Consumidores no idempotentes | `transactionId` / `event_id` únicos: un evento re-entregado no se cuenta dos veces |
| Todas las tablas en el schema `public` compartido | **Base de datos por servicio**: ms-pagos en su schema `pagos` de PostgreSQL; ms-recomendaciones en **DynamoDB** (servicio administrado de AWS, como en el ejemplo CloudNative) |
| `System.out.println` en ms-ordenes | SLF4J con niveles |
| `show-sql: true` y `org.springframework.security: DEBUG` en ms-ordenes | `show-sql: false`, nivel INFO |
| Sin timeouts en llamadas REST entre servicios | Connect 3 s / read 5 s → 503 claro en vez de cuelgues |
| Sin healthchecks | Actuator liveness/readiness + `healthcheck` en compose; Kafka con healthcheck y `kafka-init` que crea tópicos antes de levantar los servicios |
| Contenedores como root | Usuario `spring` sin privilegios en las imágenes |
| Tópicos creados por accidente | `auto.create.topics.enable=false`, creación explícita e idempotente |
| Ejemplo CloudNative: `.env` con IP versionado, `@CrossOrigin("*")`, endpoints sin autenticación | Secrets/`.env` fuera de git, CORS restringido, Azure AD (mismo tenant), endpoints de escritura autenticados y simulación solo para Admin |
| Sin tests en CI | Tests unitarios del motor de recomendaciones, de las claves DynamoDB y del antifraude; el workflow los ejecuta antes de construir las imágenes |
| Frontend: errores de ms-pagos (`message`) no se mostraban | `parseError` lee `mensaje`, `message` y `error` |

## Revisión de código (versión con auditoría en Kafka)

Errores encontrados al revisar todo el proyecto y cómo se corrigieron:

| # | Servicio | Problema | Gravedad | Corrección |
|---|---|---|---|---|
| 1 | ms-ordenes | El precio de cada ítem se tomaba del request del navegador: editando el request se podía crear una orden de $1 y pagar $1 en WebPay | **Crítica** | El precio y el stock se leen de ms-productos (`PRODUCTOS_SERVICE_URL`); el `precioUnitario` del request se ignora. Se unifican ítems repetidos y se limita la cantidad a 100 |
| 2 | ms-notificaciones | Servicio sin autenticación con `POST /test-email` público: cualquiera podía enviar correos desde la cuenta SMTP del proyecto | **Crítica** | El endpoint solo existe si `NOTIFICACIONES_TEST_ENDPOINT=true` (apagado por defecto) |
| 3 | ms-productos | Al descontar stock de una orden con un producto sin stock, los ítems anteriores ya estaban descontados y RabbitMQ reintentaba sin fin, **descontándolos otra vez en cada reintento** | Alta | Descuento de toda la orden en una transacción (todo o nada); error de negocio se registra en auditoría (`pedidos360.stock.error`) sin reintentar |
| 4 | ms-productos, ms-notificaciones | Cualquier excepción en un listener dejaba el mensaje en un ciclo infinito de reintentos (por ejemplo, SMTP caído) | Alta | `default-requeue-rejected: false` + 3 reintentos |
| 5 | ms-usuarios | Un Admin que editaba su perfil perdía el rol (se guardaba `null`) | Alta | El rol solo cambia si viene en el body |
| 6 | ms-pagos | Al reintentar un pago que quedó a medias se reutilizaba el token Webpay vencido | Media | Solo se reutiliza un token de menos de 2 min (doble clic); si no, se crea una transacción nueva |
| 7 | ms-pagos | Se podía pagar una orden en estado `PROCESADA` (ms-ordenes con `PAGO_REQUERIDO=false`) | Media | Solo se pagan órdenes `PENDIENTE_PAGO` o `PAGO_RECHAZADO`, con mensaje claro |
| 8 | ms-usuarios, ms-productos, ms-ordenes | Los rechazos de permisos (`@PreAuthorize`, "no tienes permiso") respondían **500** en vez de **403** | Media | Handler de `AccessDeniedException` → 403 |
| 9 | ms-carrito | Sin manejador de errores: "stock insuficiente", "producto no existe" o catálogo caído llegaban como 500 sin mensaje | Media | `GlobalExceptionHandler` con 404 / 409 / 503 y mensaje legible |
| 10 | ms-usuarios + frontend | El perfil dependía de un id en `localStorage`: desde otro navegador "desaparecía" y registrarlo de nuevo daba 500 | Media | Nuevo `GET /api/v1/usuarios/me/perfil` (por OID de Azure) y 409 si ya existe |
| 11 | ms-carrito | `vaciarCarrito` respondía 500 si el carrito no existía (se llama tras un pago aprobado) | Baja | Idempotente |
| 12 | ms-carrito | `httpclient5` declarado dos veces en el `pom.xml` (una fijada a 5.3.1) | Baja | Se deja solo la versión gestionada por Spring Boot |
| 13 | ms-auditoria | Cargaba toda la tabla en memoria para ordenarla | Baja | Consulta de los 500 eventos más recientes |
| 14 | ms-pagos | `response.getResponseCode()` es `int` en el SDK de Transbank y se comparaba con `null` (no compilaba) | — | Corregido por el equipo; se mantiene |
| 15 | Varios | `show-sql: true`, `org.springframework.security: DEBUG`, `System.out.println` | Baja | Logs SLF4J y niveles INFO |

Observación sin cambio: ms-carrito usa `@CircuitBreaker` (Resilience4j para Boot 3), pero el `pom.xml` no incluye el starter de AOP, así que el circuit breaker probablemente no se activa. El nuevo manejador de errores devuelve igualmente un 503 claro si ms-productos no responde.

## Pendientes recomendados (fuera del alcance de esta entrega)

- Exponer ms-pagos y ms-recomendaciones por **API Gateway (HTTPS)** como ms-ordenes, para evitar *mixed content* si el frontend se sirve por HTTPS.
- `vite.config.js` usa `base: './'`: al recargar una ruta profunda como `/producto/5` los assets se piden en `/producto/assets/...` y la página queda en blanco. Si el frontend se sirve en la raíz del dominio, cambiar a `base: '/'`. (No afecta el retorno de WebPay a `/pedidos`.)
- Kafka con 3 brokers y `replication-factor 3` en producción (en AWS Academy se usa 1 broker por costo).

## Cambios v5: eliminar pedidos pendientes y prueba del antifraude

| Dónde | Cambio |
|---|---|
| ms-ordenes | Nuevo `DELETE /api/v1/ordenes/{id}`. Solo el dueño (o un admin) puede eliminar, y solo pedidos `PENDIENTE_PAGO` o `PAGO_RECHAZADO` (otros estados → 409). Se registra el evento `pedidos360.orden.eliminada` en auditoría. Esos pedidos nunca descontaron stock ni se cobraron. |
| ms-ordenes | Si llega un pago `AUTHORIZED` para un pedido que ya no existe, se registra en log de error y en auditoría (`pedidos360.orden.pago-sin-orden`) para revisión manual. |
| ms-pagos | Cada solicitud a `/iniciar` cuenta como intento para el velocity check (antes, reutilizar el token de un pedido no contaba), así 3 solicitudes en 10 s bloquean al usuario 30 min aunque sean del mismo pedido. |
| Frontend (Mis pedidos) | Botón **Eliminar** en pedidos pendientes/rechazados y panel **Prueba del antifraude** que envía 5 solicitudes seguidas sin ir a WebPay y muestra el código de cada una (200 / 429). Se oculta con `VITE_DEMO_ANTIFRAUDE=false`. |

API Gateway: agregar la ruta `DELETE /api/v1/ordenes/{id}` (si no hay una `ANY /{proxy+}`) y permitir el método `DELETE` en la configuración CORS.
