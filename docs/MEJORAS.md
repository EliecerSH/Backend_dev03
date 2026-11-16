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
| ms-recomendaciones simple | Motor explicable (afinidad por categoría con decaimiento, popularidad, co-ocurrencia) alimentado por Kafka. Bloques "Recomendado para ti" / "Lo más popular" en el home y "También te puede interesar" en la ficha de producto |
| ¿Colas → Kafka? | Explicado en [RABBITMQ_VS_KAFKA.md](RABBITMQ_VS_KAFKA.md): se mantienen en RabbitMQ; lo nuevo va en Kafka; ms-ordenes hace de puente |

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
| Todas las tablas en el schema `public` compartido | **Base de datos por servicio** vía schema propio (`pagos`, `recomendaciones`) |
| `System.out.println` en ms-ordenes | SLF4J con niveles |
| `show-sql: true` y `org.springframework.security: DEBUG` en ms-ordenes | `show-sql: false`, nivel INFO |
| Sin timeouts en llamadas REST entre servicios | Connect 3 s / read 5 s → 503 claro en vez de cuelgues |
| Sin healthchecks | Actuator liveness/readiness + `healthcheck` en compose; Kafka con healthcheck y `kafka-init` que crea tópicos antes de levantar los servicios |
| Contenedores como root | Usuario `spring` sin privilegios en las imágenes |
| Tópicos creados por accidente | `auto.create.topics.enable=false`, creación explícita e idempotente |
| Ejemplo CloudNative: `.env` con IP versionado, `@CrossOrigin("*")`, endpoints sin autenticación | Secrets/`.env` fuera de git, CORS restringido, Azure AD (mismo tenant), endpoints de escritura autenticados y simulación solo para Admin |
| Sin tests en CI | Tests unitarios del motor de recomendaciones y del antifraude; el workflow los ejecuta antes de construir las imágenes |
| Frontend: errores de ms-pagos (`message`) no se mostraban | `parseError` lee `mensaje`, `message` y `error` |

## Pendientes recomendados (fuera del alcance de esta entrega)

- Exponer ms-pagos y ms-recomendaciones por **API Gateway (HTTPS)** como ms-ordenes, para evitar *mixed content* si el frontend se sirve por HTTPS.
- `vite.config.js` usa `base: './'`: al recargar una ruta profunda como `/producto/5` los assets se piden en `/producto/assets/...` y la página queda en blanco. Si el frontend se sirve en la raíz del dominio, cambiar a `base: '/'`. (No afecta el retorno de WebPay a `/pedidos`.)
- Migrar `pedidos360.auditoria.queue` a un consumidor Kafka cuando todos los eventos del sistema estén en Kafka.
- Kafka con 3 brokers y `replication-factor 3` en producción (en AWS Academy se usa 1 broker por costo).
