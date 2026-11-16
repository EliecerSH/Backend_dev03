# Backend_dev03 — Pagos (WebPay + antifraude), Recomendaciones y Kafka

Tercer stack de backend de **Pedidos360**. Corre en su propia EC2 y contiene:

| Componente | Puerto | Descripción |
|---|---|---|
| **Kafka** (KRaft, sin Zookeeper) | 9092 | Broker de eventos. 4 tópicos de dominio + sus `.DLT` |
| **Kafka UI** | 8089 | Ver tópicos, mensajes y consumer groups (demo/evaluación) |
| **ms-pagos** | 8087 | Botón de pago **WebPay Plus** (Transbank) con **detección de fraude** (velocity check sobre Kafka) |
| **ms-recomendaciones** | 8088 | Servicio de **recomendaciones simple** alimentado por eventos Kafka |

> ¿Por qué las colas antiguas siguen en RabbitMQ y lo nuevo va en Kafka? Ver **[docs/RABBITMQ_VS_KAFKA.md](docs/RABBITMQ_VS_KAFKA.md)**.
> Mejoras y observaciones atendidas: **[docs/MEJORAS.md](docs/MEJORAS.md)**.

---

## 1. Arquitectura

```mermaid
flowchart TB
    subgraph EC2_FE[EC2 Frontend_dev]
        FE[React + MSAL :80]
    end
    subgraph EC2_B1[EC2 Backend_dev]
        USU[ms-usuarios :8081]
        PRO[ms-productos :8082]
        CAR[ms-carrito :8083]
    end
    subgraph EC2_B2[EC2 Backend_dev02]
        ORD[ms-ordenes :8084]
        NOT[ms-notificaciones :8085]
        AUD[ms-auditoria :8086]
        RMQ[(RabbitMQ :5672)]
    end
    subgraph EC2_B3[EC2 Backend_dev03]
        KAF[(Kafka :9092)]
        PAG[ms-pagos :8087]
        REC[ms-recomendaciones :8088]
    end
    subgraph EC2_D[EC2 Data_dev]
        PG[(PostgreSQL :5432)]
    end
    WP[WebPay Transbank]

    FE -->|crear orden| ORD
    FE -->|POST /api/pagos/iniciar| PAG
    PAG -->|GET orden con token del usuario| ORD
    PAG <-->|create / commit| WP
    WP -->|redirige navegador a /api/pagos/retorno| PAG
    PAG -->|pagos.intentos| KAF
    KAF -->|velocity check| PAG
    PAG -->|pagos.resultados / pagos.alertas-fraude| KAF
    KAF -->|pagos.resultados| ORD
    KAF -->|pagos.resultados| REC
    FE -->|vistas, carrito / GET recomendaciones| REC
    REC <-->|recomendaciones.interacciones| KAF
    REC -->|catálogo público| PRO
    ORD -->|orden.creada si AUTHORIZED| RMQ
    PAG -.->|pedidos360.pago.* auditoría| RMQ
    PAG --> PG
    REC --> PG
```

### Flujo de compra completo
1. El usuario presiona **"Pagar con WebPay"** en el carrito → el frontend crea la orden en ms-ordenes (queda `PENDIENTE_PAGO`).
2. El frontend llama `POST /api/pagos/iniciar {orderId}`. ms-pagos consulta la orden en ms-ordenes **con el token del usuario** (el monto y el dueño los decide el backend, nunca el navegador).
3. **Gate antifraude:** si el cliente está bloqueado → `429`. Si no, publica el intento en `pagos.intentos` (key = clientId) y crea la transacción en WebPay.
4. El navegador se envía por `POST` (campo `token_ws`) al formulario de WebPay.
5. WebPay redirige a `/api/pagos/retorno`; ms-pagos hace `commit`, guarda el resultado y publica `pagos.resultados`.
6. **ms-ordenes** consume el resultado: `AUTHORIZED` → `PAGADA` y **recién ahí** publica `orden.creada` en RabbitMQ (stock + correo + auditoría). Rechazo/anulación → `PAGO_RECHAZADO` (se puede reintentar).
7. **ms-recomendaciones** consume el mismo evento y registra cada producto como `COMPRA`.
8. El usuario vuelve a `FRONTEND_URL/pedidos?paymentStatus=...` y ve el resultado; el carrito se vacía solo si fue `AUTHORIZED`.

### Detección de fraude (velocity check, visto en clases)
- Cada intento se publica en Kafka; el consumidor (`IntentoPagoConsumer`) lo registra y cuenta los intentos del cliente en los últimos **10 s** bajo un bloqueo de fila en PostgreSQL (seguro ante concurrencia y ante varias réplicas).
- **3 intentos en 10 s → bloqueo de 30 min** (configurable con `FRAUD_*`). Se publica `BLOQUEO_APLICADO` en `pagos.alertas-fraude`.
- Mientras dure el bloqueo, `/iniciar` responde **429** y publica `INTENTO_BLOQUEADO`.
- Idempotente: un intento re-entregado por Kafka (mismo `transactionId`) no se cuenta dos veces.
- El conteo es asíncrono (milisegundos): el intento que alcanza el umbral activa el bloqueo y **los siguientes** reciben 429.

### Recomendaciones (simple y explicable)
Pesos: `VISTA = 1`, `CARRITO = 3`, `COMPRA = 5`, con **decaimiento temporal** (vida media de 7 días).

| Endpoint | Estrategia |
|---|---|
| `GET /api/v1/recomendaciones/me` (auth) | 55 % afinidad por categoría + 25 % interés directo en el producto + 20 % popularidad. Excluye lo ya comprado y lo sin stock. Sin historial → populares. |
| `GET /api/v1/recomendaciones/populares` (público) | Suma de pesos de todos los usuarios (últimos 30 días) |
| `GET /api/v1/recomendaciones/productos/{id}/similares` (público) | Co-ocurrencia "otros clientes también lo vieron" + misma categoría |
| `POST /api/v1/recomendaciones/interacciones` (auth) | `{productoId, accion}` → Kafka (202 Accepted) |
| `POST /api/v1/recomendaciones/admin/simular?cantidad=200&usuarios=10` (Admin) | Genera tráfico aleatorio sobre el catálogo real (prueba de carga) |

Cada recomendación incluye un **motivo** legible ("Porque te interesa Notebooks", "Lo agregaste a tu carrito", …).

---

## 2. Tópicos Kafka

| Tópico | Key | Productor | Consumer groups |
|---|---|---|---|
| `pagos.intentos` | clientId | ms-pagos | `ms-pagos-antifraude` |
| `pagos.resultados` | orderId | ms-pagos | `ms-ordenes`, `ms-recomendaciones` |
| `pagos.alertas-fraude` | clientId | ms-pagos | (monitoreo en Kafka UI) |
| `recomendaciones.interacciones` | usuarioId | ms-recomendaciones | `ms-recomendaciones` |
| `*.DLT` | — | DefaultErrorHandler | mensajes que fallaron 3 veces |

Los crea el contenedor `kafka-init` (idempotente) con 3 particiones. `auto.create.topics` está desactivado para que ningún typo cree tópicos fantasma.

---

## 3. Despliegue en AWS Academy (EC2)

### 3.1 Instancia y Security Group
- **t3.medium** o superior (Kafka + 2 JVM ≈ 2 GB). Amazon Linux 2023.
- Reglas de entrada:

| Puerto | Origen | Para |
|---|---|---|
| 22 | Tu IP | SSH |
| 8087 | 0.0.0.0/0 | ms-pagos (navegador + retorno WebPay) |
| 8088 | 0.0.0.0/0 | ms-recomendaciones |
| 9092 | SG / IP privada de Backend_dev02 | Kafka para ms-ordenes |
| 8089 | Tu IP | Kafka UI (no dejar abierto a internet) |

En la EC2 de **Data_dev** permitir 5432 desde esta EC2; en **Backend_dev02** permitir 5672 (RabbitMQ) y 8084 desde esta EC2.

### 3.2 Preparar la EC2
```bash
sudo dnf install -y docker git
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user && newgrp docker
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
docker network create ecommerce-network || true
```

### 3.3 Levantar (manual)
```bash
git clone <repo> && cd Backend_dev03
cp .env.example .env
# KAFKA_ADVERTISED_HOST = IP PRIVADA de esta EC2 (curl -s http://169.254.169.254/latest/meta-data/local-ipv4)
nano .env
docker compose up -d --build
docker compose ps          # kafka healthy, kafka-init exited (0), ms-* healthy
docker compose logs -f ms-pagos
```

### 3.4 Despliegue automático (GitHub Actions, rama `deploy`)
`.github/workflows/deploy.yml` corre los tests, construye las imágenes, las sube a ECR y despliega por **SSM** (igual que Backend_dev y Backend_dev02). Secrets requeridos:

`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `EC2_INSTANCE_ID`, `KAFKA_ADVERTISED_HOST`, `DATASOURCE_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `CORS_ALLOWED_ORIGINS`, `ORDENES_SERVICE_URL`, `PRODUCTOS_SERVICE_URL`, `FRONTEND_URL`, `TRANSBANK_RETURN_URL`, `RABBITMQ_HOST`, `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD` (opcionales: `TRANSBANK_COMMERCE_CODE`, `TRANSBANK_API_KEY`, `TRANSBANK_ENVIRONMENT`, `RABBITMQ_PORT`).

> En AWS Academy las credenciales expiran con cada sesión del lab: actualizar los 3 secrets `AWS_*` antes de cada push a `deploy`.

### 3.5 Cambios en los otros stacks (ya incluidos en este entregable)
- **Backend_dev02 / ms-ordenes:** agregar en `.env` / secrets `KAFKA_BOOTSTRAP_SERVERS=<IP privada Backend_dev03>:9092` y `PAGO_REQUERIDO=true`.
- **Frontend_dev:** agregar `VITE_MS_PAGOS_URL` y `VITE_MS_RECOMENDACIONES_URL` (secrets del workflow). Si quedan vacías, el frontend funciona como antes.
- **HTTPS:** si el frontend se sirve por HTTPS (API Gateway), el navegador bloquea llamadas a `http://IP:8087` (*mixed content*). Exponer ms-pagos y ms-recomendaciones por API Gateway (integración HTTP proxy, igual que ms-ordenes) y usar esas URLs. `TRANSBANK_RETURN_URL` debe ser la URL pública de `/api/pagos/retorno` que el **navegador** del cliente pueda abrir.

---

## 4. Guía de prueba para la evaluación

```bash
# 1) Tópicos creados
docker exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list

# 2) Ver eventos en vivo (otra terminal), o abrir Kafka UI en http://<IP>:8089
docker exec -it ecommerce-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic pagos.resultados --from-beginning

# 3) Salud
curl http://<IP>:8087/actuator/health
curl http://<IP>:8088/actuator/health

# 4) Recomendaciones: generar tráfico (token de Admin) y consultar populares
curl -X POST -H "Authorization: Bearer $TOKEN_ADMIN" "http://<IP>:8088/api/v1/recomendaciones/admin/simular?cantidad=200"
curl "http://<IP>:8088/api/v1/recomendaciones/populares?limit=5"

# 5) Consumer groups (ms-ordenes y ms-recomendaciones leen el mismo tópico)
docker exec ecommerce-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --describe --all-groups
```

**Pago en WebPay (ambiente de integración):**

| Dato | Valor de prueba |
|---|---|
| Tarjeta VISA (aprobada) | `4051 8856 0044 6623`, CVV `123`, cualquier fecha futura |
| Tarjeta Mastercard (rechazada) | `5186 0595 5959 0568`, CVV `123` |
| RUT / clave en el simulador del banco | `11.111.111-1` / `123` |

**Demostrar el antifraude:** crear 3 pedidos y presionar "Pagar con WebPay" en cada uno en menos de 10 s (o cancelar en WebPay y reintentar rápido). El siguiente intento muestra *"Detectamos demasiados intentos de pago seguidos…"* (HTTP 429), aparece `BLOQUEO_APLICADO` en `pagos.alertas-fraude` y `pedidos360.pago.bloqueado` en la pantalla de Auditoría.

---

## 5. Variables de entorno
Ver [`.env.example`](.env.example). Las principales:

| Variable | Uso |
|---|---|
| `KAFKA_ADVERTISED_HOST` | IP privada de esta EC2 (Kafka la anuncia a ms-ordenes) |
| `DATASOURCE_URL` | PostgreSQL de Data_dev. Cada servicio usa su **propio schema** (`pagos`, `recomendaciones`) |
| `ORDENES_SERVICE_URL` / `PRODUCTOS_SERVICE_URL` | URLs base incluyendo `/api/v1/ordenes` y `/api/v1/productos` |
| `TRANSBANK_*` | Credenciales WebPay (por defecto, integración) |
| `TRANSBANK_RETURN_URL` / `FRONTEND_URL` | Retorno de WebPay y redirección final al frontend |
| `RABBITMQ_*` | Solo para enviar eventos de pago a la auditoría existente |
| `FRAUD_MAX_ATTEMPTS` / `FRAUD_WINDOW_SECONDS` / `FRAUD_BLOCK_MINUTES` | Umbral del velocity check (3 / 10 / 30) |

## 6. Estructura
```
Backend_dev03/
├── docker-compose.yml          # kafka, kafka-init, kafka-ui, ms-pagos, ms-recomendaciones
├── .env.example
├── .github/workflows/deploy.yml
├── docs/
│   ├── RABBITMQ_VS_KAFKA.md    # decisión colas vs Kafka
│   └── MEJORAS.md              # observaciones y mejoras aplicadas
├── ms-pagos/                   # Webpay + antifraude (Kafka)
└── ms-recomendaciones/         # motor de recomendaciones (Kafka)
```
