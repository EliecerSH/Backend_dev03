# ms-recomendaciones (puerto 8088)

Servicio de **recomendaciones simple y explicable** alimentado por eventos Kafka.
Java 17 · Spring Boot 4.1 · **Amazon DynamoDB** (AWS SDK for Java v2) · Kafka · Azure AD.

## Señales (Kafka)

| Tópico | Origen | Se guarda como |
|---|---|---|
| `recomendaciones.interacciones` | Frontend → `POST /interacciones` (vistas de producto, agregar al carrito) | `VISTA` (peso 1) / `CARRITO` (peso 3) |
| `pagos.resultados` (group `ms-recomendaciones`) | ms-pagos, solo `AUTHORIZED` | una `COMPRA` (peso 5) por producto pagado |

Cada interacción tiene un `eventId` único: los eventos re-entregados no se duplican.

## Almacenamiento: Amazon DynamoDB (tabla única)

Tabla `pedidos360-recomendaciones` (`DYNAMODB_TABLE`), clave `pk` (partición) + `sk` (orden), facturación on-demand y TTL en `expiraEn`. La crea el propio servicio al arrancar si no existe (`DYNAMODB_CREATE_TABLE=true`).

| Patrón de acceso | `pk` | `sk` | Operación |
|---|---|---|---|
| Historial del usuario | `USER#{usuarioId}` | `INT#{epochMillis 13 dígitos}#{eventId}` | `Query sk > INT#{desde}`, más recientes primero |
| Popularidad diaria | `POP#{yyyy-MM-dd}` | `PROD#{productoId}` | `ADD score` (contador atómico); 1 `Query` por día de la ventana, cache 60 s |
| Quién vio un producto | `PROD#{productoId}` | `USER#{usuarioId}` | `ADD peso`, `SET ultimo`; base de "similares" |
| Deduplicación | `EVT#{eventId}` | `EVT` | `attribute_not_exists` dentro de la transacción |

Escritura: `TransactWriteItems` con los 4 ítems. Si el marcador `EVT#` ya existe, la transacción completa se cancela (`ConditionalCheckFailed`) y se trata como duplicado.

Credenciales: cadena por defecto del SDK → rol de la EC2 (`LabInstanceProfile` en AWS Academy). Sin llaves en el código. Para correr sin AWS: `docker compose -f docker-compose.yml -f docker-compose.local.yml up` (DynamoDB Local).

| Variable | Defecto |
|---|---|
| `AWS_REGION` | `us-east-1` |
| `DYNAMODB_TABLE` | `pedidos360-recomendaciones` |
| `DYNAMODB_ENDPOINT` | vacío (AWS real); `http://dynamodb-local:8000` en local |
| `DYNAMODB_CREATE_TABLE` | `true` |
| `DYNAMODB_TTL_DIAS` | `60` |

## Algoritmo (`engine/MotorRecomendaciones.java`, sin dependencias de Spring)

- **Personalizada:** `0.55 · afinidad de categoría + 0.25 · interés en el producto + 0.20 · popularidad`.
  Cada interacción pierde la mitad de su peso cada 7 días. No recomienda lo ya comprado ni lo sin stock.
- **Populares:** suma de pesos de todos los usuarios en los últimos 30 días.
- **Similares:** co-ocurrencia ("otros clientes también lo vieron") + productos de la misma categoría.
- El catálogo (nombre, precio, stock, imagen) se lee del endpoint público de ms-productos y se cachea 5 min; si ms-productos cae, se usa la última copia.

## API (`/api/v1/recomendaciones`)

| Método | Ruta | Auth |
|---|---|---|
| GET | `/me?limit=8` | User/Admin |
| GET | `/populares?limit=8` | pública |
| GET | `/productos/{id}/similares?limit=4` | pública |
| POST | `/interacciones` `{ "productoId": 5, "accion": "VISTA" }` | User/Admin (202) |
| POST | `/admin/simular?cantidad=200&usuarios=10` | Admin |

Respuesta: `{ "estrategia": "PERSONALIZADA|POPULARES|SIMILARES", "items": [{ productoId, nombre, categoria, precio, stock, imagenUrl, score, motivo }] }`

`accion` acepta `VISTA|CARRITO|COMPRA` y los alias del ejemplo CloudNative `view|cart|purchase`.

## Tests
`mvn test`
- `MotorRecomendacionesTest` (7 casos): usuario nuevo, afinidad, exclusión de comprados/sin stock, decaimiento temporal, similares, alias de acciones.
- `DynamoKeysTest` (5 casos): orden cronológico del sort key, formato de claves, días de la ventana, TTL.
