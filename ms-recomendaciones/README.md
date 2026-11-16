# ms-recomendaciones (puerto 8088)

Servicio de **recomendaciones simple y explicable** alimentado por eventos Kafka.
Java 17 · Spring Boot 4.1 · PostgreSQL (schema `recomendaciones`) · Kafka · Azure AD.

## Señales (Kafka)

| Tópico | Origen | Se guarda como |
|---|---|---|
| `recomendaciones.interacciones` | Frontend → `POST /interacciones` (vistas de producto, agregar al carrito) | `VISTA` (peso 1) / `CARRITO` (peso 3) |
| `pagos.resultados` (group `ms-recomendaciones`) | ms-pagos, solo `AUTHORIZED` | una `COMPRA` (peso 5) por producto pagado |

Cada interacción tiene un `event_id` único: los eventos re-entregados no se duplican.

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
`mvn test` — `MotorRecomendacionesTest` (7 casos: usuario nuevo, afinidad, exclusión de comprados/sin stock, decaimiento temporal, similares, alias de acciones).
