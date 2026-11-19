package cl.proyectoEcommerce.ms_recomendaciones.store;

import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Interaccion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cl.proyectoEcommerce.ms_recomendaciones.store.DynamoKeys.*;

/**
 * Interacciones en Amazon DynamoDB (tabla única, ver {@link DynamoKeys}).
 *
 * Cada interacción se escribe en UNA transacción de 4 ítems:
 *  1. EVT#{eventId}            -> condición attribute_not_exists: si ya existe, es un duplicado y no se escribe nada.
 *  2. USER#{u} / INT#{t}#{id}  -> historial del usuario.
 *  3. POP#{día} / PROD#{p}     -> ADD score (popularidad diaria, contador atómico).
 *  4. PROD#{p} / USER#{u}      -> ADD peso (índice inverso para "otros clientes también lo vieron").
 * Así los agregados nunca quedan inconsistentes con el historial aunque Kafka re-entregue el mensaje.
 */
@Component
public class DynamoInteraccionStore implements InteraccionStore {
    private static final Logger log = LoggerFactory.getLogger(DynamoInteraccionStore.class);
    private static final int MAX_USUARIOS_COOCURRENCIA = 50;
    private static final int MAX_INTERACCIONES_POR_USUARIO = 100;
    private static final Duration CACHE_POPULARIDAD = Duration.ofSeconds(60);

    private final DynamoDbClient dynamo;
    private final String tabla;
    private final int ttlDias;
    private volatile Map<Long, Double> popularidadCache = Map.of();
    private volatile Instant popularidadExpira = Instant.EPOCH;

    public DynamoInteraccionStore(DynamoDbClient dynamo,
                                  @Value("${recomendaciones.dynamodb.table}") String tabla,
                                  @Value("${recomendaciones.dynamodb.ttl-dias:60}") int ttlDias) {
        this.dynamo = dynamo;
        this.tabla = tabla;
        this.ttlDias = ttlDias;
    }

    @Override
    public boolean guardar(InteraccionRegistro r) {
        Instant t = r.ocurridoEn();
        String peso = String.valueOf(r.peso());
        String ttl = String.valueOf(DynamoKeys.ttl(t, ttlDias));

        Map<String, AttributeValue> marcador = Map.of(
                PK, s(evento(r.eventId())), SK, s("EVT"),
                TTL, n(String.valueOf(DynamoKeys.ttl(Instant.now(), 7))));

        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK, s(usuario(r.usuarioId())));
        item.put(SK, s(interaccion(t, r.eventId())));
        item.put("eventId", s(r.eventId()));
        item.put("usuarioId", s(r.usuarioId()));
        item.put("productoId", n(String.valueOf(r.productoId())));
        if (r.categoria() != null) item.put("categoria", s(r.categoria()));
        item.put("accion", s(r.accion()));
        item.put("peso", n(peso));
        if (r.origen() != null) item.put("origen", s(r.origen()));
        item.put("ocurridoEn", s(t.toString()));
        item.put(TTL, n(ttl));

        LocalDate dia = t.atZone(java.time.ZoneOffset.UTC).toLocalDate();

        TransactWriteItemsRequest tx = TransactWriteItemsRequest.builder().transactItems(
                TransactWriteItem.builder().put(Put.builder().tableName(tabla).item(marcador)
                        .conditionExpression("attribute_not_exists(#pk)")
                        .expressionAttributeNames(Map.of("#pk", PK)).build()).build(),
                TransactWriteItem.builder().put(Put.builder().tableName(tabla).item(item).build()).build(),
                TransactWriteItem.builder().update(Update.builder().tableName(tabla)
                        .key(Map.of(PK, s(popularidadDia(dia)), SK, s(producto(r.productoId()))))
                        .updateExpression("SET #ttl = :ttl ADD #score :p")
                        .expressionAttributeNames(Map.of("#ttl", TTL, "#score", "score"))
                        .expressionAttributeValues(Map.of(":p", n(peso), ":ttl", n(ttl)))
                        .build()).build(),
                TransactWriteItem.builder().update(Update.builder().tableName(tabla)
                        .key(Map.of(PK, s(producto(r.productoId())), SK, s(usuario(r.usuarioId()))))
                        .updateExpression("SET #ultimo = :u, #ttl = :ttl ADD #peso :p")
                        .expressionAttributeNames(Map.of("#ultimo", "ultimo", "#ttl", TTL, "#peso", "peso"))
                        .expressionAttributeValues(Map.of(":p", n(peso), ":ttl", n(ttl),
                                ":u", n(String.valueOf(t.toEpochMilli()))))
                        .build()).build()
        ).build();

        try {
            dynamo.transactWriteItems(tx);
            return true;
        } catch (TransactionCanceledException e) {
            boolean duplicado = e.cancellationReasons().stream()
                    .map(CancellationReason::code)
                    .anyMatch("ConditionalCheckFailed"::equals);
            if (duplicado) {
                log.debug("Evento duplicado ignorado: {}", r.eventId());
                return false;
            }
            throw e; // otro motivo (throttling, conflicto): el error handler de Kafka reintenta y luego envía a DLT
        }
    }

    @Override
    public List<Interaccion> historialUsuario(String usuarioId, Instant desde, int limite) {
        QueryRequest q = QueryRequest.builder().tableName(tabla)
                .keyConditionExpression("#pk = :pk AND #sk > :desde")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
                .expressionAttributeValues(Map.of(":pk", s(usuario(usuarioId)), ":desde", s(instante(desde))))
                .scanIndexForward(false) // más recientes primero
                .limit(limite)
                .build();
        return dynamo.query(q).items().stream().map(DynamoInteraccionStore::toInteraccion).toList();
    }

    @Override
    public Map<Long, Double> popularidad(Instant desde) {
        Instant ahora = Instant.now();
        if (ahora.isBefore(popularidadExpira)) return popularidadCache;

        Map<Long, Double> total = new HashMap<>();
        for (LocalDate dia : diasEntre(desde, ahora)) {
            QueryRequest q = QueryRequest.builder().tableName(tabla)
                    .keyConditionExpression("#pk = :pk")
                    .expressionAttributeNames(Map.of("#pk", PK))
                    .expressionAttributeValues(Map.of(":pk", s(popularidadDia(dia))))
                    .build();
            for (Map<String, AttributeValue> item : dynamo.queryPaginator(q).items()) {
                total.merge(productoDesde(item.get(SK).s()), num(item.get("score")), Double::sum);
            }
        }
        popularidadCache = Map.copyOf(total);
        popularidadExpira = ahora.plus(CACHE_POPULARIDAD);
        return popularidadCache;
    }

    @Override
    public Map<Long, Double> coocurrencia(Long productoId, Instant desde) {
        QueryRequest usuariosQ = QueryRequest.builder().tableName(tabla)
                .keyConditionExpression("#pk = :pk AND begins_with(#sk, :u)")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
                .expressionAttributeValues(Map.of(":pk", s(producto(productoId)), ":u", s("USER#")))
                .build();

        long desdeMs = desde.toEpochMilli();
        List<Map<String, AttributeValue>> recientes = new ArrayList<>();
        for (Map<String, AttributeValue> item : dynamo.queryPaginator(usuariosQ).items()) {
            if (num(item.get("ultimo")) >= desdeMs) recientes.add(item);
        }
        recientes.sort(Comparator.comparingDouble((Map<String, AttributeValue> i) -> num(i.get("ultimo"))).reversed());

        Map<Long, Double> co = new HashMap<>();
        for (Map<String, AttributeValue> item : recientes.subList(0, Math.min(MAX_USUARIOS_COOCURRENCIA, recientes.size()))) {
            String usuarioId = usuarioDesde(item.get(SK).s());
            for (Interaccion i : historialUsuario(usuarioId, desde, MAX_INTERACCIONES_POR_USUARIO)) {
                if (i.productoId() != null && !i.productoId().equals(productoId)) {
                    co.merge(i.productoId(), (double) i.peso(), Double::sum);
                }
            }
        }
        return co;
    }

    private static Interaccion toInteraccion(Map<String, AttributeValue> item) {
        AttributeValue cat = item.get("categoria");
        return new Interaccion(
                (long) num(item.get("productoId")),
                cat == null ? null : cat.s(),
                item.get("accion").s(),
                (int) num(item.get("peso")),
                Instant.parse(item.get("ocurridoEn").s()));
    }

    private static AttributeValue s(String v) {
        return AttributeValue.builder().s(v).build();
    }

    private static AttributeValue n(String v) {
        return AttributeValue.builder().n(v).build();
    }

    private static double num(AttributeValue v) {
        return v == null || v.n() == null ? 0.0 : Double.parseDouble(v.n());
    }
}
