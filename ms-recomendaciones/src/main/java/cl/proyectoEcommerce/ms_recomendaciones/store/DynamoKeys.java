package cl.proyectoEcommerce.ms_recomendaciones.store;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Diseño de tabla única (single-table design) en DynamoDB.
 *
 * <pre>
 * Patrón de acceso                         pk                        sk
 * ---------------------------------------  ------------------------  ---------------------------------
 * Historial de un usuario (por fecha)      USER#{usuarioId}          INT#{epochMillis 13 dígitos}#{eventId}
 * Popularidad por día (contador ADD)       POP#{yyyy-MM-dd}          PROD#{productoId}
 * Usuarios que tocaron un producto         PROD#{productoId}         USER#{usuarioId}
 * Deduplicación de eventos Kafka           EVT#{eventId}             EVT
 * </pre>
 *
 * El epoch en milisegundos con ceros a la izquierda hace que el orden lexicográfico del sort key
 * sea el orden cronológico, por lo que "desde X" es simplemente {@code sk > INT#X}.
 * Todos los ítems llevan el atributo TTL {@code expiraEn} para que DynamoDB borre lo antiguo sin costo.
 */
public final class DynamoKeys {
    public static final String PK = "pk";
    public static final String SK = "sk";
    public static final String TTL = "expiraEn";

    private DynamoKeys() {}

    public static String usuario(String usuarioId) {
        return "USER#" + usuarioId;
    }

    public static String interaccion(Instant ocurridoEn, String eventId) {
        return instante(ocurridoEn) + "#" + eventId;
    }

    /** Límite inferior para consultar el historial desde un instante. */
    public static String instante(Instant t) {
        return String.format("INT#%013d", Math.max(0, t.toEpochMilli()));
    }

    public static String popularidadDia(LocalDate dia) {
        return "POP#" + dia;
    }

    public static String producto(Long productoId) {
        return "PROD#" + productoId;
    }

    public static Long productoDesde(String clave) {
        return Long.valueOf(clave.substring("PROD#".length()));
    }

    public static String usuarioDesde(String clave) {
        return clave.substring("USER#".length());
    }

    public static String evento(String eventId) {
        return "EVT#" + eventId;
    }

    /** Días UTC entre {@code desde} y {@code hasta}, ambos incluidos (para las particiones de popularidad). */
    public static List<LocalDate> diasEntre(Instant desde, Instant hasta) {
        LocalDate inicio = desde.atZone(ZoneOffset.UTC).toLocalDate();
        LocalDate fin = hasta.atZone(ZoneOffset.UTC).toLocalDate();
        List<LocalDate> dias = new ArrayList<>();
        for (LocalDate d = inicio; !d.isAfter(fin); d = d.plusDays(1)) dias.add(d);
        return dias;
    }

    public static long ttl(Instant base, int dias) {
        return base.plusSeconds(dias * 86_400L).getEpochSecond();
    }
}
