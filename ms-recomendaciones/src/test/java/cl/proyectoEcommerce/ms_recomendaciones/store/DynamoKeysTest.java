package cl.proyectoEcommerce.ms_recomendaciones.store;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DynamoKeysTest {

    @Test
    void sortKeyOrdenaCronologicamente() {
        String antes = DynamoKeys.interaccion(Instant.parse("2026-01-09T23:59:59Z"), "zzz");
        String despues = DynamoKeys.interaccion(Instant.parse("2026-01-10T00:00:00Z"), "aaa");
        assertTrue(antes.compareTo(despues) < 0, "orden lexicográfico == orden temporal");
        // Consulta "desde": todo lo posterior al límite queda dentro del rango sk > desde
        String desde = DynamoKeys.instante(Instant.parse("2026-01-10T00:00:00Z"));
        assertTrue(despues.compareTo(desde) > 0);
        assertTrue(antes.compareTo(desde) < 0);
    }

    @Test
    void epochConCerosALaIzquierda() {
        assertEquals("INT#0000000001000", DynamoKeys.instante(Instant.ofEpochMilli(1000)));
        assertEquals(17, DynamoKeys.instante(Instant.parse("2026-11-18T12:00:00Z")).length());
    }

    @Test
    void clavesIdaYVuelta() {
        assertEquals(42L, DynamoKeys.productoDesde(DynamoKeys.producto(42L)));
        assertEquals("oid-123", DynamoKeys.usuarioDesde(DynamoKeys.usuario("oid-123")));
        assertEquals("POP#2026-11-18", DynamoKeys.popularidadDia(LocalDate.of(2026, 11, 18)));
        assertEquals("EVT#abc", DynamoKeys.evento("abc"));
    }

    @Test
    void diasDeLaVentanaIncluyenAmbosExtremos() {
        List<LocalDate> dias = DynamoKeys.diasEntre(Instant.parse("2026-11-16T23:00:00Z"),
                Instant.parse("2026-11-18T01:00:00Z"));
        assertEquals(List.of(LocalDate.of(2026, 11, 16), LocalDate.of(2026, 11, 17), LocalDate.of(2026, 11, 18)), dias);
    }

    @Test
    void ttlEnSegundosEpoch() {
        Instant base = Instant.parse("2026-11-18T00:00:00Z");
        assertEquals(base.getEpochSecond() + 60L * 86_400L, DynamoKeys.ttl(base, 60));
    }
}
