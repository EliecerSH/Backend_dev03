package cl.proyectoEcommerce.ms_recomendaciones.engine;

import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Accion;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Interaccion;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Producto;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Recomendacion;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MotorRecomendacionesTest {

    private final Instant ahora = Instant.parse("2026-11-14T12:00:00Z");

    private final List<Producto> catalogo = List.of(
            new Producto(1L, "Notebook Gamer", "Notebooks", new BigDecimal("899990"), 5, null),
            new Producto(2L, "Notebook Ultra", "Notebooks", new BigDecimal("699990"), 3, null),
            new Producto(3L, "Mouse Inalámbrico", "Accesorios", new BigDecimal("19990"), 50, null),
            new Producto(4L, "Audífonos BT", "Audio", new BigDecimal("49990"), 10, null),
            new Producto(5L, "Notebook Básico", "Notebooks", new BigDecimal("399990"), 0, null)); // sin stock

    private Interaccion inter(long productoId, String categoria, Accion accion, int diasAtras) {
        return new Interaccion(productoId, categoria, accion.name(), accion.peso(),
                ahora.minus(diasAtras, ChronoUnit.DAYS));
    }

    @Test
    void usuarioSinHistorialRecibePopulares() {
        List<Recomendacion> r = MotorRecomendaciones.personalizadas(List.of(), Map.of(4L, 10.0, 3L, 2.0),
                catalogo, ahora, 3);
        assertEquals(4L, r.get(0).producto().id());
        assertEquals("Popular en la tienda", r.get(0).motivo());
        assertEquals(3, r.size());
    }

    @Test
    void afinidadPorCategoriaPriorizaNotebooks() {
        List<Interaccion> historial = List.of(
                inter(1L, "Notebooks", Accion.VISTA, 0),
                inter(1L, "Notebooks", Accion.CARRITO, 0));
        List<Recomendacion> r = MotorRecomendaciones.personalizadas(historial, Map.of(4L, 50.0), catalogo, ahora, 4);

        assertEquals(1L, r.get(0).producto().id(), "el producto que dejó en el carro va primero");
        assertEquals("Lo agregaste a tu carrito", r.get(0).motivo());
        assertEquals(2L, r.get(1).producto().id(), "luego otro notebook por afinidad de categoría");
        assertTrue(r.get(1).motivo().contains("Notebooks"));
    }

    @Test
    void noRecomiendaProductosCompradosNiSinStock() {
        List<Interaccion> historial = List.of(inter(1L, "Notebooks", Accion.COMPRA, 0));
        List<Recomendacion> r = MotorRecomendaciones.personalizadas(historial, Map.of(), catalogo, ahora, 10);
        assertTrue(r.stream().noneMatch(x -> x.producto().id() == 1L), "ya comprado");
        assertTrue(r.stream().noneMatch(x -> x.producto().id() == 5L), "sin stock");
    }

    @Test
    void interaccionesAntiguasPesanMenos() {
        assertEquals(1.0, MotorRecomendaciones.decaimiento(ahora, ahora), 1e-9);
        assertEquals(0.5, MotorRecomendaciones.decaimiento(ahora.minus(7, ChronoUnit.DAYS), ahora), 1e-3);

        // Audio visto hoy vs Notebooks vistos hace 60 días: gana Audio
        List<Interaccion> historial = List.of(
                inter(4L, "Audio", Accion.VISTA, 0),
                inter(2L, "Notebooks", Accion.CARRITO, 60));
        List<Recomendacion> r = MotorRecomendaciones.personalizadas(historial, Map.of(), catalogo, ahora, 2);
        assertEquals(4L, r.get(0).producto().id());
    }

    @Test
    void similaresUsaCoocurrenciaYLuegoCategoria() {
        List<Recomendacion> r = MotorRecomendaciones.similares(1L, Map.of(3L, 8.0), catalogo, 5);
        assertEquals(3L, r.get(0).producto().id(), "co-ocurrencia primero");
        assertEquals(2L, r.get(1).producto().id(), "misma categoría después");
        assertTrue(r.stream().noneMatch(x -> x.producto().id() == 1L), "no se recomienda a sí mismo");
        assertTrue(r.stream().noneMatch(x -> x.producto().id() == 4L), "sin relación");
    }

    @Test
    void accionAceptaAliasDelEjemploCloudNative() {
        assertEquals(Accion.VISTA, Accion.desde("view"));
        assertEquals(Accion.CARRITO, Accion.desde("cart"));
        assertEquals(Accion.COMPRA, Accion.desde("purchase"));
        assertThrows(IllegalArgumentException.class, () -> Accion.desde("like"));
    }

    @Test
    void popularesExcluyeIds() {
        List<Recomendacion> r = MotorRecomendaciones.populares(Map.of(4L, 5.0), catalogo, Set.of(4L), 10);
        assertTrue(r.stream().noneMatch(x -> x.producto().id() == 4L));
    }
}
