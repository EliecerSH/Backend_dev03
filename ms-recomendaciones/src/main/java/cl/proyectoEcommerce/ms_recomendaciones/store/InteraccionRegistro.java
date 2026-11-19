package cl.proyectoEcommerce.ms_recomendaciones.store;

import java.time.Instant;

/** Interacción ya validada, lista para persistir. eventId es la clave de idempotencia. */
public record InteraccionRegistro(
        String eventId,
        String usuarioId,
        Long productoId,
        String categoria,
        String accion,
        int peso,
        String origen,
        Instant ocurridoEn) {}
