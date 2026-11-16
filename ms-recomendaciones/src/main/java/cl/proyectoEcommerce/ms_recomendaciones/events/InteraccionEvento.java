package cl.proyectoEcommerce.ms_recomendaciones.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * Evento del tópico Kafka "recomendaciones.interacciones" (key = usuarioId).
 * accion: VISTA | CARRITO | COMPRA   origen: WEB | SIMULACION
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record InteraccionEvento(
        String eventId,
        String usuarioId,
        Long productoId,
        String categoria,
        String accion,
        int peso,
        String origen,
        Instant ocurridoEn) {}
