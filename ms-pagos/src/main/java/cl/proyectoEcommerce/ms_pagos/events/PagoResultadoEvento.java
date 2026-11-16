package cl.proyectoEcommerce.ms_pagos.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Evento publicado en el tópico Kafka "pagos.resultados" (key = orderId).
 * status: AUTHORIZED | REJECTED | CANCELLED | ERROR
 * Consumidores: ms-ordenes (actualiza el estado de la orden) y ms-recomendaciones (registra la compra).
 */
public record PagoResultadoEvento(
        String eventId,
        Long orderId,
        String clientId,
        String status,
        BigDecimal amount,
        String authorizationCode,
        List<Item> items,
        Instant occurredAt) {

    public record Item(Long productoId, Integer cantidad, BigDecimal precioUnitario) {}
}
