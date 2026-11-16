package cl.proyectoEcommerce.ms_pagos.events;

import java.time.Instant;

/**
 * Evento publicado en el tópico Kafka "pagos.alertas-fraude" (key = clientId)
 * cuando el velocity check bloquea a un cliente o cuando un cliente bloqueado vuelve a intentar pagar.
 * tipo: BLOQUEO_APLICADO | INTENTO_BLOQUEADO
 */
public record AlertaFraudeEvento(
        String eventId,
        String clientId,
        String tipo,
        long intentosEnVentana,
        int ventanaSegundos,
        Long orderId,
        Instant bloqueadoHasta,
        Instant occurredAt) {}
