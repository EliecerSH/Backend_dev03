package cl.proyectoEcommerce.ms_pagos.dtos;

import java.math.BigDecimal;
import java.time.Instant;

/** Estado del pago de una orden, para que el frontend muestre el resultado en "Mis pedidos". */
public record EstadoPagoDTO(Long orderId, String status, BigDecimal amount, String authorizationCode,
                            Instant updatedAt) {}
