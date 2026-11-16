package cl.proyectoEcommerce.ms_pagos.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.List;

/** Vista mínima de la orden que expone ms-ordenes en GET /api/v1/ordenes/{id}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrdenResponseDTO(Long id, String usuarioOid, List<ItemOrdenDTO> items, BigDecimal total, String estado) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ItemOrdenDTO(Long productoId, Integer cantidad, BigDecimal precioUnitario) {}
}
