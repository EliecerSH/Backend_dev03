package cl.proyectoEcommerce.ms_pagos.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.math.BigDecimal;

/** Snapshot de los ítems de la orden al momento de pagar (se publica en pagos.resultados). */
@Embeddable
public class PagoItem {
    @Column(name = "producto_id", nullable = false)
    private Long productoId;
    @Column(nullable = false)
    private Integer cantidad;
    @Column(name = "precio_unitario", precision = 12, scale = 2)
    private BigDecimal precioUnitario;

    protected PagoItem() {}

    public PagoItem(Long productoId, Integer cantidad, BigDecimal precioUnitario) {
        this.productoId = productoId;
        this.cantidad = cantidad;
        this.precioUnitario = precioUnitario;
    }

    public Long getProductoId() { return productoId; }
    public Integer getCantidad() { return cantidad; }
    public BigDecimal getPrecioUnitario() { return precioUnitario; }
}
