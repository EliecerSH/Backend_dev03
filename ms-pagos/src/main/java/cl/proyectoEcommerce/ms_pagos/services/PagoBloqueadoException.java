package cl.proyectoEcommerce.ms_pagos.services;

import java.time.Instant;

public class PagoBloqueadoException extends RuntimeException {
    private final Instant bloqueadoHasta;
    public PagoBloqueadoException(Instant bloqueadoHasta) {
        super("Detectamos demasiados intentos de pago seguidos. Por seguridad tu cuenta quedó bloqueada para pagos temporalmente.");
        this.bloqueadoHasta = bloqueadoHasta;
    }
    public Instant getBloqueadoHasta() { return bloqueadoHasta; }
}
