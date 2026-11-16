package cl.proyectoEcommerce.ms_recomendaciones.dtos;

import java.math.BigDecimal;

public record RecomendacionDTO(Long productoId, String nombre, String categoria, BigDecimal precio,
                               Integer stock, String imagenUrl, double score, String motivo) {}
