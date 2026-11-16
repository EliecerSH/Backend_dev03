package cl.proyectoEcommerce.ms_recomendaciones.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;

/** Producto tal como lo expone ms-productos (GET /api/v1/productos). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProductoDTO(Long id, String nombre, BigDecimal precio, Integer stock, String categoria,
                          String imagenUrl) {}
