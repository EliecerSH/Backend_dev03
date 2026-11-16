package cl.proyectoEcommerce.ms_recomendaciones.dtos;

import java.util.List;

/** estrategia: PERSONALIZADA | POPULARES | SIMILARES */
public record RecomendacionesResponse(String estrategia, List<RecomendacionDTO> items) {}
