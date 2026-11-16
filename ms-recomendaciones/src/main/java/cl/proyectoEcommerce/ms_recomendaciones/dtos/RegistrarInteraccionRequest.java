package cl.proyectoEcommerce.ms_recomendaciones.dtos;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/** accion: VISTA | CARRITO | COMPRA (también acepta view / cart / purchase). */
public record RegistrarInteraccionRequest(@NotNull @Positive Long productoId, @NotBlank String accion) {}
