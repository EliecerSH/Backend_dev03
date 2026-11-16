package cl.proyectoEcommerce.ms_recomendaciones.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Vista del evento que ms-pagos publica en "pagos.resultados". Solo interesan los AUTHORIZED. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PagoResultadoEvento(
        String eventId,
        Long orderId,
        String clientId,
        String status,
        List<Item> items,
        Instant occurredAt) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Item(Long productoId, Integer cantidad, BigDecimal precioUnitario) {}
}
