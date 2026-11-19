package cl.proyectoEcommerce.ms_recomendaciones.services;

import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Accion;
import cl.proyectoEcommerce.ms_recomendaciones.events.InteraccionEvento;
import cl.proyectoEcommerce.ms_recomendaciones.events.PagoResultadoEvento;
import cl.proyectoEcommerce.ms_recomendaciones.store.InteraccionRegistro;
import cl.proyectoEcommerce.ms_recomendaciones.store.InteraccionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/** Consumidores Kafka de ms-recomendaciones: persisten en DynamoDB de forma idempotente (por eventId). */
@Component
public class EventosConsumer {
    private static final Logger log = LoggerFactory.getLogger(EventosConsumer.class);
    private final InteraccionStore store;
    private final CatalogoService catalogoService;

    public EventosConsumer(InteraccionStore store, CatalogoService catalogoService) {
        this.store = store;
        this.catalogoService = catalogoService;
    }

    @KafkaListener(topics = "${recomendaciones.kafka.topics.interacciones}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onInteraccion(InteraccionEvento e) {
        if (e == null || e.usuarioId() == null || e.productoId() == null || e.eventId() == null) {
            throw new IllegalArgumentException("Evento de interacción incompleto: " + e); // -> DLT
        }
        Accion accion = Accion.desde(e.accion());
        store.guardar(new InteraccionRegistro(e.eventId(), e.usuarioId(), e.productoId(), e.categoria(),
                accion.name(), accion.peso(), e.origen(), e.ocurridoEn() == null ? Instant.now() : e.ocurridoEn()));
    }

    /** Una compra autorizada es la señal más fuerte: cada producto pagado se registra como COMPRA. */
    @KafkaListener(topics = "${recomendaciones.kafka.topics.pagos-resultados}",
            containerFactory = "pagoResultadoListenerFactory")
    public void onPagoResultado(PagoResultadoEvento e) {
        if (e == null || !"AUTHORIZED".equals(e.status()) || e.items() == null) return;
        Instant cuando = e.occurredAt() == null ? Instant.now() : e.occurredAt();
        int nuevos = 0;
        for (PagoResultadoEvento.Item item : e.items()) {
            if (item.productoId() == null) continue;
            String categoria = catalogoService.buscar(item.productoId()).map(p -> p.categoria()).orElse(null);
            // eventId del pago + producto: si ms-pagos republica el mismo resultado, no se duplica
            boolean guardado = store.guardar(new InteraccionRegistro(e.eventId() + "#" + item.productoId(),
                    e.clientId(), item.productoId(), categoria, Accion.COMPRA.name(), Accion.COMPRA.peso(),
                    "PAGO", cuando));
            if (guardado) nuevos++;
        }
        log.info("Compra de la orden {} registrada para recomendaciones ({} productos nuevos)", e.orderId(), nuevos);
    }
}
