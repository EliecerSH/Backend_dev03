package cl.proyectoEcommerce.ms_recomendaciones.services;

import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Accion;
import cl.proyectoEcommerce.ms_recomendaciones.events.InteraccionEvento;
import cl.proyectoEcommerce.ms_recomendaciones.events.PagoResultadoEvento;
import cl.proyectoEcommerce.ms_recomendaciones.models.Interaccion;
import cl.proyectoEcommerce.ms_recomendaciones.repositories.InteraccionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/** Consumidores Kafka de ms-recomendaciones. Ambos son idempotentes (event_id único). */
@Component
public class EventosConsumer {
    private static final Logger log = LoggerFactory.getLogger(EventosConsumer.class);
    private final InteraccionRepository repository;
    private final CatalogoService catalogoService;

    public EventosConsumer(InteraccionRepository repository, CatalogoService catalogoService) {
        this.repository = repository;
        this.catalogoService = catalogoService;
    }

    @KafkaListener(topics = "${recomendaciones.kafka.topics.interacciones}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onInteraccion(InteraccionEvento e) {
        if (e == null || e.usuarioId() == null || e.productoId() == null || e.eventId() == null) {
            throw new IllegalArgumentException("Evento de interacción incompleto: " + e); // -> DLT
        }
        Accion accion = Accion.desde(e.accion());
        guardar(new Interaccion(e.eventId(), e.usuarioId(), e.productoId(), e.categoria(), accion.name(),
                accion.peso(), e.origen(), e.ocurridoEn() == null ? Instant.now() : e.ocurridoEn()));
    }

    /** Una compra autorizada es la señal más fuerte: cada producto pagado se registra como COMPRA. */
    @KafkaListener(topics = "${recomendaciones.kafka.topics.pagos-resultados}",
            containerFactory = "pagoResultadoListenerFactory")
    public void onPagoResultado(PagoResultadoEvento e) {
        if (e == null || !"AUTHORIZED".equals(e.status()) || e.items() == null) return;
        Instant cuando = e.occurredAt() == null ? Instant.now() : e.occurredAt();
        for (PagoResultadoEvento.Item item : e.items()) {
            if (item.productoId() == null) continue;
            String categoria = catalogoService.buscar(item.productoId()).map(p -> p.categoria()).orElse(null);
            guardar(new Interaccion(e.eventId() + "#" + item.productoId(), e.clientId(), item.productoId(),
                    categoria, Accion.COMPRA.name(), Accion.COMPRA.peso(), "PAGO", cuando));
        }
        log.info("Compra de la orden {} registrada para recomendaciones ({} productos)", e.orderId(), e.items().size());
    }

    private void guardar(Interaccion interaccion) {
        if (repository.existsByEventId(interaccion.getEventId())) {
            log.debug("Evento duplicado ignorado: {}", interaccion.getEventId());
            return;
        }
        try {
            repository.save(interaccion);
        } catch (DataIntegrityViolationException dup) {
            log.debug("Evento duplicado (carrera) ignorado: {}", interaccion.getEventId());
        }
    }
}
