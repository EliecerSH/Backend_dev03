package cl.proyectoEcommerce.ms_recomendaciones.services;

import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Accion;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Producto;
import cl.proyectoEcommerce.ms_recomendaciones.events.InteraccionEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Productor del tópico recomendaciones.interacciones. */
@Service
public class InteraccionService {
    private static final Logger log = LoggerFactory.getLogger(InteraccionService.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final CatalogoService catalogoService;
    private final String topic;

    public InteraccionService(KafkaTemplate<String, Object> kafkaTemplate, CatalogoService catalogoService,
                              @Value("${recomendaciones.kafka.topics.interacciones}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.catalogoService = catalogoService;
        this.topic = topic;
    }

    /** La categoría se resuelve en el backend desde el catálogo: el navegador solo envía id y acción. */
    public InteraccionEvento registrar(String usuarioId, Long productoId, String accionTexto) {
        Accion accion = Accion.desde(accionTexto);
        Producto producto = catalogoService.buscar(productoId)
                .orElseThrow(() -> new NoSuchElementException("El producto " + productoId + " no existe en el catálogo."));
        InteraccionEvento evento = new InteraccionEvento(UUID.randomUUID().toString(), usuarioId, productoId,
                producto.categoria(), accion.name(), accion.peso(), "WEB", Instant.now());
        enviar(evento);
        return evento;
    }

    /**
     * Prueba de carga / entrenamiento (equivalente a /generar-lote del ejemplo CloudNative):
     * genera interacciones aleatorias para usuarios demo sobre el catálogo real.
     */
    public int simular(int cantidad, int usuarios) {
        List<Producto> productos = new ArrayList<>(catalogoService.productos());
        if (productos.isEmpty()) throw new IllegalStateException("El catálogo está vacío o ms-productos no responde.");
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int enviados = 0;
        for (int i = 0; i < cantidad; i++) {
            Producto p = productos.get(rnd.nextInt(productos.size()));
            int dado = rnd.nextInt(10); // 60% vista, 30% carrito, 10% compra
            Accion accion = dado < 6 ? Accion.VISTA : dado < 9 ? Accion.CARRITO : Accion.COMPRA;
            String usuario = "demo-user-" + (1 + rnd.nextInt(Math.max(1, usuarios)));
            kafkaTemplate.send(topic, usuario, new InteraccionEvento(UUID.randomUUID().toString(), usuario,
                    p.id(), p.categoria(), accion.name(), accion.peso(), "SIMULACION", Instant.now()));
            enviados++;
        }
        kafkaTemplate.flush();
        log.info("Simulación: {} interacciones enviadas a {}", enviados, topic);
        return enviados;
    }

    private void enviar(InteraccionEvento evento) {
        try {
            kafkaTemplate.send(topic, evento.usuarioId(), evento).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Se interrumpió el envío de la interacción.", e);
        } catch (Exception e) {
            throw new org.springframework.kafka.KafkaException("Kafka no confirmó la interacción.", e);
        }
    }
}
