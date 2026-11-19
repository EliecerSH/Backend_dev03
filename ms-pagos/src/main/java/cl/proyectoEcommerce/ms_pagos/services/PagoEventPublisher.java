package cl.proyectoEcommerce.ms_pagos.services;

import cl.proyectoEcommerce.ms_pagos.events.PagoEvento;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Bitácora de auditoría: publica cada hito del pago en el tópico Kafka "auditoria.eventos",
 * que consume ms-auditoria (reemplaza a la antigua cola RabbitMQ pedidos360.auditoria.queue).
 *
 * El tipo de evento viaja en el header "tipo-evento" con el mismo nombre que antes tenía la
 * routing key (pedidos360.pago.iniciado | authorized | rejected | cancelled | bloqueado),
 * así la pantalla de Auditoría del frontend no cambia. Best effort: nunca interrumpe el pago.
 */
@Component
public class PagoEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(PagoEventPublisher.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public PagoEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${pagos.kafka.topics.auditoria:auditoria.eventos}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publicar(Long orderId, String clientId, String status, BigDecimal amount) {
        String tipo = "pedidos360.pago." + status.toLowerCase();
        try {
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, String.valueOf(orderId),
                    new PagoEvento(orderId, clientId, status, amount, Instant.now()));
            record.headers().add("tipo-evento", tipo.getBytes(StandardCharsets.UTF_8));
            record.headers().add("event-id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("origen", "ms-pagos".getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).whenComplete((r, ex) -> {
                if (ex != null) log.warn("No se pudo auditar {} de la orden {}: {}", tipo, orderId, ex.getMessage());
            });
        } catch (Exception e) {
            log.warn("No se pudo auditar {} de la orden {}: {}", tipo, orderId, e.getMessage());
        }
    }
}
