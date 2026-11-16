package cl.proyectoEcommerce.ms_pagos.services;

import cl.proyectoEcommerce.ms_pagos.dtos.IntentoPagoDTO;
import cl.proyectoEcommerce.ms_pagos.events.AlertaFraudeEvento;
import cl.proyectoEcommerce.ms_pagos.events.PagoResultadoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/** Único punto de publicación hacia Kafka de ms-pagos. */
@Component
public class PagoKafkaPublisher {
    private static final Logger log = LoggerFactory.getLogger(PagoKafkaPublisher.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topicIntentos;
    private final String topicResultados;
    private final String topicAlertas;

    public PagoKafkaPublisher(KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${pagos.kafka.topics.intentos}") String topicIntentos,
            @Value("${pagos.kafka.topics.resultados}") String topicResultados,
            @Value("${pagos.kafka.topics.alertas}") String topicAlertas) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicIntentos = topicIntentos;
        this.topicResultados = topicResultados;
        this.topicAlertas = topicAlertas;
    }

    /**
     * Registra el intento en pagos.intentos. Es bloqueante: si Kafka no confirma (acks=all),
     * no se crea la transacción Webpay, porque el antifraude no podría contabilizar el intento.
     */
    public void publicarIntento(IntentoPagoDTO intento) {
        enviarSincrono(topicIntentos, intento.clientId(), intento);
    }

    /** Publica el resultado final del pago. Lanza KafkaException si falla (el llamador lo reintenta). */
    public void publicarResultado(PagoResultadoEvento evento) {
        enviarSincrono(topicResultados, String.valueOf(evento.orderId()), evento);
    }

    /** Alertas: best effort, no deben interrumpir el flujo de pago. */
    public void publicarAlerta(AlertaFraudeEvento alerta) {
        try {
            enviarSincrono(topicAlertas, alerta.clientId(), alerta);
        } catch (RuntimeException e) {
            log.warn("No se pudo publicar la alerta de fraude del cliente {}: {}", alerta.clientId(), e.getMessage());
        }
    }

    private void enviarSincrono(String topic, String key, Object payload) {
        try {
            SendResult<String, Object> result = kafkaTemplate.send(topic, key, payload).get(10, TimeUnit.SECONDS);
            log.info("Evento publicado en Kafka topic={} partition={} offset={} key={}",
                    topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset(), key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("Se interrumpió la publicación en " + topic, e);
        } catch (Exception e) {
            log.error("Kafka no confirmó el evento en {} (key={})", topic, key, e);
            throw new KafkaException("Kafka no confirmó el evento en " + topic, e);
        }
    }
}
