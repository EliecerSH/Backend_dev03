package cl.proyectoEcommerce.ms_pagos.config;

import cl.proyectoEcommerce.ms_pagos.dtos.IntentoPagoDTO;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración Kafka de ms-pagos.
 *
 * Tópicos que produce este servicio:
 *  - pagos.intentos        : cada intento de pago (key = clientId -> orden garantizado por cliente).
 *                            Lo consume el propio ms-pagos (velocity check antifraude).
 *  - pagos.resultados      : resultado final de Webpay (key = orderId). Lo consumen ms-ordenes y ms-recomendaciones.
 *  - pagos.alertas-fraude  : bloqueos antifraude (key = clientId) para monitoreo / auditoría.
 *
 * Un mensaje que falla 3 veces se envía a {tópico}.DLT para no bloquear la partición (poison pill).
 */
@Configuration
public class KafkaConfig {

    @Bean
    ProducerFactory<String, Object> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        JacksonJsonSerializer<Object> serializer = new JacksonJsonSerializer<>();
        serializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), serializer);
    }

    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    ConsumerFactory<String, IntentoPagoDTO> intentoPagoConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id:ms-pagos-antifraude}") String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        JacksonJsonDeserializer<IntentoPagoDTO> json =
                new JacksonJsonDeserializer<>(IntentoPagoDTO.class).ignoreTypeHeaders();
        // ErrorHandlingDeserializer evita que un JSON inválido deje al consumidor en un loop infinito
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new ErrorHandlingDeserializer<>(json));
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
        // 1 intento + 2 reintentos con 1 s de espera; luego DLT
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, IntentoPagoDTO> kafkaListenerContainerFactory(
            ConsumerFactory<String, IntentoPagoDTO> intentoPagoConsumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, IntentoPagoDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(intentoPagoConsumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    // Los tópicos también se crean con el contenedor kafka-init; estas declaraciones
    // permiten levantar el servicio contra un Kafka vacío (por ejemplo en local).
    @Bean
    KafkaAdmin.NewTopics pagosTopics(
            @Value("${pagos.kafka.topics.intentos}") String intentos,
            @Value("${pagos.kafka.topics.resultados}") String resultados,
            @Value("${pagos.kafka.topics.alertas}") String alertas,
            @Value("${pagos.kafka.partitions:3}") int partitions,
            @Value("${pagos.kafka.replicas:1}") int replicas) {
        return new KafkaAdmin.NewTopics(
                topic(intentos, partitions, replicas), topic(intentos + ".DLT", 1, replicas),
                topic(resultados, partitions, replicas), topic(resultados + ".DLT", 1, replicas),
                topic(alertas, partitions, replicas), topic(alertas + ".DLT", 1, replicas));
    }

    private static NewTopic topic(String name, int partitions, int replicas) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).build();
    }
}
