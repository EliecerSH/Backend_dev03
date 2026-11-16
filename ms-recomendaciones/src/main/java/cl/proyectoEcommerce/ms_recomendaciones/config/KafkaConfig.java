package cl.proyectoEcommerce.ms_recomendaciones.config;

import cl.proyectoEcommerce.ms_recomendaciones.events.InteraccionEvento;
import cl.proyectoEcommerce.ms_recomendaciones.events.PagoResultadoEvento;
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
 * Kafka en ms-recomendaciones:
 *  - Produce y consume "recomendaciones.interacciones" (clickstream: vistas y carrito desde el frontend).
 *  - Consume "pagos.resultados" (publicado por ms-pagos) con su propio consumer group, por lo que recibe
 *    TODOS los eventos aunque ms-ordenes también los consuma (publish/subscribe).
 */
@Configuration
public class KafkaConfig {

    private final String bootstrapServers;
    private final String groupId;

    public KafkaConfig(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                       @Value("${spring.kafka.consumer.group-id:ms-recomendaciones}") String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    @Bean
    ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        JacksonJsonSerializer<Object> serializer = new JacksonJsonSerializer<>();
        serializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), serializer);
    }

    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }

    /** Factory por defecto: tópico de interacciones. */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, InteraccionEvento> kafkaListenerContainerFactory(
            DefaultErrorHandler kafkaErrorHandler) {
        return factory(InteraccionEvento.class, kafkaErrorHandler);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, PagoResultadoEvento> pagoResultadoListenerFactory(
            DefaultErrorHandler kafkaErrorHandler) {
        return factory(PagoResultadoEvento.class, kafkaErrorHandler);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> factory(Class<T> type,
                                                                          DefaultErrorHandler errorHandler) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        JacksonJsonDeserializer<T> json = new JacksonJsonDeserializer<>(type).ignoreTypeHeaders();
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new ErrorHandlingDeserializer<>(json)));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @Bean
    KafkaAdmin.NewTopics recomendacionesTopics(
            @Value("${recomendaciones.kafka.topics.interacciones}") String interacciones,
            @Value("${recomendaciones.kafka.topics.pagos-resultados}") String pagosResultados,
            @Value("${recomendaciones.kafka.partitions:3}") int partitions,
            @Value("${recomendaciones.kafka.replicas:1}") int replicas) {
        return new KafkaAdmin.NewTopics(
                topic(interacciones, partitions, replicas),
                topic(interacciones + ".DLT", 1, replicas),
                // pagos.resultados es de ms-pagos; se declara también aquí para tolerar el orden de arranque
                topic(pagosResultados, partitions, replicas),
                topic(pagosResultados + ".DLT", 1, replicas));
    }

    private static NewTopic topic(String name, int partitions, int replicas) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).build();
    }
}
