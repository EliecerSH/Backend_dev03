package cl.proyectoEcommerce.ms_recomendaciones.config;

import cl.proyectoEcommerce.ms_recomendaciones.store.DynamoKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

import java.net.URI;

/**
 * Cliente DynamoDB (AWS SDK for Java v2).
 *
 * Credenciales: cadena por defecto del SDK. En la EC2 de AWS Academy se usan las del rol
 * LabInstanceProfile/LabRole a través del metadata service: NO se ponen llaves en el código ni en el .env.
 * En local se puede apuntar a DynamoDB Local con DYNAMODB_ENDPOINT.
 */
@Configuration
public class DynamoConfig {
    private static final Logger log = LoggerFactory.getLogger(DynamoConfig.class);

    @Bean(destroyMethod = "close")
    DynamoDbClient dynamoDbClient(@Value("${recomendaciones.dynamodb.region:us-east-1}") String region,
                                  @Value("${recomendaciones.dynamodb.endpoint:}") String endpoint) {
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.builder().build());
        if (endpoint != null && !endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint));
            log.info("DynamoDB apuntando a endpoint local {}", endpoint);
        }
        return builder.build();
    }

    /** Crea la tabla (on-demand) y activa el TTL si no existe. Idempotente. */
    @Bean
    ApplicationRunner crearTablaDynamo(DynamoDbClient dynamo,
                                       @Value("${recomendaciones.dynamodb.table}") String tabla,
                                       @Value("${recomendaciones.dynamodb.create-table:true}") boolean crear) {
        return args -> {
            try {
                dynamo.describeTable(DescribeTableRequest.builder().tableName(tabla).build());
                log.info("Tabla DynamoDB '{}' disponible", tabla);
                return;
            } catch (ResourceNotFoundException noExiste) {
                if (!crear) {
                    log.error("La tabla DynamoDB '{}' no existe y DYNAMODB_CREATE_TABLE=false. Créala antes de usar el servicio.", tabla);
                    return;
                }
            } catch (Exception e) {
                log.error("No se pudo consultar DynamoDB ({}). Revisa credenciales/rol IAM y región.", e.getMessage());
                return;
            }
            try {
                dynamo.createTable(CreateTableRequest.builder()
                        .tableName(tabla)
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .attributeDefinitions(
                                AttributeDefinition.builder().attributeName(DynamoKeys.PK).attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName(DynamoKeys.SK).attributeType(ScalarAttributeType.S).build())
                        .keySchema(
                                KeySchemaElement.builder().attributeName(DynamoKeys.PK).keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName(DynamoKeys.SK).keyType(KeyType.RANGE).build())
                        .build());
                dynamo.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(tabla).build());
                log.info("Tabla DynamoDB '{}' creada (PAY_PER_REQUEST)", tabla);
            } catch (ResourceInUseException yaCreada) {
                log.info("Tabla DynamoDB '{}' ya estaba siendo creada", tabla);
            } catch (Exception e) {
                log.error("No se pudo crear la tabla DynamoDB '{}': {}", tabla, e.getMessage());
                return;
            }
            try {
                dynamo.updateTimeToLive(UpdateTimeToLiveRequest.builder().tableName(tabla)
                        .timeToLiveSpecification(TimeToLiveSpecification.builder()
                                .attributeName(DynamoKeys.TTL).enabled(true).build())
                        .build());
                log.info("TTL activado en '{}' (atributo {})", tabla, DynamoKeys.TTL);
            } catch (Exception e) {
                log.warn("No se pudo activar el TTL en '{}': {}", tabla, e.getMessage());
            }
        };
    }
}
