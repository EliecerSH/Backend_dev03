package cl.proyectoEcommerce.ms_recomendaciones.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class CatalogoClientConfig {
    @Bean
    RestClient productosRestClient(@Value("${services.productos.url}") String url) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(3_000);
        requestFactory.setReadTimeout(5_000);
        return RestClient.builder().baseUrl(url).requestFactory(requestFactory).build();
    }
}
