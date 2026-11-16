package cl.proyectoEcommerce.ms_pagos.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class OrdenClientConfig {
    @Bean
    RestClient ordenesRestClient(@Value("${services.ordenes.url:http://localhost:8084/api/v1/ordenes}") String url) {
        // Timeouts explícitos: si ms-ordenes no responde, el usuario recibe 503 en segundos y no un cuelgue
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(3_000);
        requestFactory.setReadTimeout(5_000);
        return RestClient.builder().baseUrl(url).requestFactory(requestFactory).build();
    }
}
