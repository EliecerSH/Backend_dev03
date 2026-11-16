package cl.proyectoEcommerce.ms_recomendaciones;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MsRecomendacionesApplication {
    public static void main(String[] args) {
        SpringApplication.run(MsRecomendacionesApplication.class, args);
    }
}
