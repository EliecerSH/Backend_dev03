package cl.proyectoEcommerce.ms_recomendaciones.services;

import cl.proyectoEcommerce.ms_recomendaciones.dtos.ProductoDTO;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Producto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Copia en memoria del catálogo de ms-productos (endpoint público).
 * Se refresca periódicamente; si ms-productos cae, se sigue recomendando con la última copia conocida.
 */
@Service
public class CatalogoService {
    private static final Logger log = LoggerFactory.getLogger(CatalogoService.class);
    private final RestClient productosRestClient;
    private volatile Map<Long, Producto> catalogo = Map.of();

    public CatalogoService(RestClient productosRestClient) {
        this.productosRestClient = productosRestClient;
    }

    @Scheduled(fixedDelayString = "${recomendaciones.catalogo.refresh-ms:300000}", initialDelay = 5_000)
    public void refrescar() {
        try {
            List<ProductoDTO> productos = productosRestClient.get().retrieve()
                    .body(new ParameterizedTypeReference<List<ProductoDTO>>() {});
            if (productos == null) return;
            Map<Long, Producto> nuevo = new LinkedHashMap<>();
            for (ProductoDTO p : productos) {
                if (p.id() == null) continue;
                nuevo.put(p.id(), new Producto(p.id(), p.nombre(), p.categoria(), p.precio(), p.stock(), p.imagenUrl()));
            }
            catalogo = Map.copyOf(nuevo);
            log.info("Catálogo sincronizado desde ms-productos: {} productos", nuevo.size());
        } catch (Exception e) {
            log.warn("No se pudo sincronizar el catálogo desde ms-productos ({}). Se usa la copia anterior ({} productos).",
                    e.getMessage(), catalogo.size());
        }
    }

    public Collection<Producto> productos() {
        if (catalogo.isEmpty()) refrescar();
        return catalogo.values();
    }

    public Optional<Producto> buscar(Long id) {
        if (id == null) return Optional.empty();
        Producto p = catalogo.get(id);
        if (p == null) {
            refrescar(); // producto nuevo creado después del último refresco
            p = catalogo.get(id);
        }
        return Optional.ofNullable(p);
    }
}
