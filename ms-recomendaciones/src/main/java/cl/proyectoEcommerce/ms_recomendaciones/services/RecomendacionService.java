package cl.proyectoEcommerce.ms_recomendaciones.services;

import cl.proyectoEcommerce.ms_recomendaciones.dtos.RecomendacionDTO;
import cl.proyectoEcommerce.ms_recomendaciones.dtos.RecomendacionesResponse;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Interaccion;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Recomendacion;
import cl.proyectoEcommerce.ms_recomendaciones.store.InteraccionStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class RecomendacionService {
    private static final int LIMITE_MAXIMO = 20;
    private static final int MAX_HISTORIAL = 200;
    private final InteraccionStore store;
    private final CatalogoService catalogoService;
    private final int ventanaDias;

    public RecomendacionService(InteraccionStore store, CatalogoService catalogoService,
                                @Value("${recomendaciones.ventana-dias:30}") int ventanaDias) {
        this.store = store;
        this.catalogoService = catalogoService;
        this.ventanaDias = ventanaDias;
    }

    public RecomendacionesResponse paraUsuario(String usuarioId, int limite) {
        Instant ahora = Instant.now();
        List<Interaccion> historial = store.historialUsuario(usuarioId, desde(ahora), MAX_HISTORIAL);
        List<Recomendacion> r = MotorRecomendaciones.personalizadas(historial, store.popularidad(desde(ahora)),
                catalogoService.productos(), ahora, acotar(limite));
        return new RecomendacionesResponse(historial.isEmpty() ? "POPULARES" : "PERSONALIZADA", toDto(r));
    }

    public RecomendacionesResponse populares(int limite) {
        Instant ahora = Instant.now();
        return new RecomendacionesResponse("POPULARES", toDto(MotorRecomendaciones.populares(
                store.popularidad(desde(ahora)), catalogoService.productos(), Set.of(), acotar(limite))));
    }

    public RecomendacionesResponse similares(Long productoId, int limite) {
        Map<Long, Double> co = store.coocurrencia(productoId, desde(Instant.now()));
        return new RecomendacionesResponse("SIMILARES", toDto(MotorRecomendaciones.similares(productoId, co,
                catalogoService.productos(), acotar(limite))));
    }

    private Instant desde(Instant ahora) {
        return ahora.minus(ventanaDias, ChronoUnit.DAYS);
    }

    private static int acotar(int limite) {
        return Math.max(1, Math.min(limite, LIMITE_MAXIMO));
    }

    private static List<RecomendacionDTO> toDto(List<Recomendacion> r) {
        return r.stream().map(x -> new RecomendacionDTO(x.producto().id(), x.producto().nombre(),
                x.producto().categoria(), x.producto().precio(), x.producto().stock(), x.producto().imagenUrl(),
                x.score(), x.motivo())).toList();
    }
}
