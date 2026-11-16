package cl.proyectoEcommerce.ms_recomendaciones.services;

import cl.proyectoEcommerce.ms_recomendaciones.dtos.RecomendacionDTO;
import cl.proyectoEcommerce.ms_recomendaciones.dtos.RecomendacionesResponse;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Interaccion;
import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Recomendacion;
import cl.proyectoEcommerce.ms_recomendaciones.repositories.InteraccionRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class RecomendacionService {
    private static final int LIMITE_MAXIMO = 20;
    private final InteraccionRepository repository;
    private final CatalogoService catalogoService;
    private final int ventanaDias;

    public RecomendacionService(InteraccionRepository repository, CatalogoService catalogoService,
                                @Value("${recomendaciones.ventana-dias:30}") int ventanaDias) {
        this.repository = repository;
        this.catalogoService = catalogoService;
        this.ventanaDias = ventanaDias;
    }

    @Transactional(readOnly = true)
    public RecomendacionesResponse paraUsuario(String usuarioId, int limite) {
        Instant ahora = Instant.now();
        List<Interaccion> historial = repository
                .findTop200ByUsuarioIdAndOcurridoEnAfterOrderByOcurridoEnDesc(usuarioId, desde(ahora)).stream()
                .map(i -> new Interaccion(i.getProductoId(), i.getCategoria(), i.getAccion(), i.getPeso(), i.getOcurridoEn()))
                .toList();
        List<Recomendacion> r = MotorRecomendaciones.personalizadas(historial, popularidad(ahora),
                catalogoService.productos(), ahora, acotar(limite));
        return new RecomendacionesResponse(historial.isEmpty() ? "POPULARES" : "PERSONALIZADA", toDto(r));
    }

    @Transactional(readOnly = true)
    public RecomendacionesResponse populares(int limite) {
        Instant ahora = Instant.now();
        return new RecomendacionesResponse("POPULARES", toDto(MotorRecomendaciones.populares(popularidad(ahora),
                catalogoService.productos(), Set.of(), acotar(limite))));
    }

    @Transactional(readOnly = true)
    public RecomendacionesResponse similares(Long productoId, int limite) {
        Instant desde = desde(Instant.now());
        List<String> usuarios = repository.usuariosQueInteractuaronCon(productoId, desde);
        Map<Long, Double> co = usuarios.isEmpty() ? Map.of()
                : agregar(repository.coocurrencia(usuarios, productoId, desde));
        return new RecomendacionesResponse("SIMILARES", toDto(MotorRecomendaciones.similares(productoId, co,
                catalogoService.productos(), acotar(limite))));
    }

    private Map<Long, Double> popularidad(Instant ahora) {
        return agregar(repository.popularidadDesde(desde(ahora)));
    }

    private Instant desde(Instant ahora) {
        return ahora.minus(ventanaDias, ChronoUnit.DAYS);
    }

    private static Map<Long, Double> agregar(List<Object[]> filas) {
        Map<Long, Double> m = new HashMap<>();
        for (Object[] f : filas) m.put(((Number) f[0]).longValue(), ((Number) f[1]).doubleValue());
        return m;
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
