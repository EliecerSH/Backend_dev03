package cl.proyectoEcommerce.ms_recomendaciones.controllers;

import cl.proyectoEcommerce.ms_recomendaciones.dtos.RecomendacionesResponse;
import cl.proyectoEcommerce.ms_recomendaciones.dtos.RegistrarInteraccionRequest;
import cl.proyectoEcommerce.ms_recomendaciones.events.InteraccionEvento;
import cl.proyectoEcommerce.ms_recomendaciones.services.InteraccionService;
import cl.proyectoEcommerce.ms_recomendaciones.services.RecomendacionService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/recomendaciones")
@Validated
public class RecomendacionController {
    private final RecomendacionService recomendacionService;
    private final InteraccionService interaccionService;

    public RecomendacionController(RecomendacionService recomendacionService, InteraccionService interaccionService) {
        this.recomendacionService = recomendacionService;
        this.interaccionService = interaccionService;
    }

    /** Recomendaciones personalizadas del usuario autenticado (claim oid de Azure AD). */
    @GetMapping("/me")
    public RecomendacionesResponse misRecomendaciones(@AuthenticationPrincipal Jwt jwt,
                                                      @RequestParam(defaultValue = "8") int limit) {
        return recomendacionService.paraUsuario(usuarioId(jwt), limit);
    }

    /** Público: productos más populares de los últimos días (home de usuarios anónimos). */
    @GetMapping("/populares")
    public RecomendacionesResponse populares(@RequestParam(defaultValue = "8") int limit) {
        return recomendacionService.populares(limit);
    }

    /** Público: "clientes que vieron este producto también vieron…" para la ficha de producto. */
    @GetMapping("/productos/{productoId}/similares")
    public RecomendacionesResponse similares(@PathVariable Long productoId,
                                             @RequestParam(defaultValue = "4") int limit) {
        return recomendacionService.similares(productoId, limit);
    }

    /** El frontend registra VISTA / CARRITO; se publica en Kafka y se procesa de forma asíncrona (202). */
    @PostMapping("/interacciones")
    public ResponseEntity<InteraccionEvento> registrar(@AuthenticationPrincipal Jwt jwt,
                                                       @Valid @RequestBody RegistrarInteraccionRequest request) {
        InteraccionEvento evento = interaccionService.registrar(usuarioId(jwt), request.productoId(), request.accion());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(evento);
    }

    /** Solo Admin: genera tráfico aleatorio para demostrar el flujo Kafka (prueba de carga). */
    @PostMapping("/admin/simular")
    public ResponseEntity<Map<String, Object>> simular(
            @RequestParam(defaultValue = "50") @Min(1) @Max(5000) int cantidad,
            @RequestParam(defaultValue = "5") @Min(1) @Max(100) int usuarios) {
        int enviados = interaccionService.simular(cantidad, usuarios);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "enviados", enviados,
                "mensaje", "Interacciones publicadas en Kafka. Consulta /populares en unos segundos."));
    }

    private static String usuarioId(Jwt jwt) {
        String oid = jwt.getClaimAsString("oid");
        return (oid == null || oid.isBlank()) ? jwt.getSubject() : oid;
    }
}
