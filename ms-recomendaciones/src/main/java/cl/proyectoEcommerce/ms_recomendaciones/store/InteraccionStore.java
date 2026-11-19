package cl.proyectoEcommerce.ms_recomendaciones.store;

import cl.proyectoEcommerce.ms_recomendaciones.engine.MotorRecomendaciones.Interaccion;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Persistencia de interacciones. Implementación productiva: {@link DynamoInteraccionStore}. */
public interface InteraccionStore {

    /** @return true si se guardó; false si el evento ya existía (re-entrega de Kafka). */
    boolean guardar(InteraccionRegistro registro);

    /** Interacciones del usuario desde {@code desde}, de la más reciente a la más antigua. */
    List<Interaccion> historialUsuario(String usuarioId, Instant desde, int limite);

    /** productoId -> suma de pesos de todos los usuarios desde {@code desde}. */
    Map<Long, Double> popularidad(Instant desde);

    /** productoId -> peso acumulado entre los usuarios que también interactuaron con {@code productoId}. */
    Map<Long, Double> coocurrencia(Long productoId, Instant desde);
}
