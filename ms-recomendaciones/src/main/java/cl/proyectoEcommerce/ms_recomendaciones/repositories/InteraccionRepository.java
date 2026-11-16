package cl.proyectoEcommerce.ms_recomendaciones.repositories;

import cl.proyectoEcommerce.ms_recomendaciones.models.Interaccion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface InteraccionRepository extends JpaRepository<Interaccion, Long> {

    boolean existsByEventId(String eventId);

    List<Interaccion> findTop200ByUsuarioIdAndOcurridoEnAfterOrderByOcurridoEnDesc(String usuarioId, Instant desde);

    /** Popularidad: suma de pesos por producto en la ventana. Fila = [productoId, sumaPesos]. */
    @Query("select i.productoId, sum(i.peso) from Interaccion i where i.ocurridoEn > :desde group by i.productoId")
    List<Object[]> popularidadDesde(@Param("desde") Instant desde);

    @Query("select distinct i.usuarioId from Interaccion i where i.productoId = :productoId and i.ocurridoEn > :desde")
    List<String> usuariosQueInteractuaronCon(@Param("productoId") Long productoId, @Param("desde") Instant desde);

    /** Co-ocurrencia: qué otros productos tocaron esos usuarios. Fila = [productoId, sumaPesos]. */
    @Query("select i.productoId, sum(i.peso) from Interaccion i where i.usuarioId in :usuarios " +
            "and i.productoId <> :productoId and i.ocurridoEn > :desde group by i.productoId")
    List<Object[]> coocurrencia(@Param("usuarios") Collection<String> usuarios,
                                @Param("productoId") Long productoId,
                                @Param("desde") Instant desde);

    long countByOcurridoEnAfter(Instant desde);
}
