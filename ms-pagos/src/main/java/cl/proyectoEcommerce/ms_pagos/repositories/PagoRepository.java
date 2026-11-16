package cl.proyectoEcommerce.ms_pagos.repositories;

import cl.proyectoEcommerce.ms_pagos.entities.Pago;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface PagoRepository extends JpaRepository<Pago, Long> {
    Optional<Pago> findByOrderId(Long orderId);
    Optional<Pago> findByWebpayToken(String token);
    Optional<Pago> findByBuyOrder(String buyOrder);

    /** Pagos finalizados cuyo evento aún no fue confirmado por Kafka (outbox simplificado). */
    @Query("select p from Pago p where p.status in :statuses " +
            "and (p.resultadoPublicado is null or p.resultadoPublicado = false) order by p.updatedAt")
    List<Pago> findResultadosPendientesDePublicar(@Param("statuses") Collection<String> statuses);
}
