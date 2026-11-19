package cl.proyectoEcommerce.ms_pagos.services;

import cl.proyectoEcommerce.ms_pagos.dtos.EstadoPagoDTO;
import cl.proyectoEcommerce.ms_pagos.dtos.IniciarPagoResponse;
import cl.proyectoEcommerce.ms_pagos.dtos.IntentoPagoDTO;
import cl.proyectoEcommerce.ms_pagos.dtos.OrdenResponseDTO;
import cl.proyectoEcommerce.ms_pagos.dtos.PagoResultadoDTO;
import cl.proyectoEcommerce.ms_pagos.entities.Pago;
import cl.proyectoEcommerce.ms_pagos.entities.PagoItem;
import cl.proyectoEcommerce.ms_pagos.events.PagoResultadoEvento;
import cl.proyectoEcommerce.ms_pagos.repositories.PagoRepository;
import cl.transbank.webpay.webpayplus.WebpayPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
public class PagoService {
    private static final Logger log = LoggerFactory.getLogger(PagoService.class);
    /** Estados finales de Webpay que generan un evento en pagos.resultados. */
    static final Set<String> ESTADOS_FINALES = Set.of("AUTHORIZED", "REJECTED", "CANCELLED");
    /** Estados desde los que se permite volver a iniciar un pago. */
    private static final Set<String> ESTADOS_REINTENTABLES = Set.of("ERROR", "REJECTED", "CANCELLED");
    /** Ventana en la que un token Webpay recién creado se reutiliza ante un doble clic. */
    private static final java.time.Duration TOKEN_REUTILIZABLE = java.time.Duration.ofMinutes(2);
    /** Estados de ms-ordenes que admiten un pago. */
    private static final Set<String> ESTADOS_ORDEN_PAGABLES = Set.of("PENDIENTE_PAGO", "PAGO_RECHAZADO");

    private final PagoRepository pagoRepository;
    private final OrdenService ordenService;
    private final AntifraudeService antifraudeService;
    private final PagoEventPublisher auditPublisher;
    private final PagoKafkaPublisher kafkaPublisher;
    private final WebpayPlus.Transaction webpay;
    private final String frontendUrl;
    private final String callbackUrl;

    public PagoService(PagoRepository pagoRepository, OrdenService ordenService, AntifraudeService antifraudeService,
            PagoEventPublisher auditPublisher, PagoKafkaPublisher kafkaPublisher,
            WebpayPlus.Transaction webpay,
            @Value("${app.frontend-url:http://localhost:5173}") String frontendUrl,
            @Value("${transbank.return-url:http://localhost:8087/api/pagos/retorno}") String callbackUrl) {
        this.pagoRepository = pagoRepository;
        this.ordenService = ordenService;
        this.antifraudeService = antifraudeService;
        this.auditPublisher = auditPublisher;
        this.kafkaPublisher = kafkaPublisher;
        this.webpay = webpay;
        this.frontendUrl = frontendUrl.replaceAll("/+$", "");
        this.callbackUrl = callbackUrl;
    }

    /**
     * 1) Valida la orden contra ms-ordenes (monto y dueño los decide el backend, nunca el navegador).
     * 2) Gate antifraude: si el cliente está bloqueado -> 429.
     * 3) Publica el intento en Kafka (pagos.intentos) para el velocity check.
     * 4) Crea la transacción en Webpay y devuelve url + token.
     */
    public IniciarPagoResponse iniciar(Long orderId, String clientId, String bearerToken) {
        OrdenResponseDTO order = ordenService.obtenerOrden(orderId, bearerToken);
        if (order == null || !orderId.equals(order.id())) throw new IllegalArgumentException("No se encontró la orden.");
        if (!clientId.equals(order.usuarioOid())) throw new SecurityException("La orden no pertenece al usuario autenticado.");
        if ("PAGADA".equalsIgnoreCase(order.estado())) throw new IllegalStateException("La orden ya se encuentra pagada.");
        if (order.estado() != null && !ESTADOS_ORDEN_PAGABLES.contains(order.estado().toUpperCase())) {
            // Ej.: PROCESADA (ms-ordenes con PAGO_REQUERIDO=false ya descontó stock sin pasar por Webpay)
            throw new IllegalStateException("La orden no está pendiente de pago (estado " + order.estado()
                    + "). Revisa que ms-ordenes tenga PAGO_REQUERIDO=true.");
        }
        if (order.total() == null || order.total().signum() <= 0) throw new IllegalArgumentException("El total de la orden no es válido.");
        BigDecimal amount;
        try {
            amount = order.total().setScale(0, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Webpay requiere un monto entero en pesos chilenos.");
        }

        Pago existing = pagoRepository.findByOrderId(orderId).orElse(null);
        boolean pendiente = existing != null && "PENDING".equals(existing.getStatus());
        if (pendiente && existing.getWebpayToken() != null && existing.getUpdatedAt() != null
                && existing.getUpdatedAt().isAfter(Instant.now().minus(TOKEN_REUTILIZABLE))) {
            // Doble clic en el botón: se reutiliza la transacción recién creada (no cuenta como nuevo intento)
            return new IniciarPagoResponse(orderId, existing.getWebpayUrl(), existing.getWebpayToken());
        }
        // Un PENDING antiguo es un pago que nunca volvió de Webpay (cliente cerró la pestaña o el retorno falló):
        // ese token ya no sirve, así que se crea una transacción nueva.
        if (existing != null && !pendiente && !ESTADOS_REINTENTABLES.contains(existing.getStatus())) {
            throw new IllegalStateException("Esta orden ya tiene un pago registrado: " + existing.getStatus());
        }

        Instant blockedUntil = antifraudeService.bloqueoActivoHasta(clientId);
        if (blockedUntil != null) {
            auditPublisher.publicar(orderId, clientId, "bloqueado", amount);
            antifraudeService.notificarIntentoBloqueado(clientId, orderId, blockedUntil);
            throw new PagoBloqueadoException(blockedUntil);
        }

        String attemptId = "TX-" + UUID.randomUUID();
        kafkaPublisher.publicarIntento(new IntentoPagoDTO(clientId, amount.longValueExact(), attemptId,
                Instant.now(), orderId));

        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        String buyOrder = "O" + Long.toString(orderId, 36) + "-" + suffix; // máx. 26 caracteres (Webpay)
        String sessionId = UUID.randomUUID().toString();
        List<PagoItem> items = order.items() == null ? List.of() : order.items().stream()
                .map(i -> new PagoItem(i.productoId(), i.cantidad(), i.precioUnitario()))
                .toList();

        Pago pago;
        if (existing == null) {
            pago = new Pago(orderId, clientId, buyOrder, sessionId, amount);
        } else {
            existing.reiniciarTransaccion(buyOrder, sessionId, amount);
            pago = existing;
        }
        pago.reemplazarItems(items);
        pago.setResultadoPublicado(false);
        pago = pagoRepository.save(pago);
        try {
            var response = webpay.create(buyOrder, sessionId, amount.doubleValue(), callbackUrl);
            pago.setWebpayToken(response.getToken());
            pago.setWebpayUrl(response.getUrl());
            pago.setStatus("PENDING");
            pagoRepository.save(pago);
            auditPublisher.publicar(orderId, clientId, "iniciado", amount);
            return new IniciarPagoResponse(orderId, response.getUrl(), response.getToken());
        } catch (Exception e) {
            pago.setStatus("ERROR");
            pagoRepository.save(pago);
            log.error("Error creando transacción Webpay para la orden {}", orderId, e);
            throw new IllegalStateException("No se pudo iniciar el pago en Webpay. Intenta nuevamente.");
        }
    }

    /** Retorno normal de Webpay (token_ws): se confirma con commit() en el backend. */
    public PagoResultadoDTO confirmar(String token) {
        Pago pago = pagoRepository.findByWebpayToken(token)
                .orElseThrow(() -> new IllegalArgumentException("No existe un pago asociado al token recibido."));
        if (ESTADOS_FINALES.contains(pago.getStatus())) {
            // Doble refresco del navegador: no se vuelve a hacer commit
            return new PagoResultadoDTO(pago.getOrderId(), pago.getStatus(), pago.getAmount(),
                    pago.getAuthorizationCode(), "Pago ya procesado.");
        }
        try {
            var response = webpay.commit(token);
            int responseCode = response.getResponseCode();
            boolean authorized = "AUTHORIZED".equals(response.getStatus()) && responseCode == 0;
            pago.setStatus(authorized ? "AUTHORIZED" : "REJECTED");
            pago.setAuthorizationCode(response.getAuthorizationCode());
            pago.setResponseCode(responseCode);
            pago.setResultadoPublicado(false);
            pago = pagoRepository.save(pago);
        } catch (Exception e) {
            log.error("Error confirmando el pago de la orden {}", pago.getOrderId(), e);
            throw new IllegalStateException("No se pudo confirmar la transacción en Webpay.");
        }
        antifraudeService.reiniciarDespuesDeCallback(pago.getClientId());
        auditPublisher.publicar(pago.getOrderId(), pago.getClientId(), pago.getStatus(), pago.getAmount());
        publicarResultado(pago);
        boolean authorized = "AUTHORIZED".equals(pago.getStatus());
        return new PagoResultadoDTO(pago.getOrderId(), pago.getStatus(), pago.getAmount(),
                pago.getAuthorizationCode(), authorized ? "Pago autorizado." : "El pago fue rechazado.");
    }

    /** El usuario anuló en el formulario de Webpay (TBK_TOKEN + TBK_ORDEN_COMPRA). */
    public PagoResultadoDTO cancelar(String buyOrder) {
        Pago pago = pagoRepository.findByBuyOrder(buyOrder)
                .orElseThrow(() -> new IllegalArgumentException("No se encontró la orden de compra devuelta por Webpay."));
        if (!"AUTHORIZED".equals(pago.getStatus()) && !"CANCELLED".equals(pago.getStatus())) {
            pago.setStatus("CANCELLED");
            pago.setResultadoPublicado(false);
            pago = pagoRepository.save(pago);
            antifraudeService.reiniciarDespuesDeCallback(pago.getClientId());
            auditPublisher.publicar(pago.getOrderId(), pago.getClientId(), pago.getStatus(), pago.getAmount());
            publicarResultado(pago);
        }
        return new PagoResultadoDTO(pago.getOrderId(), pago.getStatus(), pago.getAmount(), null,
                "La transacción fue cancelada en Webpay.");
    }

    @Transactional(readOnly = true)
    public EstadoPagoDTO estadoPorOrden(Long orderId, String clientId, boolean esAdmin) {
        Pago pago = pagoRepository.findByOrderId(orderId)
                .orElseThrow(() -> new java.util.NoSuchElementException("La orden no tiene pagos registrados."));
        if (!esAdmin && !pago.getClientId().equals(clientId)) {
            throw new SecurityException("La orden no pertenece al usuario autenticado.");
        }
        return new EstadoPagoDTO(pago.getOrderId(), pago.getStatus(), pago.getAmount(),
                pago.getAuthorizationCode(), pago.getUpdatedAt());
    }

    /**
     * Publica el resultado en Kafka. Si Kafka no está disponible, el pago queda marcado como
     * pendiente de publicar y el job {@link #republicarResultadosPendientes()} lo reintenta.
     */
    private void publicarResultado(Pago pago) {
        try {
            kafkaPublisher.publicarResultado(toEvento(pago));
            pago.setResultadoPublicado(true);
            pagoRepository.save(pago);
        } catch (RuntimeException e) {
            log.warn("Resultado del pago de la orden {} quedó pendiente de publicar en Kafka: {}",
                    pago.getOrderId(), e.getMessage());
        }
    }

    @Scheduled(fixedDelayString = "${pagos.outbox.delay-ms:30000}", initialDelay = 30000)
    public void republicarResultadosPendientes() {
        List<Pago> pendientes = pagoRepository.findResultadosPendientesDePublicar(ESTADOS_FINALES);
        for (Pago pago : pendientes) {
            publicarResultado(pago);
        }
    }

    static PagoResultadoEvento toEvento(Pago pago) {
        List<PagoResultadoEvento.Item> items = pago.getItems().stream()
                .map(i -> new PagoResultadoEvento.Item(i.getProductoId(), i.getCantidad(), i.getPrecioUnitario()))
                .toList();
        // eventId determinista: si el evento se republica, los consumidores lo detectan como duplicado
        String eventId = "pago-" + pago.getBuyOrder() + "-" + pago.getStatus();
        return new PagoResultadoEvento(eventId, pago.getOrderId(), pago.getClientId(), pago.getStatus(),
                pago.getAmount(), pago.getAuthorizationCode(), items, Instant.now());
    }

    public String urlResultado(PagoResultadoDTO result) {
        return frontendUrl + "/pedidos?paymentStatus=" + result.status()
                + (result.orderId() == null ? "" : "&orderId=" + result.orderId());
    }
}
