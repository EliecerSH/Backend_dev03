package cl.proyectoEcommerce.ms_pagos.controllers;

import cl.proyectoEcommerce.ms_pagos.dtos.EstadoPagoDTO;
import cl.proyectoEcommerce.ms_pagos.dtos.IniciarPagoRequest;
import cl.proyectoEcommerce.ms_pagos.dtos.IniciarPagoResponse;
import cl.proyectoEcommerce.ms_pagos.dtos.PagoResultadoDTO;
import cl.proyectoEcommerce.ms_pagos.services.PagoService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

@Controller
@RequestMapping("/api/pagos")
public class PagosController {
    private final PagoService pagoService;
    public PagosController(PagoService pagoService) { this.pagoService = pagoService; }

    @ResponseBody
    @PostMapping("/iniciar")
    public ResponseEntity<IniciarPagoResponse> iniciar(@Valid @RequestBody IniciarPagoRequest request,
            JwtAuthenticationToken authentication) {
        String bearer = "Bearer " + authentication.getToken().getTokenValue();
        IniciarPagoResponse response = pagoService.iniciar(request.orderId(), clientId(authentication), bearer);
        return ResponseEntity.ok(response);
    }

    /** Estado del pago de una orden (dueño de la orden o Admin). */
    @ResponseBody
    @GetMapping("/orden/{orderId}")
    public ResponseEntity<EstadoPagoDTO> estado(@PathVariable Long orderId, JwtAuthenticationToken authentication) {
        boolean admin = authentication.getAuthorities().stream()
                .anyMatch(a -> "ROLE_Admin".equals(a.getAuthority()));
        return ResponseEntity.ok(pagoService.estadoPorOrden(orderId, clientId(authentication), admin));
    }

    private static String clientId(JwtAuthenticationToken authentication) {
        String oid = authentication.getToken().getClaimAsString("oid");
        return (oid == null || oid.isBlank()) ? authentication.getToken().getSubject() : oid;
    }

    @RequestMapping(value = "/retorno", method = {RequestMethod.GET, RequestMethod.POST})
    public RedirectView retorno(@RequestParam(name = "token_ws", required = false) String tokenWs,
            @RequestParam(name = "TBK_TOKEN", required = false) String tbkToken,
            @RequestParam(name = "TBK_ORDEN_COMPRA", required = false) String tbkBuyOrder) {
        PagoResultadoDTO resultado;
        try {
            if (tbkToken != null && !tbkToken.isBlank()) {
                resultado = pagoService.cancelar(tbkBuyOrder);
            } else if (tokenWs != null && !tokenWs.isBlank()) {
                resultado = pagoService.confirmar(tokenWs);
            } else if (tbkBuyOrder != null && !tbkBuyOrder.isBlank()) {
                // Timeout del formulario Webpay: llega TBK_ORDEN_COMPRA sin token -> se trata como anulación
                resultado = pagoService.cancelar(tbkBuyOrder);
            } else {
                resultado = new PagoResultadoDTO(null, "ERROR", null, null,
                        "Webpay no devolvió un token de transacción.");
            }
        } catch (RuntimeException ex) {
            resultado = new PagoResultadoDTO(null, "ERROR", null, null,
                    "No se pudo confirmar el resultado del pago. Contacta al comercio antes de volver a pagar.");
        }
        RedirectView redirect = new RedirectView(pagoService.urlResultado(resultado));
        redirect.setStatusCode(org.springframework.http.HttpStatus.SEE_OTHER);
        return redirect;
    }
}
