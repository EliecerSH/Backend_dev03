package cl.proyectoEcommerce.ms_pagos.services;

import cl.proyectoEcommerce.ms_pagos.dtos.IntentoPagoDTO;
import cl.proyectoEcommerce.ms_pagos.entities.EstadoRiesgoCliente;
import cl.proyectoEcommerce.ms_pagos.events.AlertaFraudeEvento;
import cl.proyectoEcommerce.ms_pagos.repositories.EstadoRiesgoRepository;
import cl.proyectoEcommerce.ms_pagos.repositories.IntentoPagoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AntifraudeServiceTest {

    private EstadoRiesgoRepository riskRepository;
    private IntentoPagoRepository attemptRepository;
    private PagoKafkaPublisher publisher;
    private AntifraudeService service;
    private EstadoRiesgoCliente risk;
    private final Instant inicio = Instant.now().minus(1, ChronoUnit.MINUTES);

    @BeforeEach
    void setUp() {
        riskRepository = mock(EstadoRiesgoRepository.class);
        attemptRepository = mock(IntentoPagoRepository.class);
        publisher = mock(PagoKafkaPublisher.class);
        service = new AntifraudeService(riskRepository, attemptRepository, publisher, 3, 10, 30);
        risk = new EstadoRiesgoCliente("cliente-1", inicio);
        when(riskRepository.lockByClientId("cliente-1")).thenReturn(Optional.of(risk));
        when(attemptRepository.existsByTransactionId(anyString())).thenReturn(false);
    }

    private IntentoPagoDTO intento(String tx) {
        return new IntentoPagoDTO("cliente-1", 10000L, tx, Instant.now(), 99L);
    }

    @Test
    void reglaVelocityCheck() {
        assertFalse(AntifraudeService.superaUmbral(2, 3));
        assertTrue(AntifraudeService.superaUmbral(3, 3));
        assertTrue(AntifraudeService.superaUmbral(5, 3));
    }

    @Test
    void dosIntentosEnVentanaNoBloquean() {
        when(attemptRepository.countByClientIdAndCreatedAtAfterAndCreatedAtLessThanEqual(eq("cliente-1"), any(), any()))
                .thenReturn(2L);
        service.procesarIntento(intento("TX-2"));
        assertFalse(risk.isBlocked());
        verify(publisher, never()).publicarAlerta(any());
    }

    @Test
    void tercerIntentoEnVentanaBloqueaYPublicaAlerta() {
        when(attemptRepository.countByClientIdAndCreatedAtAfterAndCreatedAtLessThanEqual(eq("cliente-1"), any(), any()))
                .thenReturn(3L);
        service.procesarIntento(intento("TX-3"));

        assertTrue(risk.isBlocked());
        assertTrue(risk.getBlockedUntil().isAfter(Instant.now().plus(29, ChronoUnit.MINUTES)));
        ArgumentCaptor<AlertaFraudeEvento> alerta = ArgumentCaptor.forClass(AlertaFraudeEvento.class);
        verify(publisher).publicarAlerta(alerta.capture());
        assertEquals("BLOQUEO_APLICADO", alerta.getValue().tipo());
        assertEquals(3L, alerta.getValue().intentosEnVentana());
    }

    @Test
    void eventoDuplicadoNoSeCuentaDosVeces() {
        when(attemptRepository.existsByTransactionId("TX-dup")).thenReturn(true);
        service.procesarIntento(intento("TX-dup"));
        verify(attemptRepository, never()).save(any());
    }

    @Test
    void bloqueoVencidoSeLimpia() {
        risk.blockUntil(Instant.now().minusSeconds(1), inicio);
        when(riskRepository.findById("cliente-1")).thenReturn(Optional.of(risk));
        assertNull(service.bloqueoActivoHasta("cliente-1"));
        assertFalse(risk.isBlocked());
    }
}
