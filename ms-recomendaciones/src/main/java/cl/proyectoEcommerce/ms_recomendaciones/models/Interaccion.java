package cl.proyectoEcommerce.ms_recomendaciones.models;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "interacciones",
        uniqueConstraints = @UniqueConstraint(name = "uk_interacciones_event_id", columnNames = "event_id"),
        indexes = {
                @Index(name = "idx_interacciones_usuario_fecha", columnList = "usuario_id, ocurrido_en"),
                @Index(name = "idx_interacciones_producto", columnList = "producto_id")
        })
public class Interaccion {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Idempotencia: un mismo evento Kafka re-entregado no se cuenta dos veces. */
    @Column(name = "event_id", nullable = false, length = 120)
    private String eventId;

    @Column(name = "usuario_id", nullable = false, length = 150)
    private String usuarioId;

    @Column(name = "producto_id", nullable = false)
    private Long productoId;

    @Column(length = 80)
    private String categoria;

    @Column(nullable = false, length = 20)
    private String accion;

    @Column(nullable = false)
    private int peso;

    @Column(length = 20)
    private String origen;

    @Column(name = "ocurrido_en", nullable = false)
    private Instant ocurridoEn;

    protected Interaccion() {}

    public Interaccion(String eventId, String usuarioId, Long productoId, String categoria, String accion,
                       int peso, String origen, Instant ocurridoEn) {
        this.eventId = eventId;
        this.usuarioId = usuarioId;
        this.productoId = productoId;
        this.categoria = categoria;
        this.accion = accion;
        this.peso = peso;
        this.origen = origen;
        this.ocurridoEn = ocurridoEn;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getUsuarioId() { return usuarioId; }
    public Long getProductoId() { return productoId; }
    public String getCategoria() { return categoria; }
    public String getAccion() { return accion; }
    public int getPeso() { return peso; }
    public String getOrigen() { return origen; }
    public Instant getOcurridoEn() { return ocurridoEn; }
}
