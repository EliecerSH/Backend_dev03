package cl.proyectoEcommerce.ms_recomendaciones.engine;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Motor de recomendaciones simple y explicable (sin dependencias de Spring para poder probarlo aislado).
 *
 * Tres estrategias:
 * <ol>
 *   <li><b>Personalizada</b> (usuario con historial): mezcla afinidad por categoría con decaimiento temporal,
 *       interés directo en el producto (lo vio / lo agregó al carro y no lo compró) y popularidad global.</li>
 *   <li><b>Populares</b> (usuario nuevo o anónimo): suma de pesos de interacciones recientes de todos los usuarios.</li>
 *   <li><b>Similares</b> (ficha de producto): co-ocurrencia "usuarios que interactuaron con X también con Y",
 *       completando con productos de la misma categoría.</li>
 * </ol>
 * Pesos de interacción: VISTA = 1, CARRITO = 3, COMPRA = 5.
 */
public final class MotorRecomendaciones {

    public static final double PESO_AFINIDAD_CATEGORIA = 0.55;
    public static final double PESO_INTERES_PRODUCTO = 0.25;
    public static final double PESO_POPULARIDAD = 0.20;
    /** Una interacción pierde la mitad de su peso cada 7 días. */
    public static final double VIDA_MEDIA_DIAS = 7.0;

    public record Producto(Long id, String nombre, String categoria, BigDecimal precio, Integer stock,
                           String imagenUrl) {
        boolean disponible() {
            return stock == null || stock > 0;
        }
    }

    public record Interaccion(Long productoId, String categoria, String accion, int peso, Instant ocurridoEn) {}

    public record Recomendacion(Producto producto, double score, String motivo) {}

    public enum Accion {
        VISTA(1), CARRITO(3), COMPRA(5);

        private final int peso;

        Accion(int peso) {
            this.peso = peso;
        }

        public int peso() {
            return peso;
        }

        /** Acepta los nombres en español y los alias en inglés del ejemplo CloudNative (view/cart/purchase). */
        public static Accion desde(String valor) {
            if (valor == null) throw new IllegalArgumentException("La acción es obligatoria.");
            return switch (valor.trim().toUpperCase()) {
                case "VISTA", "VIEW", "VER" -> VISTA;
                case "CARRITO", "CART", "AGREGAR_CARRITO" -> CARRITO;
                case "COMPRA", "PURCHASE", "COMPRAR" -> COMPRA;
                default -> throw new IllegalArgumentException(
                        "Acción no soportada: " + valor + ". Use VISTA, CARRITO o COMPRA.");
            };
        }
    }

    private MotorRecomendaciones() {}

    static double decaimiento(Instant ocurridoEn, Instant ahora) {
        double dias = Math.max(0, Duration.between(ocurridoEn, ahora).toMinutes()) / (60.0 * 24.0);
        return Math.pow(0.5, dias / VIDA_MEDIA_DIAS);
    }

    // ------------------------------------------------------------------ personalizadas

    public static List<Recomendacion> personalizadas(List<Interaccion> historial, Map<Long, Double> popularidad,
            Collection<Producto> catalogo, Instant ahora, int limite) {
        if (historial == null || historial.isEmpty()) {
            return populares(popularidad, catalogo, Set.of(), limite);
        }

        Map<String, Double> afinidadCategoria = new HashMap<>();
        Map<Long, Double> interesProducto = new HashMap<>();
        Set<Long> comprados = new HashSet<>();
        Set<Long> enCarrito = new HashSet<>();

        for (Interaccion i : historial) {
            double w = i.peso() * decaimiento(i.ocurridoEn(), ahora);
            if (i.categoria() != null) afinidadCategoria.merge(normalizar(i.categoria()), w, Double::sum);
            if (i.productoId() == null) continue;
            interesProducto.merge(i.productoId(), w, Double::sum);
            if ("COMPRA".equalsIgnoreCase(i.accion())) comprados.add(i.productoId());
            if ("CARRITO".equalsIgnoreCase(i.accion())) enCarrito.add(i.productoId());
        }

        double maxAfinidad = max(afinidadCategoria.values());
        double maxInteres = max(interesProducto.values());
        double maxPop = max(popularidad.values());

        List<Recomendacion> resultado = new ArrayList<>();
        for (Producto p : catalogo) {
            if (!p.disponible() || comprados.contains(p.id())) continue; // no recomendar lo ya comprado
            double afin = ratio(afinidadCategoria.getOrDefault(normalizar(p.categoria()), 0.0), maxAfinidad);
            double interes = ratio(interesProducto.getOrDefault(p.id(), 0.0), maxInteres);
            double pop = ratio(popularidad.getOrDefault(p.id(), 0.0), maxPop);
            double score = PESO_AFINIDAD_CATEGORIA * afin + PESO_INTERES_PRODUCTO * interes + PESO_POPULARIDAD * pop;

            String motivo;
            if (enCarrito.contains(p.id())) motivo = "Lo agregaste a tu carrito";
            else if (interes > 0) motivo = "Lo viste recientemente";
            else if (afin > 0) motivo = "Porque te interesa " + p.categoria();
            else motivo = "Popular en la tienda";
            resultado.add(new Recomendacion(p, redondear(score), motivo));
        }
        return top(resultado, limite);
    }

    // ------------------------------------------------------------------ populares

    public static List<Recomendacion> populares(Map<Long, Double> popularidad, Collection<Producto> catalogo,
            Set<Long> excluir, int limite) {
        double maxPop = max(popularidad.values());
        List<Recomendacion> resultado = new ArrayList<>();
        for (Producto p : catalogo) {
            if (!p.disponible() || excluir.contains(p.id())) continue;
            double pop = ratio(popularidad.getOrDefault(p.id(), 0.0), maxPop);
            resultado.add(new Recomendacion(p, redondear(pop), pop > 0 ? "Popular en la tienda" : "Destacado del catálogo"));
        }
        return top(resultado, limite);
    }

    // ------------------------------------------------------------------ similares

    /**
     * @param coocurrencia peso acumulado de otros productos entre usuarios que interactuaron con el producto base
     */
    public static List<Recomendacion> similares(Long productoBaseId, Map<Long, Double> coocurrencia,
            Collection<Producto> catalogo, int limite) {
        Producto base = catalogo.stream().filter(p -> p.id().equals(productoBaseId)).findFirst().orElse(null);
        String categoriaBase = base == null ? null : normalizar(base.categoria());
        double maxCo = max(coocurrencia.values());

        List<Recomendacion> resultado = new ArrayList<>();
        for (Producto p : catalogo) {
            if (p.id().equals(productoBaseId) || !p.disponible()) continue;
            double co = ratio(coocurrencia.getOrDefault(p.id(), 0.0), maxCo);
            boolean mismaCategoria = categoriaBase != null && categoriaBase.equals(normalizar(p.categoria()));
            if (co == 0 && !mismaCategoria) continue;
            double score = 0.7 * co + (mismaCategoria ? 0.3 : 0.0);
            String motivo = co > 0 ? "Otros clientes también lo vieron"
                    : "De la misma categoría";
            resultado.add(new Recomendacion(p, redondear(score), motivo));
        }
        return top(resultado, limite);
    }

    // ------------------------------------------------------------------ helpers

    private static List<Recomendacion> top(List<Recomendacion> lista, int limite) {
        // desempate estable: score desc, luego id asc (resultados reproducibles)
        lista.sort(Comparator.comparingDouble(Recomendacion::score).reversed()
                .thenComparing(r -> r.producto().id()));
        Map<Long, Recomendacion> unicos = new LinkedHashMap<>();
        for (Recomendacion r : lista) unicos.putIfAbsent(r.producto().id(), r);
        return unicos.values().stream().limit(Math.max(1, limite)).toList();
    }

    private static double max(Collection<Double> valores) {
        return valores.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
    }

    private static double ratio(double valor, double maximo) {
        return maximo <= 0 ? 0.0 : valor / maximo;
    }

    private static double redondear(double v) {
        return Math.round(v * 10_000.0) / 10_000.0;
    }

    private static String normalizar(String categoria) {
        return categoria == null ? "" : categoria.trim().toLowerCase();
    }
}
