package server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dao.AdminDAO;
import dao.InventarioDAO;
import dao.PedidosDAO;
import dao.RecetaDAO;
import dao.RecetaItem;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class PedidosServer {

    private final PedidosDAO    pedidosDAO = new PedidosDAO();
    private final RecetaDAO     recetaDAO  = new RecetaDAO();
    private final InventarioDAO invDAO     = new InventarioDAO();
    private final AdminDAO      adminDAO   = new AdminDAO();

    private final Object pedidoLock = new Object();

    private static final int PUERTO = System.getenv("PORT") != null
            ? Integer.parseInt(System.getenv("PORT")) : 8888;

    private HttpServer servidor;

    private final String adminUser;
    private final String adminPass;

    private static final long VENTANA_MS       = 10_000L;
    private static final int  MAX_PEDIDOS_HORA = 5;
    private static final long HORA_MS          = 60 * 60 * 1000L;
    private static final long BLOQUEO_MS       = 30 * 60 * 1000L;

    private final Map<String, Long>    ultimoPedidoPorIp = new ConcurrentHashMap<>();
    private final Map<String, Integer> contadorPorIp     = new ConcurrentHashMap<>();
    private final Map<String, Long>    bloqueadoHasta    = new ConcurrentHashMap<>();

    private static class ItemCarrito {
        String nombre;
        String categoria;
        int    cantidad;
    }

    public PedidosServer() throws IOException {

        Properties config = new Properties();
        try (java.io.FileInputStream fis = new java.io.FileInputStream("config.properties")) {
            config.load(fis);
        }
        adminUser = config.getProperty("admin.username", "admin");
        adminPass = config.getProperty("admin.password", "");

        servidor = HttpServer.create(new InetSocketAddress("0.0.0.0", PUERTO), 0);

        servidor.createContext("/api/pedidos", exchange -> {

            agregarCorsHeaders(exchange);

            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("POST".equals(exchange.getRequestMethod())) {

                String ip = obtenerIp(exchange);

                String errorThrottle = verificarThrottle(exchange);
                if (errorThrottle != null) {
                    registrarLog(exchange, ip, 429);
                    enviarRespuesta(exchange, 429, errorThrottle);
                    return;
                }

                String body = readBody(exchange);
                System.out.println("[PEDIDOS] Body recibido: " + body);

                synchronized (pedidoLock) {
                    try {
                        String cliente  = sanitizar(extraerValor(body, "cliente"));
                        String telefono = sanitizar(extraerValor(body, "telefono"));
                        String detalle  = sanitizar(extraerValor(body, "detalle"));
                        double total    = extraerDouble(body, "total");

                        String tipoPago = extraerValor(body, "tipoPago");
                        if ("-".equals(tipoPago) || tipoPago.isBlank()) tipoPago = "EFECTIVO";

                        if (esPedidoDuplicado(cliente, detalle)) {
                            System.out.println("[PEDIDOS] Duplicado detectado para: " + cliente);
                            registrarLog(exchange, ip, 200);
                            enviarRespuesta(exchange, 200,
                                    "{\"exito\":true,\"numero\":0,\"duplicado\":true}");
                            return;
                        }

                        String categorias = extraerCategoriasDeLosItems(body);
                        System.out.println("CATEGORIAS DETECTADAS: " + categorias);

                        String franja = calcularFranjaActual(detalle, categorias);
                        System.out.println("FRANJA CALCULADA: " + franja);

                        if ("FUERA HORARIO".equals(franja)) {
                            registrarLog(exchange, ip, 403);
                            enviarRespuesta(exchange, 403,
                                    "{\"exito\":false,\"error\":\"Pedido fuera de horario permitido\"}");
                            return;
                        }

                        String fechaEntrega = extraerValor(body, "fecha_entrega");
                        if ("-".equals(fechaEntrega) || fechaEntrega.isBlank()) fechaEntrega = null;

                        String categoriasDetalle = construirCategoriasDetalle(body);
                        System.out.println("[PEDIDOS] categoriasDetalle: " + categoriasDetalle);

                        int[] resultado = pedidosDAO.guardarPedidoAutoNumero(
                                cliente, telefono, detalle, total, franja, "WEB",
                                fechaEntrega, categoriasDetalle);

                        System.out.println("[PEDIDOS] Resultado: id=" + resultado[0] + " num=" + resultado[1]);

                        int id           = resultado[0];
                        int numeroPedido = resultado[1];

                        if (id > 0) descontarInventarioDesdeItems(body);

                        registrarLog(exchange, ip, 200);
                        enviarRespuesta(exchange, 200, "{"
                                + "\"exito\":true,"
                                + "\"id\":"     + id           + ","
                                + "\"numero\":" + numeroPedido + "}");

                    } catch (Exception e) {
                        e.printStackTrace();
                        registrarLog(exchange, ip, 400);
                        enviarRespuesta(exchange, 400, "{\"exito\":false}");
                    }
                }
            }
        });

        servidor.createContext("/api/pedidos/historico", exchange -> {
            agregarCorsHeaders(exchange);
            if ("GET".equals(exchange.getRequestMethod())) {
                List<PedidosDAO.PedidoBD> pedidos = pedidosDAO.cargarPedidosDeHoy();
                StringBuilder json = new StringBuilder("[");
                for (int i = 0; i < pedidos.size(); i++) {
                    PedidosDAO.PedidoBD p = pedidos.get(i);
                    json.append("{")
                            .append("\"id\":").append(p.id).append(",")
                            .append("\"numero\":").append(p.numero).append(",")
                            .append("\"numeroFormateado\":\"")
                            .append(PedidosDAO.formatearNumero(p.numero, p.origen)).append("\",")
                            .append("\"cliente\":\"").append(escaparJson(p.cliente)).append("\",")
                            .append("\"telefono\":\"").append(escaparJson(p.telefono)).append("\",")
                            .append("\"detalle\":\"").append(escaparJson(p.detalle)).append("\",")
                            .append("\"total\":").append(p.total).append(",")
                            .append("\"estado\":\"").append(p.estado).append("\",")
                            .append("\"franja\":\"").append(p.franja).append("\",")
                            .append("\"timestamp\":\"").append(obtenerHoraExacta()).append("\"")
                            .append("}");
                    if (i < pedidos.size() - 1) json.append(",");
                }
                json.append("]");
                enviarRespuesta(exchange, 200, json.toString());
            }
        });

        servidor.createContext("/api/stock", exchange -> {
            agregarCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(204, -1); return; }
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    enviarRespuesta(exchange, 200, StockDescontador.obtenerStockJSON());
                } catch (Exception e) {
                    e.printStackTrace();
                    enviarRespuesta(exchange, 500, "{}");
                }
            }
        });
        
        servidor.createContext("/api/usuarios", exchange -> {
            agregarCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(204, -1); return; }
            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    String body   = readBody(exchange);
                    String nombre = sanitizar(extraerValor(body, "nombre"));
                    String email  = sanitizar(extraerValor(body, "email"));
                    String ip     = obtenerIp(exchange);
                    System.out.println("Usuario OAuth: " + nombre + " / " + email);
                    adminDAO.registrarOActualizarUsuario(email, nombre, ip);
                    enviarRespuesta(exchange, 200, "{\"exito\":true}");
                } catch (Exception e) {
                    e.printStackTrace();
                    enviarRespuesta(exchange, 400, "{\"exito\":false}");
                }
            }
        });

        servidor.createContext("/api/admin/stats", exchange -> {
            agregarCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(204, -1); return; }
            if (!autenticarAdmin(exchange)) return;
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    enviarRespuesta(exchange, 200, mapToJson(adminDAO.obtenerEstadisticas()));
                } catch (Exception e) {
                    e.printStackTrace();
                    enviarRespuesta(exchange, 500, "{}");
                }
            }
        });

        servidor.createContext("/api/admin/logs", exchange -> {
            agregarCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(204, -1); return; }
            if (!autenticarAdmin(exchange)) return;
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    String query  = exchange.getRequestURI().getQuery();
                    int    limite = 200;
                    if (query != null && query.contains("limite=")) {
                        try { limite = Integer.parseInt(query.split("limite=")[1].split("&")[0]); }
                        catch (NumberFormatException ignored) {}
                    }
                    enviarRespuesta(exchange, 200, listaToJson(adminDAO.obtenerLogs(limite)));
                } catch (Exception e) {
                    e.printStackTrace();
                    enviarRespuesta(exchange, 500, "[]");
                }
            }
        });

        servidor.createContext("/api/admin/ips", exchange -> {
            agregarCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(204, -1); return; }
            if (!autenticarAdmin(exchange)) return;

            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    List<Map<String, Object>> bloqueadas = adminDAO.obtenerIPsBloqueadas();
                    List<Map<String, Object>> topIPs     = adminDAO.obtenerTopIPs(20);

                    long ahora = System.currentTimeMillis();
                    for (Map.Entry<String, Long> e : bloqueadoHasta.entrySet()) {
                        if (ahora < e.getValue()) {
                            boolean yaEsta = bloqueadas.stream()
                                    .anyMatch(m -> e.getKey().equals(m.get("ip")));
                            if (!yaEsta) {
                                Map<String, Object> m = new LinkedHashMap<>();
                                m.put("ip",              e.getKey());
                                m.put("razon",           "Throttling automático (en memoria)");
                                m.put("reincidencias",   contadorPorIp.getOrDefault(e.getKey(), 0));
                                m.put("permanente",      false);
                                m.put("desbloqueada",    false);
                                m.put("bloqueada_hasta", new java.util.Date(e.getValue()).toString());
                                m.put("fecha_bloqueo",   "—");
                                bloqueadas.add(m);
                            }
                        }
                    }

                    enviarRespuesta(exchange, 200,
                            "{\"bloqueadas\":" + listaToJson(bloqueadas)
                          + ",\"top_ips\":"    + listaToJson(topIPs) + "}");
                } catch (Exception e) {
                    e.printStackTrace();
                    enviarRespuesta(exchange, 500, "{\"bloqueadas\":[],\"top_ips\":[]}");
                }
            }

            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    String body   = readBody(exchange);
                    String ip     = extraerValor(body, "ip");
                    String accion = extraerValor(body, "accion");
                    String razon  = extraerValor(body, "razon");

                    if ("bloquear".equals(accion)) {
                        adminDAO.bloquearIPManual(ip, razon);
                        bloqueadoHasta.put(ip, System.currentTimeMillis() + 24 * 60 * 60 * 1000L);
                        System.out.println("[ADMIN] IP bloqueada manualmente: " + ip);
                    } else if ("desbloquear".equals(accion)) {
                        adminDAO.desbloquearIP(ip);
                        bloqueadoHasta.remove(ip);
                        contadorPorIp.remove(ip);
                        ultimoPedidoPorIp.remove(ip);
                        System.out.println("[ADMIN] IP desbloqueada: " + ip);
                    }
                    enviarRespuesta(exchange, 200, "{\"exito\":true}");
                } catch (Exception e) {
                    e.printStackTrace();
                    enviarRespuesta(exchange, 400, "{\"exito\":false}");
                }
            }
        });

        servidor.createContext("/api/admin/usuarios", exchange -> {
            agregarCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(204, -1); return; }
            if (!autenticarAdmin(exchange)) return;
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    enviarRespuesta(exchange, 200, listaToJson(adminDAO.obtenerUsuarios(500)));
                } catch (Exception e) {
                    e.printStackTrace();
                    enviarRespuesta(exchange, 500, "[]");
                }
            }
        });

        servidor.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(10));
        System.out.println("Servidor OK puerto " + PUERTO);
    }

    private boolean autenticarAdmin(HttpExchange exchange) throws IOException {
        String auth = exchange.getRequestHeaders().getFirst("Authorization");
        if (auth == null || !auth.startsWith("Basic ")) {
            exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"Admin\"");
            enviarRespuesta(exchange, 401, "{\"error\":\"No autorizado\"}");
            return false;
        }
        try {
            String decoded = new String(
                    java.util.Base64.getDecoder().decode(auth.substring(6)),
                    StandardCharsets.UTF_8);
            String[] partes = decoded.split(":", 2);
            if (partes.length == 2 && partes[0].equals(adminUser) && partes[1].equals(adminPass)) {
                return true;
            }
        } catch (Exception ignored) {}
        enviarRespuesta(exchange, 401, "{\"error\":\"Credenciales incorrectas\"}");
        return false;
    }

    private String obtenerIp(HttpExchange exchange) {
        String forwarded = exchange.getRequestHeaders().getFirst("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) return forwarded.split(",")[0].trim();
        return exchange.getRemoteAddress().getAddress().getHostAddress();
    }

    private String verificarThrottle(HttpExchange exchange) {
        String ip  = obtenerIp(exchange);
        long ahora = System.currentTimeMillis();

        Long bloqueado = bloqueadoHasta.get(ip);
        if (bloqueado != null && ahora < bloqueado) {
            long min = (bloqueado - ahora) / 60_000 + 1;
            System.out.println("[THROTTLE] IP bloqueada rechazada: " + ip + " (" + min + " min)");
            return "{\"exito\":false,\"error\":\"Demasiados intentos. Reintenta en " + min + " minutos.\"}";
        } else if (bloqueado != null) {
            bloqueadoHasta.remove(ip);
            contadorPorIp.remove(ip);
            ultimoPedidoPorIp.remove(ip);
        }

        Long ultimo = ultimoPedidoPorIp.get(ip);
        if (ultimo != null && (ahora - ultimo) < VENTANA_MS) {
            long segs = (VENTANA_MS - (ahora - ultimo)) / 1000 + 1;
            System.out.println("[THROTTLE] Ventana activa: " + ip + " (" + segs + "s)");
            return "{\"exito\":false,\"error\":\"Espera " + segs + " segundos antes de otro pedido.\"}";
        }

        int contador = contadorPorIp.getOrDefault(ip, 0);
        if (ultimo != null && (ahora - ultimo) >= HORA_MS) {
            contador = 0;
            contadorPorIp.put(ip, 0);
        }

        if (contador >= MAX_PEDIDOS_HORA) {
            bloqueadoHasta.put(ip, ahora + BLOQUEO_MS);
            adminDAO.bloquearIPTemporal(ip,
                    "Throttling automático: superó " + MAX_PEDIDOS_HORA + " pedidos/hora",
                    LocalDateTime.now().plusMinutes(30));
            System.out.println("[THROTTLE] IP bloqueada 30min: " + ip);
            return "{\"exito\":false,\"error\":\"Demasiados pedidos. IP bloqueada 30 minutos.\"}";
        }

        ultimoPedidoPorIp.put(ip, ahora);
        contadorPorIp.put(ip, contador + 1);
        System.out.println("[THROTTLE] " + ip + " → pedido #" + (contador + 1) + " en la hora");
        return null;
    }

    private void registrarLog(HttpExchange exchange, String ip, int statusCode) {
        try {
            adminDAO.registrarLog(
                    ip,
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    statusCode,
                    exchange.getRequestHeaders().getFirst("User-Agent"),
                    null);
        } catch (Exception e) {
            System.err.println("[LOG] Error: " + e.getMessage());
        }
    }

    private boolean esPedidoDuplicado(String cliente, String detalle) {
        String sql = "SELECT COUNT(*) FROM pedidos "
                + "WHERE cliente = ? AND detalle = ? "
                + "AND fecha_hora > NOW() - INTERVAL '15 seconds' "
                + "AND origen = 'WEB'";
        Connection conn = null;
        try {
            conn = dao.Conexion.conectar();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, cliente);
                ps.setString(2, detalle);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() && rs.getInt(1) > 0;
                }
            }
        } catch (Exception e) {
            System.out.println("[PEDIDOS] Error verificando duplicado: " + e.getMessage());
            return false;
        } finally {
            if (conn != null) dao.Conexion.devolver(conn);
        }
    }

    private String construirCategoriasDetalle(String json) {
        List<ItemCarrito> items = extraerItems(json);
        Map<String, Integer> conteo = new LinkedHashMap<>();
        for (ItemCarrito item : items) {
            String cat = normalizarCategoria(item.categoria);
            conteo.merge(cat, item.cantidad, Integer::sum);
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> e : conteo.entrySet()) {
            if (sb.length() > 0) sb.append(",");
            sb.append(e.getKey()).append(":").append(e.getValue());
        }
        return sb.toString();
    }

    private void descontarInventarioDesdeItems(String json) {
        List<ItemCarrito> items = extraerItems(json);
        for (ItemCarrito item : items) {
            try {
                String cat = normalizarCategoria(item.categoria);
                List<RecetaItem> receta;
                if (cat.equals("rapido") || cat.isEmpty()) {
                    receta = recetaDAO.obtenerPorNombre(item.nombre);
                } else {
                    receta = recetaDAO.obtenerPorProducto(0, cat);
                }
                for (RecetaItem r : receta) {
                    double gramosTotal = r.getCantidadG() * item.cantidad;
                    invDAO.descontarStock(r.getIdIngrediente(), gramosTotal);
                    System.out.println("[INV] Descontado " + gramosTotal + "g de ingrediente "
                            + r.getIdIngrediente() + " por " + item.cantidad + "x " + item.nombre);
                }
            } catch (Exception e) {
                System.out.println("[INV] Error descontando " + item.nombre + ": " + e.getMessage());
            }
        }
    }

    private String normalizarCategoria(String cat) {
        if (cat == null) return "rapido";
        cat = cat.toLowerCase().trim();
        if (cat.endsWith("s") && !cat.equals("rapido")) cat = cat.substring(0, cat.length() - 1);
        cat = cat.replace("á","a").replace("é","e").replace("í","i")
                 .replace("ó","o").replace("ú","u").replace("ñ","n");
        return cat.isEmpty() ? "rapido" : cat;
    }

    private List<ItemCarrito> extraerItems(String json) {
        List<ItemCarrito> lista = new ArrayList<>();
        try {
            int inicio = json.indexOf("\"items\":");
            if (inicio == -1) return lista;
            inicio = json.indexOf("[", inicio);
            int fin = json.indexOf("]", inicio);
            if (inicio == -1 || fin == -1) return lista;
            String itemsStr = json.substring(inicio + 1, fin);
            int i = 0;
            while ((i = itemsStr.indexOf("{", i)) != -1) {
                int cierre = itemsStr.indexOf("}", i);
                if (cierre == -1) break;
                String obj = itemsStr.substring(i, cierre + 1);
                ItemCarrito item = new ItemCarrito();
                item.nombre    = extraerValor(obj, "nombre");
                item.categoria = extraerValor(obj, "categoria");
                item.cantidad  = (int) extraerDouble(obj, "cantidad");
                if (item.nombre != null && !item.nombre.equals("-") && item.cantidad > 0) {
                    lista.add(item);
                }
                i = cierre + 1;
            }
        } catch (Exception e) {
            System.out.println("[INV] Error parseando items: " + e.getMessage());
        }
        return lista;
    }

    // ── Utilidades JSON ───────────────────────────────────────────────────────

    private String mapToJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(e.getKey()).append("\":");
            Object v = e.getValue();
            if (v instanceof Number || v instanceof Boolean) sb.append(v);
            else if (v == null) sb.append("null");
            else sb.append("\"").append(escaparJson(v.toString())).append("\"");
            first = false;
        }
        return sb.append("}").toString();
    }

    private String listaToJson(List<Map<String, Object>> lista) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < lista.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(mapToJson(lista.get(i)));
        }
        return sb.append("]").toString();
    }

    private String readBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private String extraerValor(String json, String clave) {
        String patron = "\"" + clave + "\":\"";
        int i = json.indexOf(patron);
        if (i == -1) return "-";
        i += patron.length();
        int f = json.indexOf("\"", i);
        return f == -1 ? "-" : json.substring(i, f);
    }

    private double extraerDouble(String json, String clave) {
        try {
            String patron = "\"" + clave + "\":";
            int i = json.indexOf(patron);
            if (i == -1) return 0;
            i += patron.length();
            int f = json.indexOf(",", i);
            if (f == -1) f = json.indexOf("}", i);
            return Double.parseDouble(json.substring(i, f).trim());
        } catch (Exception e) { return 0; }
    }

    private String extraerCategoriasDeLosItems(String json) {
        StringBuilder cats = new StringBuilder();
        String patron = "\"categoria\":\"";
        int i = 0;
        while ((i = json.indexOf(patron, i)) != -1) {
            i += patron.length();
            int f = json.indexOf("\"", i);
            if (f != -1) cats.append(json, i, f).append(" ");
        }
        return cats.toString().toLowerCase();
    }

    private String calcularFranjaActual(String detalle, String categorias) {
        java.time.LocalTime ahora = java.time.LocalTime.now(java.time.ZoneId.of("America/Santiago"));
        int hora   = ahora.getHour();
        int minuto = ahora.getMinute();
        String d = detalle    != null ? detalle.toLowerCase()    : "";
        String c = categorias != null ? categorias.toLowerCase() : "";
        boolean esPanaderia  = c.contains("panaderia") || c.contains("panadería")
                            || d.contains("panaderia") || d.contains("panadería")
                            || d.contains("hallula")   || d.contains("marraqueta")
                            || d.contains("dobladita") || d.contains("pan amasado")
                            || d.contains("pan ");
        boolean esAnticipado = c.contains("pasteler") || c.contains("reposteri")
                            || d.contains("pasteler") || d.contains("reposteri");
        if (esPanaderia) {
            if (hora < 12 || hora >= 18) return "FUERA HORARIO";
        } else if (esAnticipado) {
            if (hora < 12 || hora >= 22) return "FUERA HORARIO";
        } else {
            if (hora < 18 || hora >= 22) return "FUERA HORARIO";
        }
        int inicioHora, inicioMin, finHora, finMin;
        if (minuto < 30) { inicioHora = hora; inicioMin = 0;  finHora = hora;     finMin = 30; }
        else             { inicioHora = hora; inicioMin = 30; finHora = hora + 1; finMin = 0;  }
        return String.format("%02d:%02d - %02d:%02d", inicioHora, inicioMin, finHora, finMin);
    }

    private String escaparJson(String t) {
        return t == null ? "" : t.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private void agregarCorsHeaders(HttpExchange e) {
        e.getResponseHeaders().set("Access-Control-Allow-Origin",  "*");
        e.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        e.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
    }

    private void enviarRespuesta(HttpExchange ex, int code, String r) throws IOException {
        byte[] b = r.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    private String obtenerHoraExacta() {
        return java.time.LocalTime.now(java.time.ZoneId.of("America/Santiago"))
                .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm"));
    }

    private String sanitizar(String v) {
        return v == null ? "-" : v.replaceAll("[<>\"']", "").trim();
    }

    public void iniciar() { servidor.start(); }

    public static void main(String[] args) throws IOException {
        new PedidosServer().iniciar();
    }
}
