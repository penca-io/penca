// CHA-333 — JDBC PreparedStatement parameter-binding probe.
//
// Drives a single prepared statement through the JDBC surface that GUI
// clients walk for parameterized writes:
//   conn.prepareStatement(sql) → setLong/setInt/setString/… → executeUpdate()
//
// Exercises the wire sequence `ActionCreatePreparedStatement` →
// `DoPutPreparedStatementQuery(params)` → `DoPutPreparedStatementUpdate(handle)`,
// where the params batch is stashed into the handle and later decoded
// server-side via `super::codec::decode_param_values` (the same
// decoder the read path uses).
//
// Reads:
//   PENCA_SQL_PORT          (default 50060)
//   PENCA_PROBE_PREPARED_SQL    one SQL string with `?` placeholders
//   PENCA_PROBE_PREPARED_PARAMS JSON array
//                                e.g. `[{"type":"string","value":"carol"},
//                                        {"type":"int","value":99}]`
//                                Bindings are 1-indexed by array position.
//   PENCA_PROBE_CATALOG     (optional) Penca catalog to pin at handshake
//                            (forwarded as `x-penca-catalog` header).
//
// Emits one line:
//   OK rows=<n>      executeUpdate returned `n`
//   CAUGHT: <msg>    SQLException — message newlines flattened
//
// Run with Java 21 single-file mode:
//   java --add-opens=java.base/java.nio=ALL-UNNAMED \
//        -cp tests/integration/jdbc/lib/flight-sql-jdbc-driver.jar \
//        tests/integration/jdbc/JdbcPreparedStatementProbe.java

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

public class JdbcPreparedStatementProbe {
    public static void main(String[] args) throws Exception {
        String port = System.getenv().getOrDefault("PENCA_SQL_PORT", "50060");
        String sql = System.getenv("PENCA_PROBE_PREPARED_SQL");
        if (sql == null || sql.isEmpty()) {
            throw new IllegalStateException("PENCA_PROBE_PREPARED_SQL must be set");
        }
        String paramsJson = System.getenv().getOrDefault("PENCA_PROBE_PREPARED_PARAMS", "[]");
        Param[] params = parseParams(paramsJson);

        String url = "jdbc:arrow-flight-sql://localhost:" + port + "?useEncryption=false";
        System.out.println("URL: " + url);
        System.out.println("SQL: " + sql);

        Properties props = new Properties();
        String pinCatalog = System.getenv("PENCA_PROBE_CATALOG");
        if (pinCatalog != null && !pinCatalog.isEmpty()) {
            // See JdbcExecuteUpdateProbe for the rationale on bare
            // `x-penca-catalog` vs the driver's `catalog` Property.
            props.setProperty("x-penca-catalog", pinCatalog);
        }

        try (Connection conn = DriverManager.getConnection(url, props);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                int idx = i + 1;  // JDBC is 1-indexed
                Param p = params[i];
                switch (p.type) {
                    case "string":
                        if (p.value == null) {
                            stmt.setNull(idx, Types.VARCHAR);
                        } else {
                            stmt.setString(idx, p.value);
                        }
                        break;
                    case "int":
                        if (p.value == null) {
                            stmt.setNull(idx, Types.INTEGER);
                        } else {
                            stmt.setInt(idx, Integer.parseInt(p.value));
                        }
                        break;
                    case "long":
                        if (p.value == null) {
                            stmt.setNull(idx, Types.BIGINT);
                        } else {
                            stmt.setLong(idx, Long.parseLong(p.value));
                        }
                        break;
                    default:
                        throw new IllegalStateException(
                            "unsupported param type: " + p.type
                            + " (extend JdbcPreparedStatementProbe.java when needed)"
                        );
                }
            }
            try {
                int rows = stmt.executeUpdate();
                System.out.println("OK rows=" + rows);
            } catch (SQLException ex) {
                String msg = String.valueOf(ex.getMessage())
                    .replace("\n", " ")
                    .replace("\r", " ");
                System.out.println("CAUGHT: " + msg);
            }
        }
    }

    private static final class Param {
        final String type;
        final String value;  // null encodes JSON `null`; numeric types parse from the string
        Param(String type, String value) { this.type = type; this.value = value; }
    }

    // Minimal JSON-array-of-objects parser. Hand-rolled because pulling
    // a JSON dep into the single-file probe defeats the point.
    // Accepts: `[{"type":"<t>","value":<v>},...]` where <v> is a
    // JSON string, number, or null. Whitespace tolerated.
    private static Param[] parseParams(String json) {
        int i = skipWs(json, 0);
        if (i >= json.length() || json.charAt(i) != '[') {
            throw new IllegalStateException("PENCA_PROBE_PREPARED_PARAMS must start with '['");
        }
        i++;
        java.util.List<Param> out = new java.util.ArrayList<>();
        i = skipWs(json, i);
        if (i < json.length() && json.charAt(i) == ']') {
            return new Param[0];
        }
        while (i < json.length()) {
            i = skipWs(json, i);
            if (json.charAt(i) != '{') {
                throw new IllegalStateException("expected '{' at offset " + i);
            }
            i++;
            String type = null;
            String value = null;
            while (i < json.length()) {
                i = skipWs(json, i);
                if (json.charAt(i) == '}') { i++; break; }
                String key = readString(json, i);
                i += key.length() + 2;  // +2 for the surrounding quotes
                i = skipWs(json, i);
                if (json.charAt(i) != ':') {
                    throw new IllegalStateException("expected ':' at offset " + i);
                }
                i++;
                i = skipWs(json, i);
                if (json.charAt(i) == '"') {
                    String s = readString(json, i);
                    i += s.length() + 2;
                    if (key.equals("type")) type = s;
                    else if (key.equals("value")) value = s;
                } else if (json.startsWith("null", i)) {
                    i += 4;
                    if (key.equals("value")) value = null;
                } else {
                    // Number — read until comma/whitespace/'}' and
                    // hand the digit run to the type-specific parser
                    // (setInt/setLong) verbatim.
                    int start = i;
                    while (i < json.length()
                        && "0123456789.+-eE".indexOf(json.charAt(i)) >= 0) {
                        i++;
                    }
                    String num = json.substring(start, i);
                    if (key.equals("value")) value = num;
                }
                i = skipWs(json, i);
                if (i < json.length() && json.charAt(i) == ',') i++;
            }
            if (type == null) {
                throw new IllegalStateException("param missing `type` field");
            }
            out.add(new Param(type, value));
            i = skipWs(json, i);
            if (i < json.length() && json.charAt(i) == ',') {
                i++;
                continue;
            }
            if (i < json.length() && json.charAt(i) == ']') break;
        }
        return out.toArray(new Param[0]);
    }

    private static int skipWs(String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        return i;
    }

    // Reads a `"..."` JSON string starting at `start` (must point at
    // the opening quote). Returns the unquoted content; caller
    // advances by `content.length() + 2`.
    private static String readString(String s, int start) {
        if (s.charAt(start) != '"') {
            throw new IllegalStateException("expected '\"' at offset " + start);
        }
        StringBuilder sb = new StringBuilder();
        int i = start + 1;
        while (i < s.length() && s.charAt(i) != '"') {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(i + 1);
                switch (n) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    default: sb.append(n);
                }
                i += 2;
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }
}
