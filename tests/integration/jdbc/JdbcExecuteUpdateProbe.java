// CHA-259 / CHA-333 — generic JDBC probe for cross-driver acceptance tests.
//
// Reads PENCA_SQL_PORT + PENCA_PROBE_SQL_STEPS (newline-separated SQL
// statements). Runs each step in order on a single Connection (so
// BEGIN/COMMIT/ROLLBACK threads through). Per step prints one line:
//
//   OK step=<n>: <rowsAffected>           (DDL / DML / SET / BEGIN / …)
//   OK_ROWS step=<n>: <json-array>        (SELECT — added in CHA-333)
//   CAUGHT step=<n>: <SQLException.getMessage()>
//
// One line per step lets the pytest wrapper parse with a simple regex
// and substring-assert against the expected wording (e.g. "CHA-172",
// "ADR 0010"). The probe always exits 0 unless connection setup fails;
// the pytest wrapper interprets per-step OK/OK_ROWS/CAUGHT, not the
// exit code.
//
// CHA-333: SELECT branch — when `stmt.execute(sql)` returns true the
// step produced a ResultSet. Iterate it via `ResultSet.getObject(i)`,
// box each row as a JSON object, and emit a single `OK_ROWS step=<n>:
// <json-array>` line. Hand-rolled JSON (no `javax.json` on the JDK 21
// default classpath) — covers String, Integer, Long, Boolean, Double,
// and null. If a new value type lands in the seeded data, extend the
// switch in `jsonValue` and cite the calling test.
//
// Run with Java 21 single-file mode:
//   java -cp tests/integration/jdbc/lib/flight-sql-jdbc-driver.jar \
//        tests/integration/jdbc/JdbcExecuteUpdateProbe.java

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcExecuteUpdateProbe {
    public static void main(String[] args) throws Exception {
        String port = System.getenv().getOrDefault("PENCA_SQL_PORT", "50060");
        String stepsRaw = System.getenv("PENCA_PROBE_SQL_STEPS");
        if (stepsRaw == null || stepsRaw.isEmpty()) {
            throw new IllegalStateException("PENCA_PROBE_SQL_STEPS must be set");
        }
        String[] steps = stepsRaw.split("\n");
        String url = "jdbc:arrow-flight-sql://localhost:" + port + "?useEncryption=false";
        System.out.println("URL: " + url);

        // CHA-333: pin the connection to a Penca catalog when the
        // caller provides one. The Apache flight-sql-jdbc-driver
        // forwards any Property whose name isn't a built-in option
        // (HOST/PORT/USER/CATALOG/…) as a Flight call header at
        // handshake (cf. `getHeaderAttributes` in the driver's
        // `ArrowFlightConnectionConfigImpl`). Using `x-penca-catalog`
        // directly mirrors ADBC's `call_header.x-penca-catalog`,
        // hitting Penca's CHA-253 handshake-time catalog pin. The
        // driver's own `catalog` property cannot be used because it
        // post-handshake-calls `SetSessionOptions`, which Penca
        // rejects with "catalog is fixed at handshake."
        Properties props = new Properties();
        String pinCatalog = System.getenv("PENCA_PROBE_CATALOG");
        if (pinCatalog != null && !pinCatalog.isEmpty()) {
            props.setProperty("x-penca-catalog", pinCatalog);
        }

        try (Connection conn = DriverManager.getConnection(url, props)) {
            for (int n = 0; n < steps.length; n++) {
                String sql = steps[n];
                System.out.println("--- step=" + n + " sql: " + sql);
                try (Statement stmt = conn.createStatement()) {
                    // execute() returns true if the result is a
                    // ResultSet, false if an update count. This is the
                    // surface every JDBC GUI (DataGrip / DBeaver) drives
                    // — going straight to executeQuery / executeUpdate
                    // would bypass the dispatch logic JDBC actually
                    // exercises.
                    boolean hasResultSet = stmt.execute(sql);
                    if (hasResultSet) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            String json = encodeResultSet(rs);
                            System.out.println("OK_ROWS step=" + n + ": " + json);
                        }
                    } else {
                        int updateCount = stmt.getUpdateCount();
                        System.out.println("OK step=" + n + ": " + updateCount);
                    }
                } catch (SQLException ex) {
                    String msg = String.valueOf(ex.getMessage())
                        .replace("\n", " ")
                        .replace("\r", " ");
                    System.out.println("CAUGHT step=" + n + ": " + msg);
                }
            }
        }
    }

    // Emit `[{col: val, ...}, ...]` on one line. Column labels from
    // `getColumnLabel` (the AS-alias if present, else the column name)
    // match what JDBC GUIs render — same source the Python helper's
    // `list[dict]` assertions key on.
    private static String encodeResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        String[] labels = new String[cols];
        for (int i = 0; i < cols; i++) {
            labels[i] = meta.getColumnLabel(i + 1);
        }
        StringBuilder sb = new StringBuilder("[");
        boolean firstRow = true;
        while (rs.next()) {
            if (!firstRow) {
                sb.append(",");
            }
            firstRow = false;
            sb.append("{");
            for (int i = 0; i < cols; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(jsonString(labels[i])).append(":");
                sb.append(jsonValue(rs.getObject(i + 1)));
            }
            sb.append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    private static String jsonValue(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof Boolean) {
            return ((Boolean) v) ? "true" : "false";
        }
        if (v instanceof Integer || v instanceof Long
            || v instanceof Short || v instanceof Byte) {
            return v.toString();
        }
        if (v instanceof Double || v instanceof Float) {
            return v.toString();
        }
        // CharSequence covers String + every JDBC variant returning a
        // text-ish value (CLOB-backed wrappers, etc.). Anything else
        // (Timestamp, BigDecimal, byte[]) falls through to toString()
        // — extend with a typed branch when a parametrize task in
        // P1-P9 actually needs it; cite the test in a comment.
        return jsonString(v.toString());
    }

    private static String jsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append("\"");
        return sb.toString();
    }
}
