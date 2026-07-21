// CHA-249 — the JDBC GUI compatibility smoke that pyarrow-based tests
// can't substitute for. Drives Apache's flight-sql-jdbc-driver (the
// same JAR DataGrip / DBeaver / every JetBrains DB tool ships with)
// through the exact path a JDBC GUI walks on first connect:
// `getDatabaseProductName()` (which loads the `SqlInfo` cache via
// `CommandGetSqlInfo` — the call that was UNIMPLEMENTED pre-CHA-249),
// `getDatabaseProductVersion()`, and three SELECTs from the ticket's
// acceptance criteria.
//
// Run with Java 21 single-file mode (no separate compile step):
//   java -cp tests/integration/jdbc/lib/flight-sql-jdbc-driver.jar \
//        tests/integration/jdbc/JdbcProbe.java
//
// The pytest wrapper in TestFlightSqlJdbcProbe sets PENCA_SQL_PORT
// and ensures `public.public.users` exists. Exit code 0 + stdout
// "OK — all three queries succeeded." is the wire signal pytest greps.

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class JdbcProbe {
    public static void main(String[] args) throws Exception {
        String port = System.getenv().getOrDefault("PENCA_SQL_PORT", "50060");
        String url = "jdbc:arrow-flight-sql://localhost:" + port + "?useEncryption=false";
        System.out.println("URL: " + url);

        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData md = conn.getMetaData();
            String product = md.getDatabaseProductName();
            String version = md.getDatabaseProductVersion();
            System.out.println("DatabaseMetaData.getDatabaseProductName() = " + product);
            System.out.println("DatabaseMetaData.getDatabaseProductVersion() = " + version);
            if (!"penca".equals(product)) {
                throw new AssertionError("expected product=penca, got " + product);
            }

            run(conn, "SELECT 1");
            run(conn, "SELECT * FROM users");
            run(conn, "SELECT * FROM public.public.users");
        }

        System.out.println("OK — all three queries succeeded.");
    }

    private static void run(Connection conn, String sql) throws Exception {
        System.out.println();
        System.out.println("--- " + sql);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            StringBuilder header = new StringBuilder();
            for (int i = 1; i <= cols; i++) {
                if (i > 1) header.append(" | ");
                header.append(md.getColumnLabel(i));
            }
            System.out.println(header);
            int n = 0;
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) row.append(" | ");
                    row.append(rs.getString(i));
                }
                System.out.println(row);
                n++;
            }
            System.out.println("(" + n + " row" + (n == 1 ? "" : "s") + ")");
        }
    }
}
