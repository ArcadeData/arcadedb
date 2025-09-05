package com.arcadedb.remote.grpc;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.arcadedb.remote.RemoteDatabase; // HTTP client
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ArcadeDbHTTPTvsGRPCBench {

	// ---- Config (edit or pass via env/args) ----
	static String DB_NAME = System.getenv().getOrDefault("ARCADE_DB", "ArcadeDB");
	static String HTTP_HOST = System.getenv().getOrDefault("ARCADE_HTTP_HOST", "127.0.0.1");
	static int HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("ARCADE_HTTP_PORT", "2489"));
	static String GRPC_HOST = System.getenv().getOrDefault("ARCADE_GRPC_HOST", "127.0.0.1");
	static int GRPC_PORT = Integer.parseInt(System.getenv().getOrDefault("ARCADE_GRPC_PORT", "50059"));

	static String USER = System.getenv().getOrDefault("ARCADE_USER", "root");
	static String PASS = System.getenv().getOrDefault("ARCADE_PASS", "root1234");
	
	static ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		// override via args if provided: host/ports/db/user/pass
		for (String a : args) {
			if (a.startsWith("--db="))
				DB_NAME = a.substring(5);
			else if (a.startsWith("--http=")) {
				var hp = a.substring(7).split(":");
				HTTP_HOST = hp[0];
				HTTP_PORT = Integer.parseInt(hp[1]);
			}
			else if (a.startsWith("--grpc=")) {
				var hp = a.substring(7).split(":");
				GRPC_HOST = hp[0];
				GRPC_PORT = Integer.parseInt(hp[1]);
			}
			else if (a.startsWith("--user="))
				USER = a.substring(7);
			else if (a.startsWith("--pass="))
				PASS = a.substring(7);
		}

		System.out.printf("Benchmarking DB='%s' HTTP=%s:%d gRPC=%s:%d as %s%n", DB_NAME, HTTP_HOST, HTTP_PORT, GRPC_HOST, GRPC_PORT, USER);

		try (RemoteGrpcServer grpcServer = new RemoteGrpcServer(GRPC_HOST, GRPC_PORT, USER, PASS)) {

			if (grpcServer.existsDatabase(DB_NAME)) {

				System.out.printf("Database %s exists%n", DB_NAME);
			}
			else {

				System.out.printf("Creating database %s%n", DB_NAME);

				grpcServer.createDatabase(DB_NAME);
			}
		}

		// ---------- Open both clients ----------
		try (RemoteDatabase http = new RemoteDatabase(HTTP_HOST, HTTP_PORT, DB_NAME, USER, PASS);
				RemoteGrpcDatabase grpc = new RemoteGrpcDatabase(GRPC_HOST, GRPC_PORT, HTTP_PORT, DB_NAME, USER, PASS)) {

			// ensure schema aligned
			prepareSchemaHTTP(http);
			prepareSchemaGRPC(grpc);

			// --- apples-to-apples data set (clear prior run for repeatability) ---
			cleanupHTTP(http);
			cleanupGRPC(grpc);

			// ---------- BULK INSERT (100 rows) ----------
			List<Map<String, Object>> rows = buildFeedbackRows(100);
			
			String jsonContent = objectMapper.writeValueAsString(rows);
			
			System.out.printf("JSON:%n%s%n", jsonContent);
			
			time("HTTP bulkInsert(100)", () -> {
				
				http.begin();

				http.command("sqlscript", "INSERT INTO UserFeedback CONTENT " + jsonContent);
				
				http.commit();
			});

			time("gRPC bulkInsert(100)", () -> {
				grpc.begin();
				InsertOptions opts = defaultInsertOptions(grpc, "UserFeedback", List.of("id")); // id unique -> idempotent
				InsertSummary s = grpc.insertBulkAsListOfMaps(opts, rows, 120_000);
				grpc.commit();
				System.out.printf("  gRPC summary: received=%d, inserted=%d, updated=%d, ignored=%d, failed=%d, errors=%d%n", s.getReceived(),
						s.getInserted(), s.getUpdated(), s.getIgnored(), s.getFailed(), s.getErrorsCount());
			});

			// ---------- Single insert ----------
			Map<String, Object> one = uf("UF-SINGLE", "tenantZ", "UI", "FEATURE", "Single insert via API", null, Instant.now());
			time("HTTP insert(1)", () -> {
				http.begin();
				http.command("sqlscript", "INSERT INTO UserFeedback CONTENT :m", Map.of("m", one));
				http.commit();
			});
			time("gRPC insert(1)", () -> {
				grpc.begin();
				InsertOptions opts = defaultInsertOptions(grpc, "UserFeedback", List.of("id"));
				grpc.insertBulkAsListOfMaps(opts, List.of(one), 60_000);
				grpc.commit();
			});

			// ---------- Update ----------
			time("HTTP update by id", () -> {
				http.begin();
				http.command("sqlscript", "UPDATE UserFeedback SET feedback = 'UPDATED_HTTP' WHERE id = 'UF-0001'");
				http.commit();
			});
			time("gRPC update by id", () -> {
				grpc.begin();
				grpc.command("sql", "UPDATE UserFeedback SET feedback = 'UPDATED_GRPC' WHERE id = 'UF-0001'", Map.of(), false, 0,
						/* tx */ null, 60_000);
				grpc.commit();
			});

			// ---------- Point read by index ----------
			time("HTTP point read (id)", () -> {
				var rs = http.query("sql", "SELECT FROM UserFeedback WHERE id = 'UF-0001'");
				int count = 0;
				while (rs.hasNext()) {
					rs.next();
					count++;
				}
				System.out.println("  HTTP rows=" + count);
			});
			time("gRPC point read (id)", () -> {
				var it = grpc.queryStream(DB_NAME, "SELECT FROM UserFeedback WHERE id = 'UF-0001'", Map.of(), 50,
						StreamQueryRequest.RetrievalMode.CURSOR, /* tx */ null, 60_000);
				int count = 0;
				while (it.hasNext()) {
					it.next();
					count++;
				}
				System.out.println("  gRPC rows=" + count);
			});

			// ---------- Streaming query (3 modes) ----------
			runStreamBench(grpc, "SELECT FROM UserFeedback", 200, StreamQueryRequest.RetrievalMode.CURSOR);
			runStreamBench(grpc, "SELECT FROM UserFeedback", 200, StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL);
			runStreamBench(grpc, "SELECT FROM UserFeedback", 200, StreamQueryRequest.RetrievalMode.PAGED);

			// ---------- Delete subset ----------
			time("HTTP delete subset", () -> {
				http.begin();
				http.command("sql", "DELETE FROM UserFeedback WHERE id LIKE 'UF-00%'");
				http.commit();
			});
			time("gRPC delete subset", () -> {
				grpc.begin();
				grpc.executeCommand("sql", "DELETE FROM UserFeedback WHERE id LIKE 'UF-00%'", Map.of(), false, 0, null, 60_000);
				grpc.commit();
			});

		} // try-with-resources
	}

	// ------------------------------------------------------
	// Schema prep - HTTP
	// ------------------------------------------------------
	private static void prepareSchemaHTTP(RemoteDatabase http) {
		// Create vertex type
		http.command("sql", "CREATE VERTEX TYPE UserFeedback IF NOT EXISTS");

		// Create properties
		http.command("sqlscript", "CREATE PROPERTY UserFeedback.id IF NOT EXISTS STRING");
		http.command("sqlscript", "CREATE PROPERTY UserFeedback.applicationArea IF NOT EXISTS STRING");
		http.command("sqlscript", "CREATE PROPERTY UserFeedback.empowerTenantId IF NOT EXISTS STRING");
		http.command("sqlscript", "CREATE PROPERTY UserFeedback.empowerType IF NOT EXISTS STRING");
		http.command("sqlscript", "CREATE PROPERTY UserFeedback.feedback IF NOT EXISTS STRING");
		http.command("sqlscript", "CREATE PROPERTY UserFeedback.timestamp IF NOT EXISTS DATETIME");
		http.command("sqlscript", "CREATE PROPERTY UserFeedback.image IF NOT EXISTS EMBEDDED");
		
		// Create index
		http.command("sqlscript", "CREATE INDEX IF NOT EXISTS ON UserFeedback (id) UNIQUE");
	}

	// ------------------------------------------------------
	// Schema prep - gRPC
	// ------------------------------------------------------
	private static void prepareSchemaGRPC(RemoteGrpcDatabase grpc) {
		
		// Create vertex type
		grpc.executeCommand("sqlscript", "CREATE VERTEX TYPE UserFeedback IF NOT EXISTS", Map.of(), false, 0, null, 60_000);

		// Create properties
		grpc.executeCommand("sqlscript", "CREATE PROPERTY UserFeedback.id IF NOT EXISTS STRING", Map.of(), false, 0, null, 60_000);
		grpc.executeCommand("sqlscript", "CREATE PROPERTY UserFeedback.applicationArea IF NOT EXISTS STRING", Map.of(), false, 0, null, 60_000);
		grpc.executeCommand("sqlscript", "CREATE PROPERTY UserFeedback.empowerTenantId IF NOT EXISTS STRING", Map.of(), false, 0, null, 60_000);
		grpc.executeCommand("sqlscript", "CREATE PROPERTY UserFeedback.empowerType IF NOT EXISTS STRING", Map.of(), false, 0, null, 60_000);
		grpc.executeCommand("sqlscript", "CREATE PROPERTY UserFeedback.feedback IF NOT EXISTS STRING", Map.of(), false, 0, null, 60_000);
		grpc.executeCommand("sqlscript", "CREATE PROPERTY UserFeedback.timestamp IF NOT EXISTS DATETIME", Map.of(), false, 0, null, 60_000);
		grpc.executeCommand("sqlscript", "CREATE PROPERTY UserFeedback.image IF NOT EXISTS EMBEDDED", Map.of(),  false, 0, null, 60_000);
		
		// Create index
		grpc.executeCommand("sqlscript", "CREATE INDEX IF NOT EXISTS ON UserFeedback (id) UNIQUE", Map.of(), false, 0, null, 60_000);
	}

	private static void cleanupHTTP(RemoteDatabase http) {
		http.command("sql", "DELETE FROM UserFeedback");
	}

	private static void cleanupGRPC(RemoteGrpcDatabase grpc) {
		grpc.executeCommand("sql", "DELETE FROM UserFeedback", Map.of(), false, 0, null, 60_000);
	}

	// ------------------------------------------------------
	// Workloads
	// ------------------------------------------------------

	private static void runStreamBench(RemoteGrpcDatabase grpc, String sql, int batchSize, StreamQueryRequest.RetrievalMode mode) {
		time("gRPC stream " + mode + " (batchSize=" + batchSize + ")", () -> {
			
			var it = grpc.queryStream(DB_NAME, sql, Map.of(), batchSize, mode, /* tx */ null, 120_000);
			var total = new AtomicInteger(0);
			while (it.hasNext()) {
				it.next();
				total.incrementAndGet();
			}
			
			System.out.println("  rows=" + total.get());
		});
	}

	// ------------------------------------------------------
	// Data builders
	// ------------------------------------------------------

	private static List<Map<String, Object>> buildFeedbackRows(int n) throws Exception {
		// optional: shared embedded image payload
		byte[] png = loadDefaultPNG();
		String b64 = (png != null) ? Base64.getEncoder().encodeToString(png) : null;
		Map<String, Object> embeddedImage = (b64 != null) ? Map.of("mime", "image/png", "data", b64) : null;

		List<Map<String, Object>> out = new ArrayList<>(n);
		for (int i = 1; i <= n; i++) {
			out.add(uf(String.format("UF-%04d", i), "tenantA", "UI", "USER_FEEDBACK", "Bulk feedback #" + i, embeddedImage, Instant.now()));
		}
		return out;
	}

	private static Map<String, Object> uf(String id, String tenant, String area, String type, String feedback, Map<String, Object> image,
			Instant ts) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("id", id);
		m.put("empowerTenantId", tenant);
		m.put("applicationArea", area);
		m.put("empowerType", type);
		m.put("feedback", feedback);
		if (image != null)
			m.put("image", image); // EMBEDDED map (server coerces to embedded doc)
		m.put("timestamp", Date.from(ts));
		return m;
	}

	private static byte[] loadDefaultPNG() {
		try {
			File f = new File("./data/Curonix/Logos/default.png");
			if (!f.exists())
				return null;
			return Files.readAllBytes(f.toPath());
		}
		catch (Exception ignore) {
			return null;
		}
	}

	private static InsertOptions defaultInsertOptions(RemoteGrpcDatabase grpc, String targetClass, List<String> keyCols) {
		// Minimal options: target class + key columns for idempotency; update
		// last-write-wins
		return InsertOptions.newBuilder().setDatabase(DB_NAME).setTargetClass(targetClass).addAllKeyColumns(keyCols)
				.setConflictMode(ConflictMode.CONFLICT_UPDATE)
				.addAllUpdateColumnsOnConflict(Arrays.asList("feedback", "image", "timestamp", "applicationArea", "empowerType"))
				.setTransactionMode(TransactionMode.PER_BATCH).setServerBatchSize(500)
				.setCredentials(grpc.buildCredentials()) // if your server validates per-request creds
				.build();
	}

	// ------------------------------------------------------
	// Timing helper
	// ------------------------------------------------------

	@FunctionalInterface
	interface ThrowingRunnable {
		void run() throws Exception;
	}

	private static void time(String label, ThrowingRunnable r) {
		long t0 = System.currentTimeMillis();
		try {
			r.run();
			long ms = System.currentTimeMillis() - t0;
			System.out.printf("%-35s %8d ms%n", label + ":", ms);
		}
		catch (Exception e) {
			long ms = System.currentTimeMillis() - t0;
			System.out.printf("%-35s %8d ms  (ERROR: %s)%n", label + ":", ms, e.getMessage());
			throw new RuntimeException(e);
		}
	}
}