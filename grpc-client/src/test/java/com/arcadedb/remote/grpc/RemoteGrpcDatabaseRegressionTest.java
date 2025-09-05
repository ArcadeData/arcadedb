package com.arcadedb.remote.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.InsertSummary;

/**
 * Regression tests that exercise ONLY the gRPC Remote database client.
 *
 * Requirements: - ArcadeDB server must be running with gRPC enabled. -
 * Connection parameters may be provided via env vars: ARCADE_DB,
 * ARCADE_GRPC_HOST, ARCADE_GRPC_PORT, ARCADE_HTTP_PORT, ARCADE_USER,
 * ARCADE_PASS
 *
 * Defaults (match the sample bench): DB=ArcadeDB, gRPC=127.0.0.1:50059,
 * HTTP=127.0.0.1:2489, user=root, pass=root1234
 *
 * These tests create an isolated vertex type "RG_Feedback" and clean up their
 * own data.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RemoteGrpcDatabaseRegressionTest {

	// -------- Config (env overrides supported) --------

	static final String DB_NAME = System.getenv().getOrDefault("ARCADE_DB", "test");
	static final String GRPC_HOST = System.getenv().getOrDefault("ARCADE_GRPC_HOST", "127.0.0.1");
	static final int GRPC_PORT = Integer.parseInt(System.getenv().getOrDefault("ARCADE_GRPC_PORT", "50051"));
	static final String HTTP_HOST = System.getenv().getOrDefault("ARCADE_HTTP_HOST", "127.0.0.1");
	static final int HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("ARCADE_HTTP_PORT", "2480"));
	static final String USER = System.getenv().getOrDefault("ARCADE_USER", "root");
	static final String PASS = System.getenv().getOrDefault("ARCADE_PASS", "oY9uU2uJ8nD8iY7t");

	// Test type & props
	static final String TYPE = "RG_Feedback";

	private RemoteGrpcDatabase grpc;

	@BeforeAll
	void ensureDatabaseExists() {
		// Prefer using the gRPC admin helper if available (same package).
		try (RemoteGrpcServer admin = new RemoteGrpcServer(GRPC_HOST, GRPC_PORT, USER, PASS)) {
			if (!admin.existsDatabase(DB_NAME)) {
				admin.createDatabase(DB_NAME);
			}
		}
		catch (Throwable t) {
			// If admin tooling isn't on the classpath, tests may still pass provided DB
			// exists.
			System.err.println("[WARN] Could not verify/create DB via RemoteGrpcServer: " + t.getMessage());
		}
	}

	@BeforeEach
	void open() {
		grpc = new RemoteGrpcDatabase(GRPC_HOST, GRPC_PORT, HTTP_PORT, DB_NAME, USER, PASS);
		// Create isolated schema for these tests (id unique, name string, n integer)
		grpc.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS", Map.of());
		grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.id IF NOT EXISTS STRING", Map.of());
		grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.name IF NOT EXISTS STRING", Map.of());
		grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.n IF NOT EXISTS INTEGER", Map.of());
		grpc.command("sql", "CREATE INDEX IF NOT EXISTS ON " + TYPE + " (id) UNIQUE", Map.of());
		// Ensure clean slate for each test method
		grpc.command("sql", "DELETE FROM `" + TYPE + "`", Map.of());
	}

	@AfterEach
	void close() {
		if (grpc != null) {
			try {
				grpc.rollback();
			}
			catch (Throwable ignore) {
			}
			grpc.close();
		}
	}

	// ---------- Helpers ----------

	private InsertOptions defaultInsertOptions(final String targetClass, final List<String> keyCols, final List<String> updateCols) {
		return InsertOptions.newBuilder().setDatabase(DB_NAME).setTargetClass(targetClass).addAllKeyColumns(keyCols)
				.setConflictMode(ConflictMode.CONFLICT_UPDATE) // idempotent: upsert by keys
				.addAllUpdateColumnsOnConflict(updateCols) // LWW on these fields
				.setTransactionMode(TransactionMode.PER_BATCH).setServerBatchSize(256).setCredentials(grpc.buildCredentials()) // package-private
																																// in same
																																// package
				.build();
	}

	private long countAll(String type) {
		
		String sql = "SELECT count(*) AS c FROM " + type;
		
		try (ResultSet rs = grpc.query("sql", sql, Map.of())) {
			long c = 0;
			while (rs.hasNext()) {
				Result r = rs.next();
				//System.out.println("Count all: r = " + r);
				Number n = r.getProperty("c");
				c = (n == null) ? 0 : n.longValue();
			}
			return c;
		}
	}

	private Map<String, Object> row(String id, String name, int n) {
		Map<String, Object> m = new HashMap<>();
		m.put("id", id);
		m.put("name", name);
		m.put("n", n);
		return m;
	}

	// ---------- Tests ----------

	@Test
	@DisplayName("Bulk insert via gRPC is idempotent by key and supports updates on conflict")
	void bulkInsertIdempotentAndUpdate() {
		// Prepare rows
		List<Map<String, Object>> rows = new ArrayList<>();
		rows.add(row("r1", "alpha", 1));
		rows.add(row("r2", "beta", 2));
		rows.add(row("r3", "gamma", 3));

		grpc.begin();
		InsertOptions opts = defaultInsertOptions(TYPE, List.of("id"), Arrays.asList("name", "n"));
		InsertSummary s1 = grpc.insertBulkAsListOfMaps(opts, rows, 60_000);
		grpc.commit();

		assertEquals(3, s1.getInserted(), "first insert should insert 3");
		assertEquals(0, s1.getUpdated(), "first insert should not update");
		assertEquals(3, countAll(TYPE), "row count after first insert");

		// Re-insert with one changed record (r2) to force an update-on-conflict
		rows.set(1, row("r2", "beta-UPDATED", 22));

		grpc.begin();
		InsertSummary s2 = grpc.insertBulkAsListOfMaps(opts, rows, 60_000);
		grpc.commit();

		assertEquals(0, s2.getInserted(), "second insert should not insert new rows");
		assertTrue(s2.getUpdated() >= 1, "should update at least 1 row on conflict");
		assertEquals(3, countAll(TYPE), "row count unchanged after upsert");

		// Verify the updated record
		try (ResultSet rs = grpc.query("sql", "SELECT from `" + TYPE + "` WHERE id = :id", Map.of("id", "r2"))) {
			assertTrue(rs.hasNext(), "record r2 must exist");
			Result r = rs.next();
			assertEquals("beta-UPDATED", r.<String>getProperty("name"));
			assertEquals(22, r.<Number>getProperty("n").intValue());
		}
	}

	@Test
	@DisplayName("Basic CRUD via SQL commands over gRPC")
	void basicCrudViaCommand() {
		// Create
		grpc.command("sql", "INSERT INTO `" + TYPE + "` set id = :id, name = :name, n = :n", Map.of("id", "x1", "name", "one", "n", 1));
		grpc.command("sql", "INSERT INTO `" + TYPE + "` set id = :id, name = :name, n = :n", Map.of("id", "x2", "name", "two", "n", 2));

		assertEquals(2, countAll(TYPE), "two rows inserted");

		// Read
		try (ResultSet rs = grpc.query("sql", "SELECT from `" + TYPE + "` WHERE id = :id", Map.of("id", "x1"))) {
			assertTrue(rs.hasNext());
			Result r = rs.next();
			assertEquals("one", r.<String>getProperty("name"));
		}

		// Update
		grpc.begin();
		grpc.command("sql", "UPDATE `" + TYPE + "` SET name = :name, n = :n WHERE id = :id", Map.of("name", "ONE!", "n", 11, "id", "x1"));
		grpc.commit();
		
		try (ResultSet rs = grpc.query("sql", "SELECT from `" + TYPE + "` WHERE id = :id", Map.of("id", "x1"))) {
			Result r = rs.next();
			assertEquals("ONE!", r.<String>getProperty("name"));
			assertEquals(11, r.<Number>getProperty("n").intValue());
		}

		// Delete
		grpc.command("sql", "DELETE FROM `" + TYPE + "` WHERE id = :id", Map.of("id", "x2"));		
		assertEquals(1, countAll(TYPE), "one row remains after delete");
	}

	@Test
	@DisplayName("Transaction: rollback undoes changes; commit persists")
	void transactionsRollbackAndCommit() {
		long before = countAll(TYPE);

		// Rollback path
		grpc.begin();
		grpc.command("sql", "INSERT INTO `" + TYPE + "` set id = :id, name = :name, n = :n", Map.of("id", "tx1", "name", "temp", "n", 99));
		grpc.rollback();
		assertEquals(before, countAll(TYPE), "rollback must revert insert");

		// Commit path
		grpc.begin();
		grpc.command("sql", "INSERT INTO `" + TYPE + "` set id = :id, name = :name, n = :n", Map.of("id", "tx2", "name", "persisted", "n", 100));
		grpc.commit();
		assertEquals(before + 1, countAll(TYPE), "commit must persist insert");
	}
}
