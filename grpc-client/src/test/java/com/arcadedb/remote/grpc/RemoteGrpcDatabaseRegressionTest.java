/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.test.BaseGraphServerTest;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.InsertSummary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests that exercise ONLY the gRPC Remote database client.
 * <p>
 * Requirements: - ArcadeDB server must be running with gRPC enabled. -
 * Connection parameters may be provided via env vars: ARCADE_DB,
 * ARCADE_GRPC_HOST, ARCADE_GRPC_PORT, ARCADE_HTTP_PORT, ARCADE_USER,
 * ARCADE_PASS
 * <p>
 * Defaults (match the sample bench): DB=ArcadeDB, gRPC=127.0.0.1:50059,
 * HTTP=127.0.0.1:2489, user=root, pass=root1234
 * <p>
 * These tests create an isolated vertex type "RG_Feedback" and clean up their
 * own data.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteGrpcDatabaseRegressionTest extends BaseGraphServerTest {

  // -------- Config (env overrides supported) --------

  // Test type & props
  static final String TYPE = "RG_Feedback";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeAll
  void ensureDatabaseExists() {

    grpcServer = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

  }

  @BeforeEach
  void open() {
    grpc = new RemoteGrpcDatabase(this.grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

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
      } catch (Throwable ignore) {
      }
      grpc.close();
    }
  }

  // ---------- Helpers ----------

  private InsertOptions defaultInsertOptions(final String targetClass, final List<String> keyCols, final List<String> updateCols) {
    return InsertOptions.newBuilder().setDatabase(getDatabaseName()).setTargetClass(targetClass).addAllKeyColumns(keyCols)
        .setConflictMode(ConflictMode.CONFLICT_UPDATE) // idempotent: upsert by keys
        .addAllUpdateColumnsOnConflict(updateCols) // LWW on these fields
        .setTransactionMode(TransactionMode.PER_BATCH).setServerBatchSize(256)
        .setCredentials(grpc.buildCredentials()) // package-private
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
        // System.out.println("Count all: r = " + r);
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

    assertThat(s1.getInserted()).as("first insert should insert 3").isEqualTo(3);
    assertThat(s1.getUpdated()).as("first insert should not update").isEqualTo(0);
    assertThat(countAll(TYPE)).as("row count after first insert").isEqualTo(3);

    // Re-insert with one changed record (r2) to force an update-on-conflict
    rows.set(1, row("r2", "beta-UPDATED", 22));

    grpc.begin();
    InsertSummary s2 = grpc.insertBulkAsListOfMaps(opts, rows, 60_000);
    grpc.commit();

    assertThat(s2.getInserted()).as("second insert should not insert new rows").isEqualTo(0);
    assertThat(s2.getUpdated() >= 1).as("should update at least 1 row on conflict").isTrue();
    assertThat(countAll(TYPE)).as("row count unchanged after upsert").isEqualTo(3);

    // Verify the updated record
    try (ResultSet rs = grpc.query("sql", "SELECT from `" + TYPE + "` WHERE id = :id", Map.of("id", "r2"))) {
      assertThat(rs.hasNext()).as("record r2 must exist").isTrue();
      Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("beta-UPDATED");
      assertThat(r.<Number>getProperty("n").intValue()).isEqualTo(22);
    }
  }

  @Test
  @DisplayName("Basic CRUD via SQL commands over gRPC")
  void basicCrudViaCommand() {
    // Create
    grpc.command("sql", "INSERT INTO `" + TYPE + "` set id = :id, name = :name, n = :n", Map.of("id", "x1", "name", "one", "n", 1));
    grpc.command("sql", "INSERT INTO `" + TYPE + "` set id = :id, name = :name, n = :n", Map.of("id", "x2", "name", "two", "n", 2));

    assertThat(countAll(TYPE)).as("two rows inserted").isEqualTo(2);

    // Read
    try (ResultSet rs = grpc.query("sql", "SELECT from `" + TYPE + "` WHERE id = :id", Map.of("id", "x1"))) {
      assertThat(rs.hasNext()).isTrue();
      Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("one");
    }

    // Update
    grpc.begin();
    grpc.command("sql", "UPDATE `" + TYPE + "` SET name = :name, n = :n WHERE id = :id",
        Map.of("name", "ONE!", "n", 11, "id", "x1"));
    grpc.commit();

    try (ResultSet rs = grpc.query("sql", "SELECT from `" + TYPE + "` WHERE id = :id", Map.of("id", "x1"))) {
      Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("ONE!");
      assertThat(r.<Number>getProperty("n").intValue()).isEqualTo(11);
    }

    // Delete
    grpc.command("sql", "DELETE FROM `" + TYPE + "` WHERE id = :id", Map.of("id", "x2"));
    assertThat(countAll(TYPE)).as("one row remains after delete").isEqualTo(1);
  }

  @Test
  @DisplayName("Transaction: rollback undoes changes; commit persists")
  void transactionsRollbackAndCommit() {
    long before = countAll(TYPE);

    // Rollback path
    grpc.begin();
    grpc.command("sql", "INSERT INTO `" + TYPE + "` set id = :id, name = :name, n = :n",
        Map.of("id", "tx1", "name", "temp", "n", 99));
    grpc.rollback();
    assertThat(countAll(TYPE)).as("rollback must revert insert").isEqualTo(before);

    // Commit path
    grpc.begin();
    grpc.command("sql", "INSERT INTO `" + TYPE + "` set id = :id, name = :name, n = :n",
        Map.of("id", "tx2", "name", "persisted", "n", 100));
    grpc.commit();
    assertThat(countAll(TYPE)).as("commit must persist insert").isEqualTo(before + 1);
  }

  @Test
  @DisplayName("schema:types → properties decoded as List<Map<..>>")
  void schemaTypesPropertiesDecodedAsMaps() {

    try (ResultSet rs = grpc.query("sql", "SELECT FROM schema:types WHERE name = :name", Map.of("name", TYPE))) {

      assertThat(rs.hasNext()).as("schema:types should contain our test type").isTrue();

      Result r = rs.next();

      Object propsObj = r.getProperty("properties");

      assertThat(propsObj).as("properties must be a List").isInstanceOf(List.class);

      List<?> props = (List<?>) propsObj;
      assertThat(props.isEmpty()).as("properties list should not be empty").isFalse();

      Object first = props.get(0);
      assertThat(first).as("each property is expected to be a Map").isInstanceOf(Map.class);

      Map<?, ?> p0 = (Map<?, ?>) first;

      // Spot-check expected keys
      assertThat(p0.containsKey("name")).as("property map must have 'name'").isTrue();
      assertThat(p0.containsKey("type")).as("property map must have 'type'").isTrue();
    }
  }

  @Test
  @DisplayName("Embedded _auditMetadata map round-trips with Long timestamps")
  void embeddedAuditMetadataRoundTrip() {

    final String recId = "audit1";
    final Map<String, Object> audit = new LinkedHashMap<>();

    audit.put("createdDate", 1720225210408L);
    audit.put("createdByUser", "service-account-empower-platform-admin");
    audit.put("lastModifiedDate", 1741795459718L);
    audit.put("lastModifiedByUser", "service-account-empower-platform-admin");

    grpc.command("sql", "INSERT INTO `" + TYPE + "` SET id = :id, name = :name, n = :n, _auditMetadata = :audit",
        Map.of("id", recId, "name", "with-audit", "n", 1, "audit", audit));

    try (ResultSet rs = grpc.query("sql", "SELECT FROM `" + TYPE + "` WHERE id = :id", Map.of("id", recId))) {
      assertThat(rs.hasNext()).as("inserted record should be queriable").isTrue();
      Result r = rs.next();
      Object auditObj = r.getProperty("_auditMetadata");
      assertThat(auditObj).as("_auditMetadata must be a Map").isInstanceOf(Map.class);
      @SuppressWarnings("unchecked")
      Map<String, Object> m = (Map<String, Object>) auditObj;

      Object cd = m.get("createdDate");
      Object md = m.get("lastModifiedDate");
      assertThat(cd).as("createdDate must be numeric").isInstanceOf(Number.class);
      assertThat(md).as("lastModifiedDate must be numeric").isInstanceOf(Number.class);
      assertThat(((Number) cd).longValue()).as("createdDate must be precise long").isEqualTo(1720225210408L);
      assertThat(((Number) md).longValue()).as("lastModifiedDate must be precise long").isEqualTo(1741795459718L);
      assertThat(m.get("createdByUser")).isEqualTo("service-account-empower-platform-admin");
      assertThat(m.get("lastModifiedByUser")).isEqualTo("service-account-empower-platform-admin");
    }
  }

  @Test
  @DisplayName("Issue #3524: newVertex().save() and newEdge() within a transaction should not throw 'Transaction not active'")
  void vertexSaveAndEdgeCreationWithinTransaction() {
    final String VERTEX_TYPE = "SVEx3524";
    final String EDGE_TYPE = "SVEx3524_Edge";

    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS", Map.of());
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.svex IF NOT EXISTS STRING", Map.of());
    grpc.command("sql", "CREATE EDGE TYPE `" + EDGE_TYPE + "` IF NOT EXISTS", Map.of());

    try {
      grpc.begin();

      MutableVertex svt1 = grpc.newVertex(VERTEX_TYPE);
      svt1.set("svex", "svt1");
      svt1.save();

      MutableVertex svt2 = grpc.newVertex(VERTEX_TYPE);
      svt2.set("svex", "svt2");
      svt2.save();

      // This should not throw 'Transaction not active'
      svt1.newEdge(EDGE_TYPE, svt2);

      // This save (updating the vertex after edge creation) should also work within the transaction
      svt1.save();

      grpc.commit();

      // Verify both vertices exist
      try (ResultSet rs = grpc.query("sql", "SELECT count(*) AS c FROM `" + VERTEX_TYPE + "`", Map.of())) {
        assertThat(rs.hasNext()).isTrue();
        Number count = rs.next().getProperty("c");
        assertThat(count.longValue()).as("both vertices should be persisted").isEqualTo(2L);
      }

      // Verify the edge exists
      try (ResultSet rs = grpc.query("sql",
          "SELECT count(*) AS c FROM `" + EDGE_TYPE + "`", Map.of())) {
        assertThat(rs.hasNext()).isTrue();
        Number count = rs.next().getProperty("c");
        assertThat(count.longValue()).as("edge should be persisted").isEqualTo(1L);
      }
    } finally {
      grpc.command("sql", "DELETE FROM `" + EDGE_TYPE + "`", Map.of());
      grpc.command("sql", "DELETE FROM `" + VERTEX_TYPE + "`", Map.of());
    }
  }

  @Test
  @DisplayName("Issue #3524: newVertex().save() within a transaction should persist only on commit and rollback on abort")
  void vertexSaveRollbackWithinTransaction() {
    final String VERTEX_TYPE = "SVEx3524Roll";

    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS", Map.of());
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.svex IF NOT EXISTS STRING", Map.of());

    try {
      // Verify rollback: creates vertices and then rolls back
      grpc.begin();

      MutableVertex svt1 = grpc.newVertex(VERTEX_TYPE);
      svt1.set("svex", "rollback1");
      svt1.save();

      grpc.rollback();

      try (ResultSet rs = grpc.query("sql", "SELECT count(*) AS c FROM `" + VERTEX_TYPE + "`", Map.of())) {
        Number count = rs.next().getProperty("c");
        assertThat(count.longValue()).as("vertex should NOT be persisted after rollback").isEqualTo(0L);
      }

      // Verify commit: creates a vertex and commits
      grpc.begin();

      MutableVertex svt2 = grpc.newVertex(VERTEX_TYPE);
      svt2.set("svex", "committed");
      svt2.save();

      grpc.commit();

      try (ResultSet rs = grpc.query("sql", "SELECT count(*) AS c FROM `" + VERTEX_TYPE + "`", Map.of())) {
        Number count = rs.next().getProperty("c");
        assertThat(count.longValue()).as("vertex should be persisted after commit").isEqualTo(1L);
      }
    } finally {
      grpc.command("sql", "DELETE FROM `" + VERTEX_TYPE + "`", Map.of());
    }
  }

  @Test
  @DisplayName("Issue #2854: gRPC ResultSet should preserve SQL aliases")
  void sqlAliasesArePreservedInGrpcResultSet() {
    // Setup: Insert test data with a known value
    grpc.command("sql", "INSERT INTO `" + TYPE + "` SET id = :id, name = :name, n = :n",
        Map.of("id", "alias-test-1", "name", "TestAuthor", "n", 42));

    // Test: Query with alias (AS clause)
    try (ResultSet rs = grpc.query("sql",
        "SELECT *, @rid, @type, name AS _aliasedName FROM `" + TYPE + "` WHERE id = :id",
        Map.of("id", "alias-test-1"))) {

      assertThat(rs.hasNext()).as("Query should return at least one result").isTrue();
      Result r = rs.next();

      // Verify original property is present
      assertThat((Object) r.getProperty("name"))
          .as("Original 'name' property should be present")
          .isEqualTo("TestAuthor");

      // Verify aliased property is present with the correct value
      assertThat((Object) r.getProperty("_aliasedName"))
          .as("Aliased property '_aliasedName' should be present and equal to original 'name'")
          .isEqualTo("TestAuthor");

      // Verify @rid is present (metadata attribute)
      assertThat((Object) r.getIdentity())
          .as("@rid property should be present")
          .isNotNull();

      // Verify @type is present (metadata attribute)
      assertThat(r.getElement().get().getTypeName())
          .as("@type property should be present")
          .isEqualTo(TYPE);

      // Verify the numeric property 'n' is also present
      assertThat(r.<Number>getProperty("n"))
          .as("Numeric property 'n' should be present")
          .isNotNull();
      assertThat(r.<Number>getProperty("n").intValue())
          .as("Numeric property 'n' should have correct value")
          .isEqualTo(42);
    }
  }
}
