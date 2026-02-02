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
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for PostgreSQL wire protocol covering executor logic.
 * These tests focus on protocol-level behaviors not covered by basic JDBC tests.
 */
class PostgresProtocolIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin,GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
    super.endTest();
  }

  @Override
  protected String getDatabaseName() {
    return "postgresdb";
  }

  private Connection getConnection() throws SQLException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
    var url = "jdbc:postgresql://localhost/" + getDatabaseName();
    var props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection(url, props);
  }

  // ==================== System Query Tests ====================

  @Test
  void selectVersion() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT VERSION()");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString(1)).isNotNull();
    }
  }

  @Test
  void selectCurrentSchema() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT CURRENT_SCHEMA()");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString(1)).isEqualTo(getDatabaseName());
    }
  }

  @Test
  void showTransactionIsolationLevel() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SHOW TRANSACTION ISOLATION LEVEL");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("LEVEL")).isNotNull();
    }
  }

  // ==================== SET Command Tests ====================

  @Test
  void setDateStyleISO() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("SET datestyle TO 'ISO'");
      // Should not throw
    }
  }

  @Test
  void setDateStyleWithEquals() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("SET datestyle = 'ISO'");
      // Should not throw
    }
  }

  @Test
  void setClientEncoding() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("SET client_encoding TO 'UTF8'");
      // Should not throw
    }
  }

  @Test
  void setApplicationName() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("SET application_name TO 'TestApp'");
      // Should not throw
    }
  }

  @Test
  void setTimeZone() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("SET timezone TO 'UTC'");
      // Should not throw
    }
  }

  @Test
  void setMultipleTimes() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("SET datestyle TO 'ISO'");
      st.execute("SET client_encoding TO 'UTF8'");
      st.execute("SET timezone TO 'UTC'");
      // All should succeed
    }
  }

  // ==================== Transaction Tests ====================

  @Test
  void beginCommitTransaction() throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("begin");
        st.execute("create vertex type TxTestProto if not exists");
        st.execute("create vertex TxTestProto set name = 'test1'");
        // Query within same transaction before commit
        var rs = st.executeQuery("SELECT FROM TxTestProto WHERE name = 'test1'");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("test1");
        st.execute("commit");
      }
    }
  }

  @Test
  void explicitBeginTransaction() throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("begin");
        st.execute("create vertex type TxTestProto2 if not exists");
        st.execute("create vertex TxTestProto2 set name = 'test2'");
        var rs = st.executeQuery("SELECT FROM TxTestProto2 WHERE name = 'test2'");
        assertThat(rs.next()).isTrue();
        st.execute("commit");
      }
    }
  }

  @Test
  void transactionWithPreparedStatements() throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("begin");
        st.execute("create vertex type PrepTxTestProto if not exists");

        final PreparedStatement pst = conn.prepareStatement("create vertex PrepTxTestProto set name = ?, value = ?");
        pst.setString(1, "prepared1");
        pst.setInt(2, 100);
        pst.execute();

        pst.setString(1, "prepared2");
        pst.setInt(2, 200);
        pst.execute();
        pst.close();

        var rs = st.executeQuery("SELECT FROM PrepTxTestProto ORDER BY value");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("prepared1");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("prepared2");

        st.execute("commit");
      }
    }
  }

  // ==================== Prepared Statement Tests ====================

  @Test
  void preparedStatementWithMultipleParameters() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE DOCUMENT TYPE MultiParam IF NOT EXISTS");
        st.execute("INSERT INTO MultiParam SET id = 1, name = 'one', value = 100");
        st.execute("INSERT INTO MultiParam SET id = 2, name = 'two', value = 200");
        st.execute("INSERT INTO MultiParam SET id = 3, name = 'three', value = 300");
      }

      try (var pst = conn.prepareStatement("SELECT FROM MultiParam WHERE id = ? AND value > ?")) {
        pst.setInt(1, 2);
        pst.setInt(2, 150);
        ResultSet rs = pst.executeQuery();
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("two");
        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  void preparedStatementReuse() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE DOCUMENT TYPE ReuseTest IF NOT EXISTS");
        st.execute("INSERT INTO ReuseTest SET id = 'a'");
        st.execute("INSERT INTO ReuseTest SET id = 'b'");
        st.execute("INSERT INTO ReuseTest SET id = 'c'");
      }

      try (var pst = conn.prepareStatement("SELECT FROM ReuseTest WHERE id = ?")) {
        // First execution
        pst.setString(1, "a");
        ResultSet rs1 = pst.executeQuery();
        assertThat(rs1.next()).isTrue();
        assertThat(rs1.getString("id")).isEqualTo("a");
        rs1.close();

        // Second execution with different parameter
        pst.setString(1, "b");
        ResultSet rs2 = pst.executeQuery();
        assertThat(rs2.next()).isTrue();
        assertThat(rs2.getString("id")).isEqualTo("b");
        rs2.close();

        // Third execution
        pst.setString(1, "c");
        ResultSet rs3 = pst.executeQuery();
        assertThat(rs3.next()).isTrue();
        assertThat(rs3.getString("id")).isEqualTo("c");
      }
    }
  }

  @Test
  void preparedStatementWithNullParameter() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE DOCUMENT TYPE NullParamTest IF NOT EXISTS");
        st.execute("INSERT INTO NullParamTest SET id = 1, name = 'test'");
        st.execute("INSERT INTO NullParamTest SET id = 2");
      }

      try (var pst = conn.prepareStatement("SELECT FROM NullParamTest WHERE id = ?")) {
        pst.setInt(1, 1);
        ResultSet rs = pst.executeQuery();
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("test");
      }
    }
  }

  // ==================== Error Handling Tests ====================

  @Test
  void syntaxErrorInQuery() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      assertThatThrownBy(() -> st.executeQuery("SELEC FROM NonExistent"))
          .isInstanceOf(PSQLException.class)
          .hasMessageContaining("Syntax error");
    }
  }

  @Test
  void typeDoesNotExist() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      assertThatThrownBy(() -> st.executeQuery("SELECT FROM NonExistentType"))
          .isInstanceOf(PSQLException.class);
    }
  }

  @Test
  void errorRecovery() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      // First: cause an error
      try {
        st.executeQuery("SELECT FROM NonExistentType");
      } catch (PSQLException e) {
        // Expected
      }

      // Second: connection should still work
      st.execute("CREATE VERTEX TYPE RecoveryTest IF NOT EXISTS");
      st.execute("CREATE VERTEX RecoveryTest SET name = 'recovered'");

      ResultSet rs = st.executeQuery("SELECT FROM RecoveryTest WHERE name = 'recovered'");
      assertThat(rs.next()).isTrue();
    }
  }

  @Test
  void consecutiveErrors() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      // Multiple consecutive errors
      for (int i = 0; i < 3; i++) {
        try {
          st.executeQuery("SELECT FROM NonExistentType" + i);
        } catch (PSQLException e) {
          // Expected
        }
      }

      // Connection should still work
      ResultSet rs = st.executeQuery("SELECT VERSION()");
      assertThat(rs.next()).isTrue();
    }
  }

  // ==================== pg_type Query Tests ====================

  @Test
  void pgTypeQueryByOid() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      // Query for int4[] array type (OID 1007)
      ResultSet rs = st.executeQuery("SELECT typelem FROM pg_type WHERE oid = 1007");
      assertThat(rs.next()).isTrue();
      // Element type should be int4 (OID 23)
      assertThat(rs.getInt("typelem")).isEqualTo(23);
    }
  }

  @Test
  void pgTypeQueryByName() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT oid FROM pg_type WHERE typname = 'int4'");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("oid")).isEqualTo(23);
    }
  }

  @Test
  void pgTypeQueryTextArray() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT typelem FROM pg_type WHERE oid = 1009");
      assertThat(rs.next()).isTrue();
      // Element type should be text (OID 25)
      assertThat(rs.getInt("typelem")).isEqualTo(25);
    }
  }

  @Test
  void pgTypeQueryFloat4Array() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT typelem FROM pg_type WHERE oid = 1021");
      assertThat(rs.next()).isTrue();
      // Element type should be float4 (OID 700)
      assertThat(rs.getInt("typelem")).isEqualTo(700);
    }
  }

  @Test
  void pgTypeQueryFloat8Array() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT typelem FROM pg_type WHERE oid = 1022");
      assertThat(rs.next()).isTrue();
      // Element type should be float8 (OID 701)
      assertThat(rs.getInt("typelem")).isEqualTo(701);
    }
  }

  @Test
  void pgTypeQueryBoolArray() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT typelem FROM pg_type WHERE oid = 1000");
      assertThat(rs.next()).isTrue();
      // Element type should be bool (OID 16)
      assertThat(rs.getInt("typelem")).isEqualTo(16);
    }
  }

  @Test
  void pgTypeQueryInt8Array() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT typelem FROM pg_type WHERE oid = 1016");
      assertThat(rs.next()).isTrue();
      // Element type should be int8 (OID 20)
      assertThat(rs.getInt("typelem")).isEqualTo(20);
    }
  }

  @Test
  void pgCatalogQuery() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT typdelim, typname FROM pg_catalog.pg_type WHERE oid = 1007");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("typdelim")).isEqualTo(",");
      assertThat(rs.getString("typname")).isEqualTo("int4");
    }
  }

  // ==================== Array Query Tests ====================

  @Test
  void insertAndSelectIntegerArray() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE IntArrayTest IF NOT EXISTS");
      st.execute("INSERT INTO IntArrayTest SET name = 'test', values = [1, 2, 3, 4, 5]");

      ResultSet rs = st.executeQuery("SELECT values FROM IntArrayTest WHERE name = 'test'");
      assertThat(rs.next()).isTrue();
      java.sql.Array arr = rs.getArray("values");
      assertThat(arr).isNotNull();
      Object[] values = (Object[]) arr.getArray();
      assertThat(values).hasSize(5);
    }
  }

  @Test
  void insertAndSelectStringArray() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE StringArrayTest IF NOT EXISTS");
      st.execute("INSERT INTO StringArrayTest SET name = 'test', tags = ['tag1', 'tag2', 'tag3']");

      ResultSet rs = st.executeQuery("SELECT tags FROM StringArrayTest WHERE name = 'test'");
      assertThat(rs.next()).isTrue();
      java.sql.Array arr = rs.getArray("tags");
      assertThat(arr).isNotNull();
      Object[] values = (Object[]) arr.getArray();
      assertThat(values).hasSize(3);
    }
  }

  @Test
  void insertAndSelectBooleanArray() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE BoolArrayTest IF NOT EXISTS");
      st.execute("INSERT INTO BoolArrayTest SET name = 'test', flags = [true, false, true]");

      ResultSet rs = st.executeQuery("SELECT flags FROM BoolArrayTest WHERE name = 'test'");
      assertThat(rs.next()).isTrue();
      java.sql.Array arr = rs.getArray("flags");
      assertThat(arr).isNotNull();
      Object[] values = (Object[]) arr.getArray();
      assertThat(values).hasSize(3);
    }
  }

  @Test
  void insertAndSelectLongArray() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE LongArrayTest IF NOT EXISTS");
      st.execute("INSERT INTO LongArrayTest SET name = 'test', bigNumbers = [9223372036854775806, 9223372036854775807]");

      ResultSet rs = st.executeQuery("SELECT bigNumbers FROM LongArrayTest WHERE name = 'test'");
      assertThat(rs.next()).isTrue();
      java.sql.Array arr = rs.getArray("bigNumbers");
      assertThat(arr).isNotNull();
      Object[] values = (Object[]) arr.getArray();
      assertThat(values).hasSize(2);
    }
  }

  // ==================== Empty Query Tests ====================

  @Test
  void emptySelect() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE EmptyTest IF NOT EXISTS");
      // Select from empty type
      ResultSet rs = st.executeQuery("SELECT FROM EmptyTest");
      assertThat(rs.next()).isFalse();
    }
  }

  // ==================== Query Language Tests ====================

  @Test
  void sqlScriptMultipleStatements() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("""
          {sqlscript}
          CREATE VERTEX TYPE ScriptTest IF NOT EXISTS;
          CREATE VERTEX ScriptTest SET id = 1, name = 'first';
          CREATE VERTEX ScriptTest SET id = 2, name = 'second';
          CREATE VERTEX ScriptTest SET id = 3, name = 'third';
          """);

      ResultSet rs = st.executeQuery("SELECT FROM ScriptTest ORDER BY id");
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertThat(count).isEqualTo(3);
    }
  }

  @Test
  void cypherCreateAndMatch() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE CypherNode IF NOT EXISTS");
      st.execute("{opencypher} CREATE (n:CypherNode {name: 'Alice', age: 30})");

      ResultSet rs = st.executeQuery("{opencypher} MATCH (n:CypherNode) WHERE n.name = 'Alice' RETURN n.name AS name, n.age AS age");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("Alice");
      assertThat(rs.getInt("age")).isEqualTo(30);
    }
  }

  // ==================== Column Schema Tests ====================

  @Test
  void selectWithExplicitColumns() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE ColumnTest IF NOT EXISTS");
      st.execute("INSERT INTO ColumnTest SET id = 1, name = 'test', value = 100");

      ResultSet rs = st.executeQuery("SELECT id, name, value FROM ColumnTest");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("id")).isEqualTo(1);
      assertThat(rs.getString("name")).isEqualTo("test");
      assertThat(rs.getInt("value")).isEqualTo(100);
    }
  }

  @Test
  void selectWithWildcard() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE WildcardTest IF NOT EXISTS");
      st.execute("INSERT INTO WildcardTest SET id = 1, name = 'test'");

      ResultSet rs = st.executeQuery("SELECT * FROM WildcardTest");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("id")).isNotNull();
      assertThat(rs.getString("name")).isEqualTo("test");
    }
  }

  // ==================== Insert with RETURN Tests ====================

  @Test
  void insertWithReturn() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE InsertReturnTest IF NOT EXISTS");

      ResultSet rs = st.executeQuery("INSERT INTO InsertReturnTest SET name = 'returned' RETURN name");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("returned");
    }
  }

  @Test
  void insertWithReturnMultipleFields() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE InsertReturnMulti IF NOT EXISTS");

      ResultSet rs = st.executeQuery("INSERT INTO InsertReturnMulti SET name = 'multi', value = 42 RETURN name, value");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("multi");
      assertThat(rs.getInt("value")).isEqualTo(42);
    }
  }

  // ==================== SHOW Command Tests ====================

  @Test
  void showCommand() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SHOW DATABASES");
      assertThat(rs.next()).isTrue();
    }
  }

  // ==================== Multiple Connection Tests ====================

  @Test
  void multipleSequentialConnections() throws Exception {
    for (int i = 0; i < 3; i++) {
      try (var conn = getConnection(); var st = conn.createStatement()) {
        ResultSet rs = st.executeQuery("SELECT VERSION()");
        assertThat(rs.next()).isTrue();
      }
    }
  }

  // ==================== Describe Statement Tests ====================

  @Test
  void preparedStatementDescribe() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE DOCUMENT TYPE DescribeTest IF NOT EXISTS");
        st.execute("INSERT INTO DescribeTest SET id = 1, name = 'test'");
      }

      // PreparedStatement internally uses DESCRIBE
      try (var pst = conn.prepareStatement("SELECT id, name FROM DescribeTest WHERE id = ?")) {
        pst.setInt(1, 1);
        ResultSet rs = pst.executeQuery();
        assertThat(rs.next()).isTrue();
        // Verify metadata is correct
        assertThat(rs.getMetaData().getColumnCount()).isGreaterThan(0);
      }
    }
  }

  // ==================== Savepoint Tests (Ignored) ====================

  @Test
  void savepointIgnored() throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("BEGIN");
        st.execute("CREATE VERTEX TYPE SavepointTest IF NOT EXISTS");
        st.execute("SAVEPOINT test_savepoint");
        st.execute("CREATE VERTEX SavepointTest SET name = 'sp_test'");
        st.execute("COMMIT");
      }
    }
    // Should complete without error (savepoint is ignored)
  }

  // ==================== Flush Command Tests ====================

  @Test
  void multipleStatementsWithFlush() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        // Use a unique type name for this test to avoid interference
        st.execute("CREATE DOCUMENT TYPE FlushTestUnique IF NOT EXISTS");
        // Clear any existing data
        st.execute("DELETE FROM FlushTestUnique");
      }

      // Execute multiple prepared statements to trigger flush
      for (int i = 0; i < 5; i++) {
        try (var pst = conn.prepareStatement("INSERT INTO FlushTestUnique SET idx = ?")) {
          pst.setInt(1, i);
          pst.execute();
        }
      }

      try (var st = conn.createStatement()) {
        ResultSet rs = st.executeQuery("SELECT count(*) as cnt FROM FlushTestUnique");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("cnt")).isEqualTo(5);
      }
    }
  }

  // ==================== Vertex/Edge Data Row Tests ====================

  @Test
  void selectVertexWithSystemProperties() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE VertexPropsTest IF NOT EXISTS");
      st.execute("CREATE VERTEX VertexPropsTest SET name = 'vertex1', value = 100");

      ResultSet rs = st.executeQuery("SELECT FROM VertexPropsTest WHERE name = 'vertex1'");
      assertThat(rs.next()).isTrue();
      // Check system properties are included
      assertThat(rs.getString("@rid")).isNotNull();
      assertThat(rs.getString("@type")).isEqualTo("VertexPropsTest");
      assertThat(rs.getString("@cat")).isEqualTo("v");
    }
  }

  @Test
  void selectEdgeWithSystemProperties() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE EdgeVertex IF NOT EXISTS");
      st.execute("CREATE EDGE TYPE EdgeEdge IF NOT EXISTS");
      st.execute("CREATE VERTEX EdgeVertex SET name = 'v1'");
      st.execute("CREATE VERTEX EdgeVertex SET name = 'v2'");
      st.execute("CREATE EDGE EdgeEdge FROM (SELECT FROM EdgeVertex WHERE name = 'v1') TO (SELECT FROM EdgeVertex WHERE name = 'v2')");

      ResultSet rs = st.executeQuery("SELECT FROM EdgeEdge");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("@rid")).isNotNull();
      assertThat(rs.getString("@type")).isEqualTo("EdgeEdge");
      assertThat(rs.getString("@cat")).isEqualTo("e");
    }
  }

  @Test
  void selectDocumentWithSystemProperties() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE DocPropsTest IF NOT EXISTS");
      st.execute("INSERT INTO DocPropsTest SET name = 'doc1'");

      ResultSet rs = st.executeQuery("SELECT FROM DocPropsTest WHERE name = 'doc1'");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("@rid")).isNotNull();
      assertThat(rs.getString("@type")).isEqualTo("DocPropsTest");
      assertThat(rs.getString("@cat")).isEqualTo("d");
    }
  }

  // ==================== More SET Command Tests ====================

  @Test
  void setDateStyleUnsupported() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      // Unsupported datestyle value should be logged but not fail
      st.execute("SET datestyle TO 'MDY'");
      // Should not throw
    }
  }

  @Test
  void setSearchPath() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("SET search_path TO public");
      // Should not throw
    }
  }

  @Test
  void setExtraFloatDigits() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("SET extra_float_digits = 3");
      // Should not throw
    }
  }

  // ==================== Update/Delete Command Tests ====================

  @Test
  void updateCommand() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE UpdateTest IF NOT EXISTS");
      st.execute("INSERT INTO UpdateTest SET id = 1, name = 'original'");
      st.execute("UPDATE UpdateTest SET name = 'updated' WHERE id = 1");

      ResultSet rs = st.executeQuery("SELECT name FROM UpdateTest WHERE id = 1");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("updated");
    }
  }

  @Test
  void deleteCommand() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE DeleteTest IF NOT EXISTS");
      st.execute("INSERT INTO DeleteTest SET id = 1, name = 'toDelete'");
      st.execute("DELETE FROM DeleteTest WHERE id = 1");

      ResultSet rs = st.executeQuery("SELECT FROM DeleteTest WHERE id = 1");
      assertThat(rs.next()).isFalse();
    }
  }

  // ==================== Match Query Tests ====================

  @Test
  void matchQuery() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE MatchVertex IF NOT EXISTS");
      st.execute("CREATE VERTEX MatchVertex SET name = 'match1'");

      ResultSet rs = st.executeQuery("MATCH {type: MatchVertex, as: m} RETURN m.name as name");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("match1");
    }
  }

  // ==================== Parameter Format Tests ====================

  @Test
  void preparedStatementWithLongParameter() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE DOCUMENT TYPE LongParamTest IF NOT EXISTS");
        st.execute("INSERT INTO LongParamTest SET id = 9223372036854775806, name = 'longValue'");
      }

      try (var pst = conn.prepareStatement("SELECT FROM LongParamTest WHERE id = ?")) {
        pst.setLong(1, 9223372036854775806L);
        ResultSet rs = pst.executeQuery();
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("longValue");
      }
    }
  }

  @Test
  void preparedStatementWithBooleanParameter() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE DOCUMENT TYPE BoolParamTest IF NOT EXISTS");
        st.execute("INSERT INTO BoolParamTest SET active = true, name = 'active'");
        st.execute("INSERT INTO BoolParamTest SET active = false, name = 'inactive'");
      }

      try (var pst = conn.prepareStatement("SELECT FROM BoolParamTest WHERE active = ?")) {
        pst.setBoolean(1, true);
        ResultSet rs = pst.executeQuery();
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("active");
      }
    }
  }

  @Test
  void preparedStatementWithFloatParameter() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE DOCUMENT TYPE FloatParamTest IF NOT EXISTS");
        st.execute("INSERT INTO FloatParamTest SET price = 29.99, name = 'item1'");
      }

      try (var pst = conn.prepareStatement("SELECT FROM FloatParamTest WHERE price > ?")) {
        pst.setFloat(1, 20.0f);
        ResultSet rs = pst.executeQuery();
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("item1");
      }
    }
  }

  // ==================== Limit Query Tests ====================

  @Test
  void selectWithLimit() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE LimitTest IF NOT EXISTS");
      for (int i = 0; i < 10; i++) {
        st.execute("INSERT INTO LimitTest SET idx = " + i);
      }

      ResultSet rs = st.executeQuery("SELECT FROM LimitTest ORDER BY idx LIMIT 3");
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertThat(count).isEqualTo(3);
    }
  }

  // ==================== JSON Data Tests ====================

  @Test
  void selectJsonProperty() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE JsonTest IF NOT EXISTS");
      st.execute("INSERT INTO JsonTest SET name = 'test', data = {\"key\": \"value\", \"num\": 42}");

      ResultSet rs = st.executeQuery("SELECT data FROM JsonTest WHERE name = 'test'");
      assertThat(rs.next()).isTrue();
      String json = rs.getString("data");
      assertThat(json).contains("key").contains("value");
    }
  }

  // ==================== Complex pg_type Query Tests ====================

  @Test
  void pgTypeQueryWithTypname() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT oid, typdelim FROM pg_type WHERE typname = '_int4'");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("oid")).isEqualTo(1007);
      assertThat(rs.getString("typdelim")).isEqualTo(",");
    }
  }

  @Test
  void pgTypeQueryForText() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT oid, typelem FROM pg_type WHERE typname = 'text'");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("oid")).isEqualTo(25);
    }
  }

  @Test
  void pgTypeQueryForVarchar() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT oid FROM pg_type WHERE typname = 'varchar'");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("oid")).isEqualTo(1043);
    }
  }

  @Test
  void pgTypeQueryUnknownType() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT oid FROM pg_type WHERE typname = 'unknown_type'");
      // Should return empty result, not error
      assertThat(rs.next()).isFalse();
    }
  }

  // ==================== Schema Query Tests ====================

  @Test
  void selectFromSchemaDatabase() throws Exception {
    try (var conn = getConnection(); var st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("SELECT FROM schema:database");
      assertThat(rs.next()).isTrue();
    }
  }
}
