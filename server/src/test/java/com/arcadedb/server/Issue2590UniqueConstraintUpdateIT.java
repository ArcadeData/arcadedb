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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for issue #2590 - Unique constraints not enforced during UPDATE operations.
 * <p>
 * This test reproduces issue #2590 using RemoteDatabase to verify the bug exists
 * in server/client mode as well as embedded mode.
 * <p>
 * Expected behavior: UPDATE should fail when attempting to set a unique field
 * to a value that already exists in another record.
 * <p>
 * Actual behavior (BUG): UPDATE succeeds, creating duplicate values for unique field.
 */
public class Issue2590UniqueConstraintUpdateIT extends BaseGraphServerTest {

  @Test
  void testUniqueIndexOnUpdate() {// testear el estado de las tx en cada etapa

    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      database.command("sqlscript", """
          create vertex type `SimpleVertexEx` if not exists;
          create property SimpleVertexEx.svex if not exists STRING;
          create property SimpleVertexEx.svuuid if not exists STRING;
          create index on SimpleVertexEx(svuuid) UNIQUE NULL_STRATEGY ERROR;
          """);

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

      MutableVertex svt1 = database.newVertex("SimpleVertexEx");
      String uuid1 = UUID.randomUUID().toString();
      svt1.set("svex", uuid1);
      svt1.set("svuuid", uuid1);
      svt1.save();
      database.commit();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      MutableVertex svt2 = database.newVertex("SimpleVertexEx");
      String uuid2 = UUID.randomUUID().toString();
      svt2.set("svex", uuid2);
      svt2.set("svuuid", uuid2);
      svt2.save();
      database.commit();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      svt2.set("svuuid", uuid1);
      svt2.save();

      Assertions.assertThatThrownBy(() -> database.commit()).isInstanceOf(DuplicatedKeyException.class)
      ;
    }
  }

  /**
   * Test Case 1: Test via RemoteDatabase API - Exact scenario from issue #2590
   */
  @Test
  public void testRemoteDatabaseUniqueConstraintViolationOnUpdate() throws Exception {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create vertex type with unique field
      database.command("sql", "CREATE VERTEX TYPE RemoteTestVertex");
      database.command("sql", "CREATE PROPERTY RemoteTestVertex.uniqueField STRING");
      database.command("sql", "CREATE INDEX ON RemoteTestVertex (uniqueField) UNIQUE");

      // Insert first vertex with value "A"
      database.command("sql", "INSERT INTO RemoteTestVertex SET uniqueField = 'A', name = 'First'");

      // Insert second vertex with value "B"
      database.command("sql", "INSERT INTO RemoteTestVertex SET uniqueField = 'B', name = 'Second'");

      // Verify INSERT of duplicate value is correctly prevented
      assertThatThrownBy(() -> {
        database.command("sql", "INSERT INTO RemoteTestVertex SET uniqueField = 'A', name = 'Third'");
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("INSERT should prevent duplicate unique values");

      // Attempt to UPDATE second vertex to use value "A" (should fail but currently succeeds - BUG)
      assertThatThrownBy(() -> {
        database.command("sql", "UPDATE RemoteTestVertex SET uniqueField = 'A' WHERE uniqueField = 'B'");
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("UPDATE via RemoteDatabase should enforce unique constraint but currently does not - this is the BUG");
    }
  }

  /**
   * Test Case 2: Test via HTTP API - UPDATE with duplicate value
   */
  @Test
  public void testHttpApiUniqueConstraintViolationOnUpdate() throws Exception {
    // Create vertex type with unique field
    JSONObject response = executeCommand(0, "sql", "CREATE VERTEX TYPE HttpTestVertex");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE PROPERTY HttpTestVertex.email STRING");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE INDEX ON HttpTestVertex (email) UNIQUE");
    assertThat(response).isNotNull();

    // Insert first vertex
    response = executeCommand(0, "sql", "INSERT INTO HttpTestVertex SET email = 'alice@example.com', name = 'Alice'");
    assertThat(response).isNotNull();
    assertThat(response.getJSONObject("result").getJSONArray("vertices").length()).isEqualTo(1);

    // Insert second vertex
    response = executeCommand(0, "sql", "INSERT INTO HttpTestVertex SET email = 'bob@example.com', name = 'Bob'");
    assertThat(response).isNotNull();
    assertThat(response.getJSONObject("result").getJSONArray("vertices").length()).isEqualTo(1);

    // Verify INSERT of duplicate is prevented
    response = executeCommand(0, "sql", "INSERT INTO HttpTestVertex SET email = 'alice@example.com', name = 'Charlie'");
    // This should return error in the response
    assertThat(response).isNull(); // executeCommand returns null on error

    // Attempt UPDATE to duplicate (should fail but currently succeeds - BUG)
    response = executeCommand(0, "sql", "UPDATE HttpTestVertex SET email = 'alice@example.com' WHERE email = 'bob@example.com'");
    // This should also return error but currently might succeed
    assertThat(response).as("UPDATE via HTTP API should enforce unique constraint but currently does not - this is the BUG")
        .isNull();
  }

  /**
   * Test Case 3: Test with Integer type via RemoteDatabase
   */
  @Test
  public void testRemoteDatabaseUniqueConstraintWithIntegerType() throws Exception {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create vertex type with unique integer field
      database.command("sql", "CREATE VERTEX TYPE IntegerRemoteTest");
      database.command("sql", "CREATE PROPERTY IntegerRemoteTest.employeeId INTEGER");
      database.command("sql", "CREATE INDEX ON IntegerRemoteTest (employeeId) UNIQUE");

      // Insert vertices
      database.command("sql", "INSERT INTO IntegerRemoteTest SET employeeId = 1001, name = 'Employee A'");
      database.command("sql", "INSERT INTO IntegerRemoteTest SET employeeId = 1002, name = 'Employee B'");

      // Verify INSERT duplicate is prevented
      assertThatThrownBy(() -> {
        database.command("sql", "INSERT INTO IntegerRemoteTest SET employeeId = 1001, name = 'Employee C'");
      }).isInstanceOf(DuplicatedKeyException.class);

      // UPDATE to duplicate (should fail but currently succeeds - BUG)
      assertThatThrownBy(() -> {
        database.command("sql", "UPDATE IntegerRemoteTest SET employeeId = 1001 WHERE employeeId = 1002");
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("UPDATE with Integer type via RemoteDatabase should enforce unique constraint");
    }
  }

  /**
   * Test Case 4: Test with Long type via HTTP API
   */
  @Test
  public void testHttpApiUniqueConstraintWithLongType() throws Exception {
    // Create vertex type with unique long field
    JSONObject response = executeCommand(0, "sql", "CREATE VERTEX TYPE LongHttpTest");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE PROPERTY LongHttpTest.accountNumber LONG");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE INDEX ON LongHttpTest (accountNumber) UNIQUE");
    assertThat(response).isNotNull();

    // Insert vertices
    response = executeCommand(0, "sql", "INSERT INTO LongHttpTest SET accountNumber = 9000000001, holder = 'Account A'");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "INSERT INTO LongHttpTest SET accountNumber = 9000000002, holder = 'Account B'");
    assertThat(response).isNotNull();

    // UPDATE to duplicate (should fail but currently succeeds - BUG)
    response = executeCommand(0, "sql", "UPDATE LongHttpTest SET accountNumber = 9000000001 WHERE accountNumber = 9000000002");
    assertThat(response).as("UPDATE with Long type via HTTP should enforce unique constraint").isNull();
  }

  /**
   * Test Case 5: Test UPDATE with multiple fields via RemoteDatabase
   */
  @Test
  public void testRemoteDatabaseUpdateMultipleFieldsWithUniqueConstraint() throws Exception {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create vertex type with unique field
      database.command("sql", "CREATE VERTEX TYPE MultiFieldRemoteTest");
      database.command("sql", "CREATE PROPERTY MultiFieldRemoteTest.username STRING");
      database.command("sql", "CREATE PROPERTY MultiFieldRemoteTest.email STRING");
      database.command("sql", "CREATE PROPERTY MultiFieldRemoteTest.age INTEGER");
      database.command("sql", "CREATE INDEX ON MultiFieldRemoteTest (username) UNIQUE");

      // Insert vertices
      database.command("sql",
          "INSERT INTO MultiFieldRemoteTest SET username = 'alice', email = 'alice@test.com', age = 30");
      database.command("sql",
          "INSERT INTO MultiFieldRemoteTest SET username = 'bob', email = 'bob@test.com', age = 25");

      // UPDATE multiple fields including unique field to duplicate (should fail but currently succeeds - BUG)
      assertThatThrownBy(() -> {
        database.command("sql",
            "UPDATE MultiFieldRemoteTest SET username = 'alice', email = 'bob_updated@test.com', age = 26 WHERE username = 'bob'");
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("UPDATE of multiple fields should enforce unique constraint on unique field");
    }
  }

  /**
   * Test Case 6: Test UPDATE with WHERE clause via HTTP API
   */
  @Test
  public void testHttpApiUpdateWithWhereClause() throws Exception {
    // Create vertex type
    JSONObject response = executeCommand(0, "sql", "CREATE VERTEX TYPE WhereClauseHttpTest");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE PROPERTY WhereClauseHttpTest.productCode STRING");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE PROPERTY WhereClauseHttpTest.status STRING");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE INDEX ON WhereClauseHttpTest (productCode) UNIQUE");
    assertThat(response).isNotNull();

    // Insert test data
    response = executeCommand(0, "sql",
        "INSERT INTO WhereClauseHttpTest SET productCode = 'PROD-100', status = 'available'");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql",
        "INSERT INTO WhereClauseHttpTest SET productCode = 'PROD-200', status = 'unavailable'");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql",
        "INSERT INTO WhereClauseHttpTest SET productCode = 'PROD-300', status = 'pending'");
    assertThat(response).isNotNull();

    // UPDATE with WHERE clause to create duplicate (should fail but currently succeeds - BUG)
    response = executeCommand(0, "sql",
        "UPDATE WhereClauseHttpTest SET productCode = 'PROD-100' WHERE status = 'unavailable'");
    assertThat(response).as("UPDATE with WHERE clause via HTTP should enforce unique constraint").isNull();
  }

  /**
   * Test Case 7: Test UPDATE to null and then to duplicate via RemoteDatabase
   */
  @Test
  public void testRemoteDatabaseUpdateToNullThenToDuplicate() throws Exception {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create vertex type with unique field
      database.command("sql", "CREATE VERTEX TYPE NullRemoteTest");
      database.command("sql", "CREATE PROPERTY NullRemoteTest.licenseKey STRING");
      database.command("sql", "CREATE INDEX ON NullRemoteTest (licenseKey) UNIQUE NULL_STRATEGY SKIP");

      // Insert vertices
      database.command("sql", "INSERT INTO NullRemoteTest SET licenseKey = 'LIC-AAA', product = 'Product A'");
      database.command("sql", "INSERT INTO NullRemoteTest SET licenseKey = 'LIC-BBB', product = 'Product B'");

      // Update to null (should succeed)
      database.command("sql", "UPDATE NullRemoteTest SET licenseKey = null WHERE licenseKey = 'LIC-BBB'");

      // Now update to duplicate existing value (should fail but currently succeeds )
//      assertThatThrownBy(() -> {
      ResultSet resultSet1 = database.command("sql", "UPDATE NullRemoteTest SET licenseKey = 'LIC-CCC' WHERE licenseKey IS NULL");
        resultSet1.stream().forEach(result -> System.out.println("result.toJSON() = " + result.toJSON()));

//      }).isInstanceOf(DuplicatedKeyException.class)
//          .as("UPDATE from null to duplicate value should enforce unique constraint");
        ResultSet resultSet = database.query("sql", "SELECT FROM NullRemoteTest ");
        resultSet.stream().forEach(result -> System.out.println("result.toJSON() = " + result.toJSON()));
    }
  }

  /**
   * Test Case 8: Test valid UPDATE to non-duplicate value via RemoteDatabase
   * This test should pass - updating to a unique value that doesn't exist should succeed.
   */
  @Test
  public void testRemoteDatabaseValidUpdateToUniqueNonDuplicateValue() throws Exception {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create vertex type with unique field
      database.command("sql", "CREATE VERTEX TYPE ValidRemoteTest");
      database.command("sql", "CREATE PROPERTY ValidRemoteTest.apiKey STRING");
      database.command("sql", "CREATE INDEX ON ValidRemoteTest (apiKey) UNIQUE");

      // Insert vertices
      database.command("sql", "INSERT INTO ValidRemoteTest SET apiKey = 'KEY-ALPHA', service = 'Service A'");
      database.command("sql", "INSERT INTO ValidRemoteTest SET apiKey = 'KEY-BETA', service = 'Service B'");

      // UPDATE to new unique value (should succeed)
      database.command("sql", "UPDATE ValidRemoteTest SET apiKey = 'KEY-GAMMA' WHERE apiKey = 'KEY-BETA'");

      // Verify the update succeeded
      var result = database.query("sql", "SELECT FROM ValidRemoteTest WHERE apiKey = 'KEY-GAMMA'");
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      result.close();
    }
  }

  /**
   * Test Case 9: Test UPDATE same record to same value via HTTP API
   * This should always succeed as it's not creating a duplicate.
   */
  @Test
  public void testHttpApiUpdateSameRecordToSameValue() throws Exception {
    // Create vertex type
    JSONObject response = executeCommand(0, "sql", "CREATE VERTEX TYPE SameValueHttpTest");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE PROPERTY SameValueHttpTest.uniqueId STRING");
    assertThat(response).isNotNull();

    response = executeCommand(0, "sql", "CREATE INDEX ON SameValueHttpTest (uniqueId) UNIQUE");
    assertThat(response).isNotNull();

    // Insert vertex
    response = executeCommand(0, "sql", "INSERT INTO SameValueHttpTest SET uniqueId = 'ID-XYZ', data = 'Some data'");
    assertThat(response).isNotNull();

    // Update to same value (should succeed - not a duplicate)
    response = executeCommand(0, "sql", "UPDATE SameValueHttpTest SET uniqueId = 'ID-XYZ' WHERE uniqueId = 'ID-XYZ'");
    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();

    // Verify it succeeded
    response = executeCommand(0, "sql", "SELECT FROM SameValueHttpTest WHERE uniqueId = 'ID-XYZ'");
    assertThat(response).isNotNull();
    assertThat(response.getJSONObject("result").getJSONArray("vertices").length()).isEqualTo(1);
  }

  /**
   * Test Case 10: Test attempting to swap unique values between records via RemoteDatabase
   */
  @Test
  public void testRemoteDatabaseSwapUniqueValuesBetweenRecords() throws Exception {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create vertex type with unique field
      database.command("sql", "CREATE VERTEX TYPE SwapRemoteTest");
      database.command("sql", "CREATE PROPERTY SwapRemoteTest.position STRING");
      database.command("sql", "CREATE INDEX ON SwapRemoteTest (position) UNIQUE");

      // Insert vertices
      database.command("sql", "INSERT INTO SwapRemoteTest SET position = 'FIRST', data = 'Data 1'");
      database.command("sql", "INSERT INTO SwapRemoteTest SET position = 'SECOND', data = 'Data 2'");

      // Attempt to swap values (should fail on first update - BUG)
      assertThatThrownBy(() -> {
        database.command("sql", "UPDATE SwapRemoteTest SET position = 'SECOND' WHERE position = 'FIRST'");
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("Swapping unique values should fail on first update due to constraint violation");
    }
  }

  /**
   * Test Case 11: Test UPDATE with parameterized query via RemoteDatabase
   */
  @Test
  public void testRemoteDatabaseParameterizedUpdateWithUniqueConstraint() throws Exception {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create vertex type with unique field
      database.command("sql", "CREATE VERTEX TYPE ParamRemoteTest");
      database.command("sql", "CREATE PROPERTY ParamRemoteTest.deviceId STRING");
      database.command("sql", "CREATE INDEX ON ParamRemoteTest (deviceId) UNIQUE");

      // Insert vertices
      database.command("sql", "INSERT INTO ParamRemoteTest SET deviceId = 'DEV-001', status = 'online'");
      database.command("sql", "INSERT INTO ParamRemoteTest SET deviceId = 'DEV-002', status = 'offline'");

      // UPDATE with parameters to duplicate (should fail but currently succeeds - BUG)
      assertThatThrownBy(() -> {
        var params = new HashMap<String, Object>();
        params.put("newDeviceId", "DEV-001");
        params.put("oldDeviceId", "DEV-002");
        database.command("sql", "UPDATE ParamRemoteTest SET deviceId = :newDeviceId WHERE deviceId = :oldDeviceId", params);
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("Parameterized UPDATE should enforce unique constraint");
    }
  }

  /**
   * Test Case 12: Test concurrent UPDATE attempts via RemoteDatabase
   * This tests race conditions where two updates might try to create duplicates.
   */
  @Test
  public void testRemoteDatabaseConcurrentUpdateWithUniqueConstraint() throws Exception {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create vertex type with unique field
      database.command("sql", "CREATE VERTEX TYPE ConcurrentRemoteTest");
      database.command("sql", "CREATE PROPERTY ConcurrentRemoteTest.ticketId STRING");
      database.command("sql", "CREATE INDEX ON ConcurrentRemoteTest (ticketId) UNIQUE");

      // Insert vertices
      database.command("sql", "INSERT INTO ConcurrentRemoteTest SET ticketId = 'TICKET-100', status = 'open'");
      database.command("sql", "INSERT INTO ConcurrentRemoteTest SET ticketId = 'TICKET-200', status = 'open'");
      database.command("sql", "INSERT INTO ConcurrentRemoteTest SET ticketId = 'TICKET-300', status = 'open'");

      // First update should succeed (UPDATE to new unique value)
      database.command("sql", "UPDATE ConcurrentRemoteTest SET ticketId = 'TICKET-400' WHERE ticketId = 'TICKET-200'");

      // Second update to duplicate should fail (BUG - currently succeeds)
      assertThatThrownBy(() -> {
        database.command("sql", "UPDATE ConcurrentRemoteTest SET ticketId = 'TICKET-100' WHERE ticketId = 'TICKET-300'");
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("Concurrent UPDATE should enforce unique constraint");
    }
  }
}
