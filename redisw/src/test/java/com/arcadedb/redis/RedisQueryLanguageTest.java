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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.test.BaseGraphServerTest;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests Redis commands as a query language via HTTP API.
 * Issue: https://github.com/ArcadeData/arcadedb/issues/1010
 */
public class RedisQueryLanguageTest extends BaseGraphServerTest {

  @Test
  void pingCommand() throws Exception {
    // Test PING command via HTTP API with language=redis
    final JSONObject response = executeCommand(0, "redis", "PING");
    assertThat(getResultValue(response)).isEqualTo("PONG");
  }

  @Test
  void pingWithArgument() throws Exception {
    // Test PING with argument
    final JSONObject response = executeCommand(0, "redis", "PING Hello");
    assertThat(getResultValue(response)).isEqualTo("Hello");
  }

  @Test
  void setAndGetCommands() throws Exception {
    // Test SET command
    JSONObject response = executeCommand(0, "redis", "SET testkey testvalue");
    assertThat(getResultValue(response)).isEqualTo("OK");

    // Test GET command
    response = executeCommand(0, "redis", "GET testkey");
    assertThat(getResultValue(response)).isEqualTo("testvalue");
  }

  @Test
  void existsCommand() throws Exception {
    // Set a key first
    executeCommand(0, "redis", "SET existskey value1");

    // Test EXISTS for existing key
    JSONObject response = executeCommand(0, "redis", "EXISTS existskey");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);

    // Test EXISTS for non-existing key
    response = executeCommand(0, "redis", "EXISTS nonexistent");
    assertThat(getResultValueAsInt(response)).isEqualTo(0);
  }

  @Test
  void incrDecrCommands() throws Exception {
    // Set initial numeric value
    executeCommand(0, "redis", "SET counter 10");

    // Test INCR
    JSONObject response = executeCommand(0, "redis", "INCR counter");
    assertThat(getResultValueAsLong(response)).isEqualTo(11L);

    // Test INCRBY
    response = executeCommand(0, "redis", "INCRBY counter 5");
    assertThat(getResultValueAsLong(response)).isEqualTo(16L);

    // Test DECR
    response = executeCommand(0, "redis", "DECR counter");
    assertThat(getResultValueAsLong(response)).isEqualTo(15L);

    // Test DECRBY
    response = executeCommand(0, "redis", "DECRBY counter 5");
    assertThat(getResultValueAsLong(response)).isEqualTo(10L);
  }

  @Test
  void getDelCommand() throws Exception {
    // Set a key
    executeCommand(0, "redis", "SET deletekey value123");

    // Test GETDEL (get and delete)
    JSONObject response = executeCommand(0, "redis", "GETDEL deletekey");
    assertThat(getResultValue(response)).isEqualTo("value123");

    // Verify key is deleted
    response = executeCommand(0, "redis", "GET deletekey");
    assertThat(getResultValue(response)).isNull();
  }

  @Test
  void hSetAndHGetWithDatabase() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // Create a document type with index
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id INTEGER");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");

    // Test HSET - persist a document (simplified syntax: HSET <type> <json>)
    JSONObject response = executeCommand(0, "redis", "HSET Person {\"id\":1,\"name\":\"John\",\"age\":30}");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);

    // Test HGET - retrieve by index (syntax: HGET <indexName> <key>)
    response = executeCommand(0, "redis", "HGET Person[id] 1");
    final JSONArray results = response.getJSONArray("result");
    assertThat(results.length()).isEqualTo(1);
    final JSONObject doc = results.getJSONObject(0);
    assertThat(doc.getInt("id")).isEqualTo(1);
    assertThat(doc.getString("name")).isEqualTo("John");
  }

  @Test
  void hGetReturnsConsistentFormatWithSQL() throws Exception {
    // Issue #3470: Redis HGET should return documents in the same format as SQL/OpenCypher
    // i.e. {"result": [{"@rid":"...","@type":"...","id":1}]}
    // NOT  {"result": [{"value": "{\"@rid\":\"...\",\"id\":1}"}]}
    final Database database = getServerDatabase(0, getDatabaseName());

    database.command("sql", "CREATE VERTEX TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.id LONG");
    database.command("sql", "CREATE INDEX ON doc (id) UNIQUE");
    database.command("sql", "INSERT INTO doc SET id = 1");

    // Query via SQL
    final JSONObject sqlResponse = executeQuery(0, "sql", "SELECT FROM doc WHERE id = 1");
    final JSONArray sqlResults = sqlResponse.getJSONArray("result");
    assertThat(sqlResults.length()).isEqualTo(1);
    final JSONObject sqlDoc = sqlResults.getJSONObject(0);

    // Query via Redis HGET
    final JSONObject redisResponse = executeQuery(0, "redis", "HGET doc[id] 1");
    final JSONArray redisResults = redisResponse.getJSONArray("result");
    assertThat(redisResults.length()).isEqualTo(1);
    final JSONObject redisDoc = redisResults.getJSONObject(0);

    // Both should have the same structure: document properties at the top level, not wrapped in "value"
    assertThat(redisDoc.has("@rid")).isTrue();
    assertThat(redisDoc.has("@type")).isTrue();
    assertThat(redisDoc.getLong("id")).isEqualTo(1L);

    // Should NOT have a "value" wrapper
    assertThat(redisDoc.has("value")).isFalse();

    // Should match SQL result structure
    assertThat(redisDoc.getString("@rid")).isEqualTo(sqlDoc.getString("@rid"));
    assertThat(redisDoc.getString("@type")).isEqualTo(sqlDoc.getString("@type"));
  }

  @Test
  void hExistsCommand() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // Create a document type with index
    database.command("sql", "CREATE DOCUMENT TYPE TestItem");
    database.command("sql", "CREATE PROPERTY TestItem.code STRING");
    database.command("sql", "CREATE INDEX ON TestItem (code) UNIQUE");

    // Insert a document
    executeCommand(0, "redis", "HSET TestItem {\"code\":\"ABC123\"}");

    // Test HEXISTS for existing key
    JSONObject response = executeCommand(0, "redis", "HEXISTS TestItem[code] ABC123");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);

    // Test HEXISTS for non-existing key
    response = executeCommand(0, "redis", "HEXISTS TestItem[code] XYZ999");
    assertThat(getResultValueAsInt(response)).isEqualTo(0);
  }

  @Test
  void hDelCommand() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // Create a document type with index
    database.command("sql", "CREATE DOCUMENT TYPE Product");
    database.command("sql", "CREATE PROPERTY Product.sku STRING");
    database.command("sql", "CREATE INDEX ON Product (sku) UNIQUE");

    // Insert documents
    executeCommand(0, "redis", "HSET Product {\"sku\":\"SKU001\",\"name\":\"Product1\"}");
    executeCommand(0, "redis", "HSET Product {\"sku\":\"SKU002\",\"name\":\"Product2\"}");

    // Verify documents exist
    JSONObject response = executeCommand(0, "redis", "HEXISTS Product[sku] SKU001");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);

    // Test HDEL
    response = executeCommand(0, "redis", "HDEL Product[sku] SKU001");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);

    // Verify document is deleted
    response = executeCommand(0, "redis", "HEXISTS Product[sku] SKU001");
    assertThat(getResultValueAsInt(response)).isEqualTo(0);

    // Verify other document still exists
    response = executeCommand(0, "redis", "HEXISTS Product[sku] SKU002");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);
  }

  @Test
  void queryEngineDirectly() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // Test direct query engine access (like MongoDB tests do)
    try (final ResultSet rs = database.query("redis", "PING")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((String) result.getProperty("value")).isEqualTo("PONG");
    }
  }

  @Test
  void commandNotFound() throws Exception {
    // Test unsupported command
    try {
      executeCommand(0, "redis", "UNKNOWNCOMMAND arg1");
      fail("Expected exception for unknown command");
    } catch (Exception e) {
      assertThat(e.getMessage()).contains("Command not found");
    }
  }

  @Test
  void batchCommands() throws Exception {
    // Test multiple commands separated by newlines (batch execution)
    final String batchCommands = """
        SET batch1 value1
        SET batch2 value2
        SET batch3 value3
        GET batch1
        GET batch2
        GET batch3
        """;

    final JSONObject response = executeCommand(0, "redis", batchCommands);

    // Batch returns array of results
    final Object value = getResultValue(response);
    assertThat(value).isInstanceOf(JSONArray.class);
    final JSONArray results = (JSONArray) value;
    assertThat(results.length()).isEqualTo(6);
    assertThat(results.get(0)).isEqualTo("OK");
    assertThat(results.get(1)).isEqualTo("OK");
    assertThat(results.get(2)).isEqualTo("OK");
    assertThat(results.get(3)).isEqualTo("value1");
    assertThat(results.get(4)).isEqualTo("value2");
    assertThat(results.get(5)).isEqualTo("value3");
  }

  @Test
  void multiExecTransaction() throws Exception {
    // Test MULTI/EXEC transaction (official Redis syntax)
    final String transactionCommands = """
        MULTI
        SET tx1 txvalue1
        SET tx2 txvalue2
        INCR counter1
        EXEC
        """;

    final JSONObject response = executeCommand(0, "redis", transactionCommands);

    // Transaction returns array of results
    final Object value = getResultValue(response);
    assertThat(value).isInstanceOf(JSONArray.class);
    final JSONArray results = (JSONArray) value;
    assertThat(results.length()).isEqualTo(3);
    assertThat(results.get(0)).isEqualTo("OK");
    assertThat(results.get(1)).isEqualTo("OK");
    assertThat(results.getLong(2)).isEqualTo(1L);

    // Verify values were persisted
    JSONObject getResponse = executeCommand(0, "redis", "GET tx1");
    assertThat(getResultValue(getResponse)).isEqualTo("txvalue1");

    getResponse = executeCommand(0, "redis", "GET tx2");
    assertThat(getResultValue(getResponse)).isEqualTo("txvalue2");
  }

  @Test
  void multiExecWithComments() throws Exception {
    // Test that comments are ignored
    final String commands = """
        # This is a comment
        SET comment1 value1
        // This is also a comment
        SET comment2 value2
        """;

    final JSONObject response = executeCommand(0, "redis", commands);

    final Object value = getResultValue(response);
    assertThat(value).isInstanceOf(JSONArray.class);
    final JSONArray results = (JSONArray) value;
    assertThat(results.length()).isEqualTo(2);
  }

  @Test
  void discard() throws Exception {
    // Test DISCARD command
    final String commands = """
        MULTI
        SET discard1 value1
        DISCARD
        """;

    final JSONObject response = executeCommand(0, "redis", commands);
    assertThat(getResultValue(response)).isEqualTo("OK");

    // Verify value was NOT persisted
    final JSONObject getResponse = executeCommand(0, "redis", "GET discard1");
    assertThat(getResultValue(getResponse)).isNull();
  }

  @Test
  void globalVariablesSharedWithSQL() throws Exception {
    // Issue #3246: Redis commands should use database's globalVariables
    // This test verifies that Redis SET/GET uses the same storage as SQL variables

    // Set a value via Redis
    executeCommand(0, "redis", "SET sharedVar hello_from_redis");

    // Verify we can read it back via Redis
    JSONObject response = executeCommand(0, "redis", "GET sharedVar");
    assertThat(getResultValue(response)).isEqualTo("hello_from_redis");

    // Verify the value is accessible via SQL using $variable syntax
    final Database database = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = database.query("sql", "SELECT $sharedVar as val")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((String) result.getProperty("val")).isEqualTo("hello_from_redis");
    }

    // Set another value via Redis and verify it's in the global variables map
    executeCommand(0, "redis", "SET counter 100");
    executeCommand(0, "redis", "INCR counter");

    // The incremented value should be accessible
    response = executeCommand(0, "redis", "GET counter");
    assertThat(getResultValueAsLong(response)).isEqualTo(101L);

    // And also via SQL
    try (final ResultSet rs = database.query("sql", "SELECT $counter as val")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(((Number) result.getProperty("val")).longValue()).isEqualTo(101L);
    }
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Redis Protocol Plugin needed for the module to be loaded
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /**
   * Extracts the "value" property from the first result in the response.
   * The response format is: {"result": [{"value": <result>}]}
   */
  private Object getResultValue(final JSONObject response) {
    final JSONArray results = response.getJSONArray("result");
    if (results.isEmpty()) {
      return null;
    }
    final JSONObject firstResult = results.getJSONObject(0);
    if (firstResult.isNull("value")) {
      return null;
    }
    return firstResult.get("value");
  }

  private int getResultValueAsInt(final JSONObject response) {
    final Object value = getResultValue(response);
    if (value instanceof Number number) {
      return number.intValue();
    }
    return Integer.parseInt(value.toString());
  }

  private long getResultValueAsLong(final JSONObject response) {
    final Object value = getResultValue(response);
    if (value instanceof Number number) {
      return number.longValue();
    }
    return Long.parseLong(value.toString());
  }

  @Test
  void nonIdempotentCommandsRejectedOnQueryEndpoint() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // Create schema for persistent commands
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.id LONG");
    database.command("sql", "CREATE INDEX ON doc (id) UNIQUE");

    // Insert a document via command endpoint (should work)
    JSONObject response = executeCommand(0, "redis", "HSET doc {\"id\":1}");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);

    // Read-only commands via query endpoint should work
    response = executeQuery(0, "redis", "PING");
    assertThat(getResultValue(response)).isEqualTo("PONG");

    response = executeQuery(0, "redis", "HGET doc[id] 1");
    assertThat(response.getJSONArray("result").length()).isEqualTo(1);
    assertThat(response.getJSONArray("result").getJSONObject(0).has("@rid")).isTrue();

    response = executeQuery(0, "redis", "HEXISTS doc[id] 1");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);

    // Non-idempotent commands via query endpoint should be rejected
    final String expectedError = "Non-idempotent Redis command";

    // HSET on query endpoint
    try {
      executeQuery(0, "redis", "HSET doc {\"id\":2}");
      fail("HSET should not be allowed on query endpoint");
    } catch (final Exception e) {
      assertThat(e.getMessage()).contains(expectedError);
    }

    // HDEL on query endpoint
    try {
      executeQuery(0, "redis", "HDEL doc[id] 1");
      fail("HDEL should not be allowed on query endpoint");
    } catch (final Exception e) {
      assertThat(e.getMessage()).contains(expectedError);
    }

    // SET on query endpoint
    try {
      executeQuery(0, "redis", "SET mykey myvalue");
      fail("SET should not be allowed on query endpoint");
    } catch (final Exception e) {
      assertThat(e.getMessage()).contains(expectedError);
    }

    // INCR on query endpoint
    try {
      executeQuery(0, "redis", "INCR counter");
      fail("INCR should not be allowed on query endpoint");
    } catch (final Exception e) {
      assertThat(e.getMessage()).contains(expectedError);
    }

    // GETDEL on query endpoint
    try {
      executeQuery(0, "redis", "GETDEL somekey");
      fail("GETDEL should not be allowed on query endpoint");
    } catch (final Exception e) {
      assertThat(e.getMessage()).contains(expectedError);
    }

    // Verify the document with id=2 was NOT created (HSET was rejected)
    response = executeQuery(0, "redis", "HEXISTS doc[id] 2");
    assertThat(getResultValueAsInt(response)).isEqualTo(0);

    // Verify the document with id=1 still exists (HDEL was rejected)
    response = executeQuery(0, "redis", "HEXISTS doc[id] 1");
    assertThat(getResultValueAsInt(response)).isEqualTo(1);
  }

  protected JSONObject executeQuery(final int serverIndex, final String language, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject();
    request.put("language", language);
    request.put("command", command);

    try (OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }

    final int responseCode = connection.getResponseCode();
    if (responseCode != 200) {
      final String error = new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      throw new RuntimeException("HTTP " + responseCode + ": " + error);
    }

    final String response = new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return new JSONObject(response);
  }

  protected JSONObject executeCommand(final int serverIndex, final String language, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject();
    request.put("language", language);
    request.put("command", command);

    try (OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }

    final int responseCode = connection.getResponseCode();
    if (responseCode != 200) {
      final String error = new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      throw new RuntimeException("HTTP " + responseCode + ": " + error);
    }

    final String response = new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return new JSONObject(response);
  }
}
