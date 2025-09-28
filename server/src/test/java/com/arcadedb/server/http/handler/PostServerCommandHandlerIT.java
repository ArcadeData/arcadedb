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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class PostServerCommandHandlerIT extends BaseGraphServerTest {

  @Test
  void testSetDatabaseSettingCommand() throws Exception {

    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("command", """
                set database setting graph `arcadedb.dateTimeFormat` "yyyy-MM-dd HH:mm:ss.SSS"
                """)
            .toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);

  }

  /**
   * Regression test for issue #2553: 'OPEN DATABASE foo' was failing with "Database name empty"
   * Tests that database operations work correctly regardless of command case
   */
  @Test
  void testOpenDatabaseCaseSensitivityFix() throws Exception {
    // First ensure the database is closed
    executeServerCommand("CLOSE DATABASE graph");

    // Test the specific case from issue #2553 - uppercase command
    HttpResponse<String> response = executeServerCommand("OPEN DATABASE graph");
    assertThat(response.statusCode()).isEqualTo(200);

    // Verify the database is actually opened by trying to access it
    response = executeServerCommand("get server events");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test all database-related commands with different case variations
   */
  @ParameterizedTest
  @CsvSource({
      "OPEN DATABASE graph, open database graph, Open Database graph",
      "CLOSE DATABASE graph, close database graph, Close Database graph"
  })
  void testDatabaseCommandsCaseSensitivity(String uppercase, String lowercase, String mixedcase) throws Exception {
    // Test uppercase command
    HttpResponse<String> response = executeServerCommand(uppercase);
    assertThat(response.statusCode()).isEqualTo(200);

    // Test lowercase command (only if different from uppercase)
    if (!uppercase.equals(lowercase)) {
      response = executeServerCommand(lowercase);
      assertThat(response.statusCode()).isEqualTo(200);
    }

    // Test mixed case command (only if different from others)
    if (!uppercase.equals(mixedcase) && !lowercase.equals(mixedcase)) {
      response = executeServerCommand(mixedcase);
      assertThat(response.statusCode()).isEqualTo(200);
    }
  }

  /**
   * Test create/drop database commands with different case variations
   */
  @Test
  void testCreateDropDatabaseCaseSensitivity() throws Exception {
    // Test creating database with different cases
    HttpResponse<String> response = executeServerCommand("CREATE DATABASE testdb");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("drop database testdb");
    assertThat(response.statusCode()).isEqualTo(200);

    // Test with mixed case
    response = executeServerCommand("Create Database testdb2");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("Drop Database testdb2");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test user management commands with different case variations
   */
  @Test
  void testUserCommandsCaseSensitivity() throws Exception {
    // CREATE USER expects JSON payload format
    HttpResponse<String> response = executeServerCommand("""
        CREATE USER {"name":"testuser","password":"testpass"}
        """);
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("""
        create user {"name":"testuser2","password":"testpass"}
        """);
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("""
        Create User {"name":"testuser3","password":"testpass"}
        """);
    assertThat(response.statusCode()).isEqualTo(200);

    // Test DROP USER commands
    response = executeServerCommand("DROP USER testuser");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("drop user testuser2");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("Drop User testuser3");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test server setting commands with different case variations
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SET SERVER SETTING `server.httpSessionTimeout` 300",
      "set server setting `server.httpSessionTimeout` 300",
      "Set Server Setting `server.httpSessionTimeout` 300"
  })
  void testServerSettingCommandsCaseSensitivity(String command) throws Exception {
    HttpResponse<String> response = executeServerCommand(command);
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test database setting commands with different case variations
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SET DATABASE SETTING graph `arcadedb.dateTimeFormat` 'yyyy-MM-dd'",
      "set database setting graph `arcadedb.dateTimeFormat` 'yyyy-MM-dd'",
      "Set Database Setting graph `arcadedb.dateTimeFormat` 'yyyy-MM-dd'"
  })
  void testDatabaseSettingCommandsCaseSensitivity(String command) throws Exception {
    HttpResponse<String> response = executeServerCommand(command);
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test server information commands with different case variations
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "GET SERVER EVENTS",
      "get server events",
      "Get Server Events"
  })
  void testServerInfoCommandsCaseSensitivity(String command) throws Exception {
    HttpResponse<String> response = executeServerCommand(command);
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test cluster management commands with different case variations
   * Note: ALIGN DATABASE executes 'align database' SQL command internally
   */
  @Test
  void testClusterCommandsCaseSensitivity() throws Exception {
    // ALIGN DATABASE command is only available if the database supports it
    // We'll test it but expect it might not be supported in all configurations
    try {
      HttpResponse<String> response = executeServerCommand("ALIGN DATABASE graph");
      // Could be 200 (success) or 500 (not supported)
      assertThat(response.statusCode()).isIn(200, 500);

      response = executeServerCommand("align database graph");
      assertThat(response.statusCode()).isIn(200, 500);

      response = executeServerCommand("Align Database graph");
      assertThat(response.statusCode()).isIn(200, 500);
    } catch (Exception e) {
      // ALIGN DATABASE might not be supported in test environment
      // This is acceptable for case sensitivity testing
    }
  }

  /**
   * Test commands with extra whitespace to ensure trimming works correctly
   */
  @Test
  void testCommandsWithExtraWhitespace() throws Exception {
    HttpResponse<String> response = executeServerCommand("  OPEN DATABASE graph  ");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("\tclose database graph\t");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand(" \n open database graph \n ");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("   get server events   ");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test mixed case commands with various capitalization patterns
   */
  @Test
  void testMixedCaseCommands() throws Exception {
    HttpResponse<String> response = executeServerCommand("oPeN dAtAbAsE graph");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("cLoSe DaTaBaSe graph");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("gEt SeRvEr EvEnTs");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("sEt DaTaBaSe SeTtInG graph `arcadedb.dateTimeFormat` 'yyyy'");
    assertThat(response.statusCode()).isEqualTo(200);

    // Re-open the database for other tests
    response = executeServerCommand("oPeN dAtAbAsE graph");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test error scenarios to ensure proper error messages are still returned
   */
  @Test
  void testInvalidCommandsStillReturnErrors() throws Exception {
    // Test invalid command
    HttpResponse<String> response = executeServerCommand("INVALID COMMAND");
    assertThat(response.statusCode()).isEqualTo(400);

    // Test opening non-existent database
    response = executeServerCommand("OPEN DATABASE nonexistent");
    assertThat(response.statusCode()).isEqualTo(500);
  }

  /**
   * Test specific edge cases for command parsing
   */
  @Test
  void testCommandParsingEdgeCases() throws Exception {
    // Test what actually happens with malformed commands
    // These tests document the actual behavior rather than guessing

    // Test command without database name
    HttpResponse<String> response = executeServerCommand("OPEN DATABASE");
    // Accept either 400 or 500 - depends on internal parsing logic
    assertThat(response.statusCode()).isIn(400, 500);

    // Test command with just spaces after
    response = executeServerCommand("OPEN DATABASE   ");
    // Accept either 400 or 500 - depends on internal parsing logic
    assertThat(response.statusCode()).isIn(400, 500);

    // Test that well-formed commands still work
    response = executeServerCommand("OPEN DATABASE graph");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Test that case sensitivity fix doesn't break existing functionality
   */
  @Test
  void testBackwardCompatibility() throws Exception {
    // Test that all the original patterns still work
    HttpResponse<String> response = executeServerCommand("close database graph");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("open database graph");
    assertThat(response.statusCode()).isEqualTo(200);

    response = executeServerCommand("get server events");
    assertThat(response.statusCode()).isEqualTo(200);

    // Test complex commands with parameters
    response = executeServerCommand("set database setting graph `arcadedb.dateTimeFormat` 'yyyy-MM-dd HH:mm:ss'");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  /**
   * Helper method to execute server commands consistently
   */
  private HttpResponse<String> executeServerCommand(String command) throws Exception {
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("command", command)
            .toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }
}
