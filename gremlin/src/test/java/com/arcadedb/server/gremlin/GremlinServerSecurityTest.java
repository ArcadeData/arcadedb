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
package com.arcadedb.server.gremlin;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class GremlinServerSecurityTest extends AbstractGremlinServerIT {

  @Test
  void getAllVertices() {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root", "test")) {
      fail("Expected security exception");
    } catch (final SecurityException e) {
      assertThat(e.getMessage().contains("User/Password")).isTrue();
    }
  }

  @Disabled
  @Test
  void testRCEBlocked_RuntimeExec() {
    // Test that Runtime.getRuntime().exec() is blocked when using Groovy engine
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Enable Groovy engine to test security restrictions
      database.command("sql", "ALTER DATABASE `arcadedb.gremlin.engine` 'groovy'");

      // Attempt RCE via Runtime.exec()
      final String maliciousQuery = "Runtime runtime = Runtime.getRuntime(); runtime.exec(\"touch /tmp/owned\");";

      try {
        database.command("gremlin", maliciousQuery);
        fail("Expected SecurityException or ScriptException when attempting to execute Runtime.exec()");
      } catch (final Exception e) {
        // Should throw exception due to security restrictions
        final String message = e.getMessage();
        assertThat(message).satisfiesAnyOf(
            msg -> assertThat(msg).containsIgnoringCase("Runtime"),
            msg -> assertThat(msg).containsIgnoringCase("not allowed"),
            msg -> assertThat(msg).containsIgnoringCase("blocked"),
            msg -> assertThat(msg).containsIgnoringCase("restricted"),
            msg -> assertThat(msg).containsIgnoringCase("SecurityException"),
            msg -> assertThat(msg).containsIgnoringCase("unable to resolve class")
        );
      }
    }
  }

  @Disabled
  @Test
  void testRCEBlocked_ProcessBuilder() {
    // Test that ProcessBuilder is blocked when using Groovy engine
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Enable Groovy engine to test security restrictions
      database.command("sql", "ALTER DATABASE `arcadedb.gremlin.engine` 'groovy'");

      final String maliciousQuery = "new ProcessBuilder(\"touch\", \"/tmp/owned\").start();";

      try {
        database.command("gremlin", maliciousQuery);
        fail("Expected SecurityException or ScriptException when attempting to use ProcessBuilder");
      } catch (final Exception e) {
        final String message = e.getMessage();
        assertThat(message).satisfiesAnyOf(
            msg -> assertThat(msg).containsIgnoringCase("ProcessBuilder"),
            msg -> assertThat(msg).containsIgnoringCase("not allowed"),
            msg -> assertThat(msg).containsIgnoringCase("blocked"),
            msg -> assertThat(msg).containsIgnoringCase("restricted"),
            msg -> assertThat(msg).containsIgnoringCase("unable to resolve class")
        );
      }
    }
  }

  @Disabled
  @Test
  void testRCEBlocked_FileAccess() {
    // Test that file system access is blocked when using Groovy engine
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Enable Groovy engine to test security restrictions
      database.command("sql", "ALTER DATABASE `arcadedb.gremlin.engine` 'groovy'");

      final String maliciousQuery = "new File(\"/tmp/owned\").createNewFile();";

      try {
        ResultSet result = database.command("gremlin", maliciousQuery);
        fail("Expected SecurityException or ScriptException when attempting file access");
      } catch (final Exception e) {
        final String message = e.getMessage();
        assertThat(message).satisfiesAnyOf(
            msg -> assertThat(msg).containsIgnoringCase("File"),
            msg -> assertThat(msg).containsIgnoringCase("not allowed"),
            msg -> assertThat(msg).containsIgnoringCase("blocked"),
            msg -> assertThat(msg).containsIgnoringCase("restricted"),
            msg -> assertThat(msg).containsIgnoringCase("unable to resolve class")
        );
      }
    }
  }

  @Disabled
  @Test
  void testRCEBlocked_Reflection() {
    // Test that reflection-based security bypass is blocked when using Groovy engine
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Enable Groovy engine to test security restrictions
      database.command("sql", "ALTER DATABASE `arcadedb.gremlin.engine` 'groovy'");

      final String maliciousQuery = "Class.forName(\"java.lang.Runtime\").getMethod(\"getRuntime\").invoke(null);";

      try {
        database.command("gremlin", maliciousQuery);
        fail("Expected SecurityException or ScriptException when attempting reflection");
      } catch (final Exception e) {
        final String message = e.getMessage();
        assertThat(message).satisfiesAnyOf(
            msg -> assertThat(msg).containsIgnoringCase("Method"),
            msg -> assertThat(msg).containsIgnoringCase("Class"),
            msg -> assertThat(msg).containsIgnoringCase("not allowed"),
            msg -> assertThat(msg).containsIgnoringCase("blocked"),
            msg -> assertThat(msg).containsIgnoringCase("restricted"),
            msg -> assertThat(msg).containsIgnoringCase("unable to resolve class")
        );
      }
    }
  }

  @Test
  void testLegitimateGremlinStillWorks() {
    // Verify that legitimate Gremlin queries still work
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      // Create a vertex
      database.command("sql", "CREATE VERTEX TYPE Person123 IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX Person123 SET name = 'Alice', age = 30");

      // Query with Gremlin (should work fine)
      final ResultSet result = database.command("gremlin", "g.V().has('Person123', 'name', 'Alice').values('age')");

      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Integer>getProperty("result")).isEqualTo(30);
    }
  }

}
