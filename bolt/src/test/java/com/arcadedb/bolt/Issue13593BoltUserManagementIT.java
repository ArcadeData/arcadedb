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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #13593 (#13539, #13540): {@code CREATE OR REPLACE USER} replaces the user instead of failing as a plain
 * {@code CREATE USER} does, a duplicate {@code CREATE USER} is the caller's mistake (Neo4j's {@code ArgumentError}, not a server
 * fault), and the password of {@code CREATE USER} / {@code ALTER USER} may be a parameter, so a client does not have to build the
 * statement with the secret in its text. Through the Neo4j Java driver, as a client sees it; every code and message is what Neo4j
 * 5.26 answers for the same statement.
 */
public class Issue13593BoltUserManagementIT extends BaseBoltServerTest {
  private static final String USER = "u13593";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      admin("DROP USER " + USER + " IF EXISTS", Map.of());
    } finally {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      super.endTest();
    }
  }

  @Test
  void createOrReplaceUserReplacesTheUser() {
    admin("CREATE OR REPLACE USER " + USER + " SET PASSWORD 'first-pw-1' CHANGE NOT REQUIRED", Map.of());
    assertThat(canLogIn("first-pw-1")).isTrue();

    admin("CREATE OR REPLACE USER " + USER + " SET PASSWORD 'second-pw-2' CHANGE NOT REQUIRED", Map.of());
    assertThat(canLogIn("second-pw-2")).isTrue();
    assertThat(canLogIn("first-pw-1")).isFalse();

    // IF NOT EXISTS leaves the replaced user as it is
    admin("CREATE USER " + USER + " IF NOT EXISTS SET PASSWORD 'third-pw-3' CHANGE NOT REQUIRED", Map.of());
    assertThat(canLogIn("second-pw-2")).isTrue();
  }

  @Test
  void createOrReplaceUserWithIfNotExistsIsRefused() {
    final Neo4jException error = failure("CREATE OR REPLACE USER " + USER + " IF NOT EXISTS SET PASSWORD 'abcdefgh1'", Map.of());
    assertThat(error.code()).isEqualTo("Neo.ClientError.Statement.SyntaxError");
    assertThat(error.getMessage()).contains("cannot have both `OR REPLACE` and `IF NOT EXISTS`");
  }

  @Test
  void aDuplicateCreateUserIsTheCallersMistake() {
    admin("CREATE USER " + USER + " SET PASSWORD 'abcdefgh1' CHANGE NOT REQUIRED", Map.of());

    final Neo4jException error = failure("CREATE USER " + USER + " SET PASSWORD 'abcdefgh2' CHANGE NOT REQUIRED", Map.of());
    assertThat(error.code()).isEqualTo("Neo.ClientError.Statement.ArgumentError");
    assertThat(error.getMessage()).isEqualTo("Failed to create the specified user '" + USER + "': User already exists.");
    // the failed statement changed nothing
    assertThat(canLogIn("abcdefgh1")).isTrue();
  }

  @Test
  void theAdministratorIsNotReplaced() {
    final Neo4jException error = failure("CREATE OR REPLACE USER root SET PASSWORD 'abcdefgh1' CHANGE NOT REQUIRED", Map.of());
    assertThat(error.code()).isEqualTo("Neo.ClientError.Statement.ArgumentError");
    assertThat(error.getMessage()).contains("Cannot delete the root user");
    assertThat(canLogIn("root", DEFAULT_PASSWORD_FOR_TESTS)).isTrue();
  }

  @Test
  void thePasswordOfCreateUserAndAlterUserMayBeAParameter() {
    admin("CREATE USER " + USER + " SET PASSWORD $pw CHANGE NOT REQUIRED", Map.of("pw", "s3cretpw!"));
    assertThat(canLogIn("s3cretpw!")).isTrue();

    admin("ALTER USER " + USER + " SET PASSWORD $pw", Map.of("pw", "another1!"));
    assertThat(canLogIn("another1!")).isTrue();
    assertThat(canLogIn("s3cretpw!")).isFalse();

    admin("CREATE OR REPLACE USER " + USER + " SET PASSWORD $pw CHANGE NOT REQUIRED", Map.of("pw", "replaced-3"));
    assertThat(canLogIn("replaced-3")).isTrue();
  }

  @Test
  void aPasswordParameterThatIsNotBoundOrNotAStringIsRefusedWithoutTheValue() {
    Neo4jException error = failure("CREATE USER " + USER + " SET PASSWORD $pw CHANGE NOT REQUIRED", Map.of());
    assertThat(error.code()).isEqualTo("Neo.ClientError.Statement.ParameterMissing");
    assertThat(error.getMessage()).contains("pw");

    error = failure("CREATE USER " + USER + " SET PASSWORD $pw CHANGE NOT REQUIRED", Map.of("pw", 12345678));
    assertThat(error.code()).isEqualTo("Neo.ClientError.Statement.TypeError");
    assertThat(error.getMessage()).contains("$pw").doesNotContain("12345678");

    // nothing was created by the refused statements
    assertThat(canLogIn("12345678")).isFalse();
  }

  private void admin(final String statement, final Map<String, Object> parameters) {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run(statement, parameters).consume();
    }
  }

  private Neo4jException failure(final String statement, final Map<String, Object> parameters) {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final Neo4jException error = catchThrowableOfType(Neo4jException.class, () -> session.run(statement, parameters).consume());
      assertThat(error).as(statement).isNotNull();
      return error;
    }
  }

  private boolean canLogIn(final String password) {
    return canLogIn(USER, password);
  }

  private boolean canLogIn(final String user, final String password) {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic(user, password),
        Config.builder().withoutEncryption().build()); final Session session = session(driver)) {
      session.run("RETURN 1").consume();
      return true;
    } catch (final Neo4jException e) {
      return false;
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
