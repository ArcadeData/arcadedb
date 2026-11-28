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
import org.neo4j.driver.exceptions.ClientException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The failure of an {@code apoc.util.validate} guard as Neo4j 5.26 with APOC answers it over Bolt: code
 * {@code Neo.ClientError.Procedure.ProcedureCallFailed} and the text {@code Failed to invoke procedure `apoc.util.validate`: Caused by:
 * java.lang.RuntimeException: <message>} ({@code function} and {@code apoc.util.validatePredicate} for the function form), which
 * {@code @neo4j/graphql} decodes to tell an authorization failure from any other.
 */
public class BoltProcedureCallFailedIT extends BaseBoltServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void theProcedureFormFailsAsAFailedProcedureCall() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final ClientException error = catchThrowableOfType(ClientException.class,
          () -> session.run("CALL apoc.util.validate(true, '@neo4j/graphql/FORBIDDEN', [])").consume());
      assertThat(error.code()).isEqualTo("Neo.ClientError.Procedure.ProcedureCallFailed");
      assertThat(error.getMessage()).isEqualTo(
          "Failed to invoke procedure `apoc.util.validate`: Caused by: java.lang.RuntimeException: @neo4j/graphql/FORBIDDEN");

      final ClientException formatted = catchThrowableOfType(ClientException.class,
          () -> session.run("UNWIND [1] AS x WITH * CALL apoc.util.validate(x = 1, 'msg %d', [x]) RETURN x").list());
      assertThat(formatted.code()).isEqualTo("Neo.ClientError.Procedure.ProcedureCallFailed");
      assertThat(formatted.getMessage()).endsWith("java.lang.RuntimeException: msg 1");
    }
  }

  @Test
  void theFunctionFormFailsAsAFailedFunctionCall() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final ClientException error = catchThrowableOfType(ClientException.class,
          () -> session.run("RETURN apoc.util.validatePredicate(true, '@neo4j/graphql/FORBIDDEN', [0])").consume());
      assertThat(error.code()).isEqualTo("Neo.ClientError.Procedure.ProcedureCallFailed");
      assertThat(error.getMessage()).isEqualTo(
          "Failed to invoke function `apoc.util.validatePredicate`: Caused by: java.lang.RuntimeException: @neo4j/graphql/FORBIDDEN");
    }
  }

  @Test
  void aGuardThatDoesNotFireAnswersTrue() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(session.run("RETURN apoc.util.validatePredicate(false, 'x', [0]) AS ok").single().get("ok").asBoolean()).isTrue();
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
