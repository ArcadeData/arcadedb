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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10692, the Bolt half: a session held across a {@code close database} is refused with {@code DatabaseUnavailable} while
 * nothing serves the name - never with the {@code DatabaseError} a driver reads as a broken server - and serves the reopened
 * database once {@code open database} gives it back, on the same driver and the same session.
 */
public class Issue10692BoltReplacedDatabaseIT extends BaseBoltServerTest {
  private static final String NAME = "boltswap10692";

  private ServerControlPlane controlPlane;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    controlPlane = new ServerControlPlane(getServer(0));
    controlPlane.createDatabase(NAME);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    try {
      final ArcadeDBServer server = getServer(0);
      server.unmarkClosedByCommand(NAME);
      if (server.existsDatabase(NAME))
        server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
      FileUtils.deleteRecursively(
          new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    } finally {
      super.endTest();
    }
  }

  @Test
  void aSessionHeldAcrossACloseAndOpenIsUnavailableThenServedAgain() {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxConnectionPoolSize(1).build());
        final Session session = driver.session(SessionConfig.forDatabase(NAME))) {
      session.run("CREATE (:Swap {i: 1})").consume();
      assertThat(count(session)).isEqualTo(1L);

      controlPlane.closeDatabase(NAME);

      final Throwable refused = catchThrowable(() -> count(session));
      assertThat(refused).isInstanceOf(Neo4jException.class);
      assertThat(((Neo4jException) refused).code()).as("the database is held, the server is not broken")
          .isEqualTo(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR);

      controlPlane.openDatabase(NAME);

      assertThat(count(session)).as("the reopened instance, not the closed one").isEqualTo(1L);
      session.run("CREATE (:Swap {i: 2})").consume();
      assertThat(count(session)).isEqualTo(2L);
    }
  }

  private static long count(final Session session) {
    return session.run("MATCH (n:Swap) RETURN count(n) AS c").single().get("c").asLong();
  }
}
