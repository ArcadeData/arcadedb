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

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11381: a RUN or BEGIN that names no database must run on the connection's default database, not on the one an
 * earlier request on the same pooled connection named.
 */
public class Issue11381BoltStickyDatabaseIT extends BaseBoltServerTest {
  private static final String NAME = "boltother11381";

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
    getServer(0).getConfiguration().setValue(GlobalConfiguration.BOLT_DEFAULT_DATABASE, getDatabaseName());
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    try {
      final ArcadeDBServer server = getServer(0);
      server.getConfiguration().setValue(GlobalConfiguration.BOLT_DEFAULT_DATABASE, null);
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
  void aRunNamingNoDatabaseFollowsTheDefaultNotThePreviousRequest() {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxConnectionPoolSize(1).build())) {
      try (final Session other = driver.session(SessionConfig.forDatabase(NAME))) {
        other.run("CREATE (:FromSessionA)").consume();
      }
      try (final Session dflt = driver.session()) {
        dflt.run("CREATE (:FromSessionB)").consume();
        assertThat(dflt.run("MATCH (n:FromSessionA) RETURN count(n) AS c").single().get("c").asLong())
            .as("the default database does not hold what session A wrote to the other one").isZero();
        assertThat(dflt.run("MATCH (n:FromSessionB) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(1L);
      }
      try (final Session other = driver.session(SessionConfig.forDatabase(NAME))) {
        assertThat(other.run("MATCH (n:FromSessionB) RETURN count(n) AS c").single().get("c").asLong())
            .as("session B's write must not land in the other database").isZero();
        assertThat(other.run("MATCH (n:FromSessionA) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(1L);
      }
      try (final Session dflt = driver.session()) {
        try (final var tx = dflt.beginTransaction()) {
          assertThat(tx.run("MATCH (n:FromSessionA) RETURN count(n) AS c").single().get("c").asLong())
              .as("BEGIN with no database also resolves to the default").isZero();
        }
      }
    }
  }
}
