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
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8908, over the wire: a scoped {@code CALL (*)} with a {@code db.propertyKeys()} branch, then
 * {@code LOAD CSV}, must run in the engine over Bolt exactly as over HTTP instead of being answered by the
 * procedure interception.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8908CallUnionLoadCsvIT extends BaseBoltServerTest {
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
  void theReportedQueryRunsInTheEngine() throws IOException {
    final File csv = new File("./target/issue8908/arcade-load.csv");
    csv.getParentFile().mkdirs();
    try (final PrintWriter writer = new PrintWriter(csv, StandardCharsets.UTF_8)) {
      writer.println("value");
      writer.println("a");
      writer.println("b");
    }

    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build()); final Session session = driver.session()) {
      final List<Record> rows = session.run("""
          CALL (*) {
            RETURN null AS x
            UNION
            CALL db.propertyKeys() YIELD propertyKey
            RETURN null AS x
          }
          WITH x
          WHERE x IS NULL
          LOAD CSV FROM '%s' AS row
          WITH x, row
          RETURN x
          """.formatted(csv.getAbsolutePath())).list();
      // 3 rows: the header line is a data row too, as there is no WITH HEADERS
      assertThat(rows).hasSize(3);
      assertThat(rows).allMatch(r -> r.get("x").isNull());
    }
  }

  @Test
  void neo4jBrowserConnectProbeIsStillServed() {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build()); final Session session = driver.session()) {
      final List<Record> rows = session.run(
          "CALL dbms.components() YIELD name, versions, edition UNWIND versions AS version RETURN name, version, edition").list();
      assertThat(rows).isNotEmpty();
      assertThat(rows.getFirst().get("name").asString()).isEqualTo("Neo4j Kernel");

      assertThat(session.run("// probe\nCALL db.ping()").list()).hasSize(1);
    }
  }

  @Test
  void aMentionOfAShowCommandInALargerStatementReachesTheEngine() {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build()); final Session session = driver.session()) {
      final List<Record> rows = session.run("RETURN 'show current user dbms.components db.ping' AS s").list();
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().get("s").asString()).isEqualTo("show current user dbms.components db.ping");

      // Shapes the engine now answers for the schema procedures
      assertThat(session.run("CALL db.labels() YIELD label RETURN label ORDER BY label").list()).isNotNull();
    }
  }
}
