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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Values;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end regression test for the OffsetDateTime storage gap: the Python/Java Neo4j driver packs a
 * fixed-offset datetime (e.g. {@code datetime.now(timezone.utc)}) as an offset {@code DateTime} struct,
 * which ArcadeDB decodes to a {@code java.time.ZonedDateTime} at that offset (#12778). Previously the type system did not
 * register the decoded value, so it was silently dropped on write. This verifies that a native
 * offset datetime sent as a query parameter is stored and read back with the instant preserved - the
 * exact graphiti write path once the driver stops sending ISO strings.
 */
public class BoltOffsetDatetimeWriteIT extends BaseBoltServerTest {

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

  private Driver getDriver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void nativeOffsetDatetimeParameterIsStoredAndReadBack() {
    try (final Driver driver = getDriver()) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // A zoned datetime at a fixed offset: the driver packs this as an offset DateTime struct, which
        // ArcadeDB decodes to a ZonedDateTime at the offset.
        final ZonedDateTime value = ZonedDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneOffset.ofHours(2));
        session.run("CREATE (e:Ev {id: 1, ts: $ts})", Values.parameters("ts", value)).consume();

        final Record row = session.run("MATCH (e:Ev {id: 1}) RETURN e.ts AS ts").single();

        // Stored with the instant and, since #12778, the offset too (a schemaless property keeps a zoned datetime).
        assertThat(row.get("ts").isNull()).isFalse();
        assertThat(row.get("ts").asZonedDateTime()).isEqualTo(value);
      }
    }
  }

  /**
   * #12778: the Java driver packs an OffsetDateTime as the offset DateTime struct; a schemaless property must keep
   * the offset as a ZONED DATETIME, like a zone-id struct or Cypher {@code datetime('...+05:30')}.
   */
  @Test
  void offsetDatetimeParameterKeepsItsOffsetInASchemalessProperty() {
    final OffsetDateTime value = OffsetDateTime.of(2024, 10, 16, 16, 25, 37, 0, ZoneOffset.ofHoursMinutes(5, 30));
    try (final Driver driver = getDriver()) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        session.run("CREATE (n:Off12778 {id: 1, p: $p})", Values.parameters("p", value)).consume();
      }
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Record row = session.run("MATCH (n:Off12778 {id: 1}) RETURN n.p AS p, valueType(n.p) AS t").single();
        assertThat(row.get("p").asOffsetDateTime()).isEqualTo(value);
        assertThat(row.get("p").asZonedDateTime().getZone()).isEqualTo(ZoneOffset.ofHoursMinutes(5, 30));
        assertThat(row.get("t").asString()).isEqualTo("ZONED DATETIME NOT NULL");
      }
    }
  }

  /**
   * #12778: a property declared DATETIME keeps storing the UTC instant (#10489), whatever the decoded class.
   */
  @Test
  void offsetDatetimeParameterIntoDeclaredDatetimePropertyStoresUtc() {
    getServerDatabase(0, getDatabaseName()).getSchema().createVertexType("Decl12778").createProperty("p", Type.DATETIME);
    final OffsetDateTime value = OffsetDateTime.of(2024, 10, 16, 16, 25, 37, 0, ZoneOffset.ofHoursMinutes(5, 30));
    try (final Driver driver = getDriver()) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        session.run("CREATE (n:Decl12778 {id: 1, p: $p})", Values.parameters("p", value)).consume();
      }
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Record row = session.run("MATCH (n:Decl12778 {id: 1}) RETURN n.p AS p").single();
        assertThat(row.get("p").asLocalDateTime()).isEqualTo(LocalDateTime.of(2024, 10, 16, 10, 55, 37));
      }
    }
  }
}
