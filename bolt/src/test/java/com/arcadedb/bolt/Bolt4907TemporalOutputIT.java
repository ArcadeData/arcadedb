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
import com.arcadedb.server.BaseGraphServerTest;

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
import org.neo4j.driver.types.Node;

import java.time.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wire test for issue #4907: a temporal property returned over Bolt must reach a Neo4j
 * client as a native temporal value (decoded by the driver into {@code java.time}) rather than an
 * ISO-8601 string. Calling {@code Value.asZonedDateTime()} / {@code asLocalDate()} would throw if the
 * value were still a string, so these assertions inherently verify native-struct output.
 */
public class Bolt4907TemporalOutputIT extends BaseGraphServerTest {

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
    return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void temporalPropertiesReturnAsNativeValues() {
    try (final Driver driver = getDriver()) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        session.run("""
            CREATE (e:Ev {id: 1,
              ts: datetime('2024-01-15T10:30:45Z'),
              tsOff: datetime('2024-01-15T10:30:45+02:00'),
              tsZone: datetime('2024-01-15T10:30:45+01:00[Europe/Rome]'),
              day: date('2024-01-15'),
              ldt: localdatetime('2024-01-15T10:30:45'),
              tOff: time('10:30:45+02:00'),
              t: localtime('10:30:45')})""").consume();

        final Record row = session.run(
            """
            MATCH (e:Ev {id: 1}) RETURN e.ts AS ts, e.tsOff AS tsOff, e.tsZone AS tsZone, e.day AS day, \
            e.ldt AS ldt, e.tOff AS tOff, e.t AS t""").single();

        // Would throw if any value came back as a string instead of a native temporal struct.
        assertThat(row.get("ts").asZonedDateTime().toInstant()).isEqualTo(Instant.parse("2024-01-15T10:30:45Z"));
        // Non-zero offset: exercises the offset datetime path against the real driver (not just self round-trip).
        assertThat(row.get("tsOff").asOffsetDateTime())
            .isEqualTo(OffsetDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneOffset.ofHours(2)));
        // Named zone: exercises the DateTimeZoneId path against the real driver.
        final ZonedDateTime tsZone = row.get("tsZone").asZonedDateTime();
        assertThat(tsZone.getZone()).isEqualTo(ZoneId.of("Europe/Rome"));
        assertThat(tsZone.toInstant()).isEqualTo(Instant.parse("2024-01-15T09:30:45Z"));
        assertThat(row.get("day").asLocalDate()).isEqualTo(LocalDate.of(2024, 1, 15));
        assertThat(row.get("ldt").asLocalDateTime()).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 45));
        // Offset time: exercises the Time path.
        assertThat(row.get("tOff").asOffsetTime()).isEqualTo(OffsetTime.of(10, 30, 45, 0, ZoneOffset.ofHours(2)));
        assertThat(row.get("t").asLocalTime()).isEqualTo(LocalTime.of(10, 30, 45));
      }
    }
  }

  @Test
  void nodeElementTemporalPropertyIsNative() {
    try (final Driver driver = getDriver()) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Store natively via a Bolt localdatetime parameter (decoded by #4905) so the value is a real
        // temporal in storage. Returning the whole node (RETURN e) serializes it through the element
        // property-map path (BoltNode.writeTo), which is distinct from the scalar projection path above.
        session.run("CREATE (e:EvNode {id: 1, ts: $ts})",
            Values.parameters("ts", LocalDateTime.of(2024, 1, 15, 10, 30, 45))).consume();

        final Node node = session.run("MATCH (e:EvNode {id: 1}) RETURN e").single().get("e").asNode();

        // Would throw if the node property came back as a string instead of a native temporal struct.
        assertThat(node.get("ts").asLocalDateTime()).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 45));
      }
    }
  }
}
