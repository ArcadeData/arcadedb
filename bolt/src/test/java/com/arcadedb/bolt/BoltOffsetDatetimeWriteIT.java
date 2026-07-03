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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end regression test for the OffsetDateTime storage gap: the Python/Java Neo4j driver packs a
 * fixed-offset datetime (e.g. {@code datetime.now(timezone.utc)}) as an offset {@code DateTime} struct,
 * which ArcadeDB decodes to a {@code java.time.OffsetDateTime}. Previously the type system did not
 * register {@code OffsetDateTime}, so it was silently dropped on write. This verifies that a native
 * offset datetime sent as a query parameter is stored and read back with the instant preserved - the
 * exact graphiti write path once the driver stops sending ISO strings.
 */
public class BoltOffsetDatetimeWriteIT extends BaseGraphServerTest {

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
  void nativeOffsetDatetimeParameterIsStoredAndReadBack() {
    try (final Driver driver = getDriver()) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // A zoned datetime at a fixed offset: the driver packs this as an offset DateTime struct, which
        // ArcadeDB decodes to OffsetDateTime.
        final ZonedDateTime value = ZonedDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneOffset.ofHours(2));
        session.run("CREATE (e:Ev {id: 1, ts: $ts})", Values.parameters("ts", value)).consume();

        final Record row = session.run("MATCH (e:Ev {id: 1}) RETURN e.ts AS ts").single();

        // Before the fix this was NULL (silently dropped). Now it is stored: ArcadeDB persists a datetime
        // as an instant (zone normalized to UTC) and reads sub-milli-precision datetimes back as a
        // LocalDateTime, so the instant 08:30:45Z is preserved as the UTC wall clock 2024-01-15T08:30:45.
        assertThat(row.get("ts").isNull()).isFalse();
        assertThat(row.get("ts").asLocalDateTime()).isEqualTo(LocalDateTime.of(2024, 1, 15, 8, 30, 45));
      }
    }
  }
}
