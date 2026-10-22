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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10498: a DATETIME string with a non-zero offset ({@code 2020-01-02T03:04:05+02:00}) was stored by
 * {@code Type.convert} as the UTC wall clock of the instant it names ({@code 01:04:05}), but the bulk path ({@link GraphBatch},
 * {@code DateUtils.dateTimeToTimestamp*}) rebased the instant onto the database's configured zone first and then stored that wall
 * clock as if it were UTC, so a database whose zone is not UTC stored a different value for one input string, and one that no
 * other write path (a {@code Date}, an {@code Instant}, an {@code OffsetDateTime}) stores for the same instant.
 * <p>
 * There is one rule now: the instant is stored as the UTC wall clock, whatever the database's zone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10498GraphBatchDateTimeOffsetStringTest extends TestHelper {

  private static final String        INSTANT = "2020-01-02T03:04:05+02:00";
  private static final LocalDateTime UTC     = LocalDateTime.of(2020, 1, 2, 1, 4, 5);

  @Test
  void anEdgeWrittenByTheBatchStoresTheUtcWallClockWhateverTheDatabaseZone() {
    final ZoneId original = database.getSchema().getZoneId();
    try {
      for (final String zone : new String[] { "UTC", "America/Los_Angeles", "Asia/Tokyo", "Europe/Rome" }) {
        database.getSchema().setZoneId(ZoneId.of(zone));
        final String edgeType = "Link_" + zone.replace('/', '_');

        database.transaction(() -> {
          database.getSchema().createVertexType("V_" + edgeType);
          database.getSchema().createEdgeType(edgeType).createProperty("at", Type.DATETIME);
        });

        final RID[] rids = new RID[2];
        database.transaction(() -> {
          rids[0] = database.newVertex("V_" + edgeType).save().getIdentity();
          rids[1] = database.newVertex("V_" + edgeType).save().getIdentity();
        });

        try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
          batch.newEdge(rids[0], edgeType, rids[1], "at", INSTANT);
          batch.newEdge(rids[0], edgeType, rids[1], "at", OffsetDateTime.parse(INSTANT));
        }

        database.transaction(() -> {
          final MutableEdge viaApi = database.newVertex("V_" + edgeType).save().newEdge(edgeType, rids[1], "at", INSTANT);
          assertThat(viaApi.getLocalDateTime("at")).as("Type.convert in zone %s", zone).isEqualTo(UTC);
        });

        database.transaction(() -> {
          int edges = 0;
          for (final Edge edge : database.lookupByRID(rids[0], true).asVertex().getEdges(Vertex.DIRECTION.OUT, edgeType)) {
            assertThat(edge.getLocalDateTime("at")).as("bulk path in zone %s", zone).isEqualTo(UTC);
            ++edges;
          }
          assertThat(edges).isEqualTo(2);
        });
      }
    } finally {
      database.getSchema().setZoneId(original);
    }
  }

  @Test
  void theTimestampOfAStringIsTheOneOfTheSameInstantTypedAsAnOffsetDateTime() {
    final ZoneId original = database.getSchema().getZoneId();
    try {
      for (final String zone : new String[] { "UTC", "America/Los_Angeles", "Asia/Tokyo" }) {
        database.getSchema().setZoneId(ZoneId.of(zone));
        for (final ChronoUnit precision : new ChronoUnit[] { ChronoUnit.SECONDS, ChronoUnit.MILLIS, ChronoUnit.MICROS,
            ChronoUnit.NANOS }) {
          final Long typed = DateUtils.dateTimeToTimestamp(database, OffsetDateTime.parse(INSTANT), precision);
          assertThat(DateUtils.dateTimeToTimestamp(database, INSTANT, precision)).as("%s in %s", precision, zone).isEqualTo(typed);
          assertThat(DateUtils.dateTimeToTimestampExact(database, INSTANT, precision)).as("exact %s in %s", precision, zone)
              .isEqualTo(typed);
        }
      }
    } finally {
      database.getSchema().setZoneId(original);
    }
  }

  @Test
  void anInputWithoutOffsetOrInUtcKeepsItsWallClockInEveryZone() {
    final ZoneId original = database.getSchema().getZoneId();
    try {
      database.getSchema().setZoneId(ZoneId.of("Asia/Tokyo"));
      final long wallClock = LocalDateTime.of(2020, 1, 2, 3, 4, 5).toEpochSecond(ZoneOffset.UTC);
      for (final String literal : new String[] { "2020-01-02T03:04:05", "2020-01-02T03:04:05Z", "2020-01-02T03:04:05+00:00",
          "2020-01-02 03:04:05" })
        assertThat(DateUtils.dateTimeToTimestamp(database, literal, ChronoUnit.SECONDS)).as(literal).isEqualTo(wallClock);
    } finally {
      database.getSchema().setZoneId(original);
    }
  }
}
