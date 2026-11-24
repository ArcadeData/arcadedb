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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.partitioning.PartitioningTestFixture;
import com.arcadedb.schema.LocalDocumentType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the atomic-DDL syntax `ALTER TYPE Doc BUCKET +newBucket WITH repartition = true`
 * (issue #4087): the bucket addition + repartition rebuild run as one logical operation, the
 * needsRepartition flag never goes true to the caller, and the result row carries the moved
 * record count.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlterTypeAtomicRepartitionTest extends TestHelper {

  private static final String TYPE_NAME = "PartDoc";

  @Test
  void alterTypeAddBucketWithRepartitionClearsTheFlagAtomically() {
    createPartitionedType();
    populate();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(type.isNeedsRepartition()).isFalse();

    final ResultSet rs = database.command("sql",
        "ALTER TYPE " + TYPE_NAME + " BUCKET +" + TYPE_NAME + "_extra WITH repartition = true");
    final Result row = rs.next();
    rs.close();

    assertThat(type.isNeedsRepartition())
        .as("the trailing repartition setting must clear the flag in the same statement")
        .isFalse();
    assertThat(row.<String>getProperty("addBucket")).isEqualTo(TYPE_NAME + "_extra");
    assertThat(row.<Long>getProperty("repartitionVisited")).isEqualTo(4L);
    // 4 records hashed under modulus 4, then re-checked under modulus 5: at least one tenant
    // must shift to a new bucket (otherwise the test isn't exercising the move path), and at
    // most all four shift. Bake a tight range rather than the exact JDK-hash-dependent value.
    final Long moved = row.<Long>getProperty("repartitionMoved");
    assertThat(moved).isNotNull();
    assertThat(moved).isBetween(1L, 4L);

    // Every tenant must still be findable after the atomic ALTER+REBUILD.
    for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella" }) {
      final ResultSet check = database.query("sql",
          "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = '" + tenant + "'");
      assertThat(check.hasNext()).as("tenant '" + tenant + "' must remain findable").isTrue();
      assertThat(check.next().<String>getProperty("tenant_id")).isEqualTo(tenant);
      check.close();
    }
  }

  @Test
  void alterTypeAddBucketWithoutSettingLeavesFlagTrue() {
    createPartitionedType();
    populate();

    database.command("sql",
        "ALTER TYPE " + TYPE_NAME + " BUCKET +" + TYPE_NAME + "_extra2").close();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    try {
      assertThat(type.isNeedsRepartition())
          .as("plain ADD BUCKET without the repartition setting must leave the stale flag true")
          .isTrue();
    } finally {
      // Clean up even on assertion failure so a regression here doesn't poison the next test
      // with a stale-flagged shared schema (TestHelper drops between tests, but reset here is
      // cheap and matches the pattern used by the Cypher tests).
      type.setNeedsRepartition(false);
    }
  }

  @Test
  void unknownSettingIsRejected() {
    createPartitionedType();
    assertThatThrownBy(() -> database.command("sql",
        "ALTER TYPE " + TYPE_NAME + " BUCKET +" + TYPE_NAME + "_b WITH foo = 'bar'"))
        .hasMessageContaining("Unrecognized setting")
        .hasMessageContaining("repartition");
  }

  @Test
  void repartitionMustBeBoolean() {
    createPartitionedType();
    assertThatThrownBy(() -> database.command("sql",
        "ALTER TYPE " + TYPE_NAME + " BUCKET +" + TYPE_NAME + "_b WITH repartition = 42"))
        .hasMessageContaining("repartition")
        .hasMessageContaining("true or false");
  }

  /**
   * Regression for issue #12033: on a vertex or edge type the chained rebuild refuses a repartition, but the strategy and the bucket the
   * statement's items set had already been applied behind the refusal.
   */
  @Test
  void aRefusedRepartitionOfAVertexOrEdgeTypeChangesNothing() {
    database.command("sql", "CREATE VERTEX TYPE PartV BUCKETS 4");
    database.command("sql", "CREATE PROPERTY PartV.k INTEGER");
    database.command("sql", "CREATE INDEX ON PartV (k) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE PartE BUCKETS 4");
    database.command("sql", "CREATE PROPERTY PartE.k INTEGER");
    database.command("sql", "CREATE INDEX ON PartE (k) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 8; i++)
        database.command("sql", "INSERT INTO PartV SET k = ?", i);
    });

    for (final String typeName : new String[] { "PartV", "PartE" }) {
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
      final int buckets = type.getBuckets(false).size();

      for (final String alter : new String[] { "BUCKETSELECTIONSTRATEGY `thread`", "BUCKETSELECTIONSTRATEGY `partitioned('k')`",
          "BUCKET +" + typeName + "_extra" }) {
        assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + typeName + " " + alter + " WITH repartition = true"))
            .as(alter)
            .hasMessageContaining("with repartition = true is not supported");

        assertThat(type.getBucketSelectionStrategy().getName()).as(alter).isEqualTo("round-robin");
        assertThat(type.isNeedsRepartition()).as(alter).isFalse();
        assertThat(type.getBuckets(false)).as(alter).hasSize(buckets);
      }
    }
    assertThat(database.getSchema().existsBucket("PartV_extra")).isFalse();
  }

  /** The other refusals of the trailing settings come before the items too (issue #12033). */
  @Test
  void aRefusedSettingLeavesTheItemsUnapplied() {
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    final int buckets = type.getBuckets(false).size();

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + TYPE_NAME + " BUCKET +" + TYPE_NAME + "_u WITH foo = 'bar'"))
        .hasMessageContaining("Unrecognized setting");
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + TYPE_NAME + " BUCKET +" + TYPE_NAME + "_l WITH lightweight = true"))
        .hasMessageContaining("apply to edge types only");

    assertThat(type.getBuckets(false)).hasSize(buckets);
    assertThat(database.getSchema().existsBucket(TYPE_NAME + "_u")).isFalse();
    assertThat(database.getSchema().existsBucket(TYPE_NAME + "_l")).isFalse();
  }

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedType() {
    PartitioningTestFixture.createPartitionedDocType(database, TYPE_NAME, 4, true);
  }

  private void populate() {
    PartitioningTestFixture.populateDocs(database, TYPE_NAME, true);
  }
}
