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

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedType() {
    PartitioningTestFixture.createPartitionedDocType(database, TYPE_NAME, 4, true);
  }

  private void populate() {
    PartitioningTestFixture.populateDocs(database, TYPE_NAME, true);
  }
}
