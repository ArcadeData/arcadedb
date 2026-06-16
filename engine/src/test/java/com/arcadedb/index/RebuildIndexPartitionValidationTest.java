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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalDocumentType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins issue #832: {@code REBUILD INDEX} on a type with a {@code partitioned} bucket selection
 * strategy must validate that each record sits in the bucket its key hashes to. When records are
 * misplaced (e.g. inserted under round-robin before the strategy was switched to partitioned, or
 * relocated by direct bucket manipulation / import), partition-aware index pruning at query time
 * would only search the hash-target bucket and miss them. The rebuild detects the mismatch,
 * reports the count, and raises {@code needsRepartition} so the planner fans out and queries stay
 * correct until {@code REBUILD TYPE ... WITH repartition = true} relocates the records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildIndexPartitionValidationTest extends TestHelper {

  private static final String TYPE_NAME = "Tenant";
  private static final String INDEX_NAME = TYPE_NAME + "[tenant_id]";

  @Test
  void rebuildIndexDetectsMisplacedRecordsAndFlagsRepartition() {
    createRoundRobinTypeWithUniqueIndex();
    populate();

    // Switch to partitioned. This is the case-2 scenario from the issue: the existing records were
    // placed round-robin, so most now sit in the wrong hash-target bucket.
    database.command("sql", "ALTER TYPE " + TYPE_NAME + " BucketSelectionStrategy `partitioned('tenant_id')`");

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);

    // Clear the flag to simulate a database where the schema-mutation hook never ran (older DB,
    // direct import, manual bucket moves). The rebuild must re-detect the misplacement on its own.
    type.setNeedsRepartition(false);
    assertThat(type.isNeedsRepartition()).isFalse();

    final long misplaced = rebuildIndex();

    assertThat(misplaced).as("rebuild must report the misplaced records").isGreaterThan(0L);
    assertThat(type.isNeedsRepartition())
        .as("detecting misplaced records during rebuild must raise needsRepartition")
        .isTrue();

    // With the flag raised the planner fans out across all buckets, so every tenant stays findable.
    for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella", "wayne", "stark", "oscorp" }) {
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = '" + tenant + "'")) {
        assertThat(rs.hasNext()).as("tenant '" + tenant + "' must remain findable after rebuild").isTrue();
        assertThat(rs.next().<String>getProperty("tenant_id")).isEqualTo(tenant);
      }
    }
  }

  @Test
  void rebuildIndexOnCorrectlyPartitionedTypeReportsZeroMisplaced() {
    // Built partitioned from the start: every record was routed by the hash, so nothing is
    // misplaced and the rebuild must leave the flag untouched.
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(8).create();
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tenant_id STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + "(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE " + TYPE_NAME + " BucketSelectionStrategy `partitioned('tenant_id')`");
    });
    populate();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(type.isNeedsRepartition()).isFalse();

    final long misplaced = rebuildIndex();

    assertThat(misplaced).as("correctly partitioned records have nothing misplaced").isEqualTo(0L);
    assertThat(type.isNeedsRepartition())
        .as("a clean rebuild must not raise needsRepartition")
        .isFalse();
  }

  // ---- helpers ------------------------------------------------------------

  private void createRoundRobinTypeWithUniqueIndex() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(8).create();
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tenant_id STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + "(tenant_id) UNIQUE");
    });
  }

  private void populate() {
    database.transaction(() -> {
      for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella", "wayne", "stark", "oscorp" })
        database.command("sql", "INSERT INTO " + TYPE_NAME + " SET tenant_id = '" + tenant + "'");
    });
  }

  private long rebuildIndex() {
    try (final ResultSet rs = database.command("sql", "REBUILD INDEX `" + INDEX_NAME + "`")) {
      final Result row = rs.next();
      final Long misplaced = row.getProperty("recordsMisplaced");
      return misplaced == null ? 0L : misplaced;
    }
  }
}
