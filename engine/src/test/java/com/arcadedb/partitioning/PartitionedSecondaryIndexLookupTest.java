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
package com.arcadedb.partitioning;

import com.arcadedb.TestHelper;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5589. A type using {@code PartitionedBucketSelectionStrategy} places a record in the bucket its
 * PARTITION key hashes to, but {@link TypeIndex#getIndexesByKeys} used to prune every lookup to the bucket
 * the LOOKUP key hashes to. For any index other than the partition one those are unrelated, so the pruned
 * search read a bucket the record was not in: the lookup found nothing, and a secondary UNIQUE index stopped
 * enforcing its constraint because the commit-time duplicate check reads through the same path.
 * <p>
 * The same mismatch hit a PARTIAL key on a composite partition index: placement hashes every partition
 * property, a partial lookup hashes only the values it was given, so the two disagree.
 * <p>
 * Pruning must therefore happen only when the lookup covers exactly the partition properties. These tests
 * pin both the correctness of the fallback and the survival of the optimisation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionedSecondaryIndexLookupTest extends TestHelper {

  private static final String TYPE_NAME = "PartitionedSecondary";
  private static final int    BUCKETS   = 8;
  private static final int    TOTAL     = 200;

  private void createType(final boolean partitioned) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tenant_id STRING");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".code STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + "(tenant_id) UNIQUE");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + "(code) UNIQUE");
      if (partitioned)
        database.command("sql", "ALTER TYPE " + TYPE_NAME + " BucketSelectionStrategy `partitioned('tenant_id')`");
    });
  }

  private void populate() {
    database.transaction(() -> {
      for (int i = 0; i < TOTAL; i++)
        database.command("sql", "INSERT INTO " + TYPE_NAME + " SET tenant_id = 't-" + i + "', code = 'c-" + i + "'");
    });
  }

  private TypeIndex indexOn(final String property) {
    return database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties(property);
  }

  @Test
  void secondaryIndexLookupFindsEveryRowOnAPartitionedType() {
    assertLookupFindsEveryRow(true);
  }

  /** Control: the same schema on the default round-robin strategy, which never prunes. */
  @Test
  void secondaryIndexLookupFindsEveryRowOnARoundRobinType() {
    assertLookupFindsEveryRow(false);
  }

  private void assertLookupFindsEveryRow(final boolean partitioned) {
    createType(partitioned);
    populate();

    assertThat(database.countType(TYPE_NAME, false)).as("rows actually stored").isEqualTo(TOTAL);

    // Straight through the index API, which is also what the commit-time duplicate check uses.
    final TypeIndex codeIndex = indexOn("code");
    assertThat(codeIndex).isNotNull();

    int foundViaIndex = 0;
    int foundViaSql = 0;
    database.begin();
    try {
      for (int i = 0; i < TOTAL; i++) {
        final IndexCursor cursor = codeIndex.get(new Object[] { "c-" + i });
        if (cursor.hasNext())
          foundViaIndex++;

        final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE code = 'c-" + i + "'");
        if (rs.hasNext())
          foundViaSql++;
      }
    } finally {
      database.rollback();
    }

    assertThat(foundViaIndex).as("rows found by a secondary-index lookup").isEqualTo(TOTAL);
    assertThat(foundViaSql).as("rows found by a SQL equality on the secondary index").isEqualTo(TOTAL);
  }

  @Test
  void secondaryUniqueIndexStillRejectsDuplicatesOnAPartitionedType() {
    createType(true);

    database.transaction(
        () -> database.command("sql", "INSERT INTO " + TYPE_NAME + " SET tenant_id = 't-1', code = 'SAME'"));

    int accepted = 0;
    for (int i = 2; i < 40; i++) {
      final int n = i;
      try {
        database.transaction(
            () -> database.command("sql", "INSERT INTO " + TYPE_NAME + " SET tenant_id = 't-" + n + "', code = 'SAME'"));
        accepted++;
      } catch (final DuplicatedKeyException e) {
        // THE CONSTRAINT HELD
      }
    }

    assertThat(accepted).as("rows accepted that violate the UNIQUE index on code").isZero();
  }

  /**
   * The optimisation must survive the fix: a lookup on the PARTITION index still narrows to a single
   * sub-index, otherwise the guard would have simply disabled partition pruning everywhere.
   */
  @Test
  void partitionIndexLookupStillPrunesToOneSubIndex() {
    createType(true);
    populate();

    final TypeIndex tenantIndex = indexOn("tenant_id");
    final List<? extends Index> pruned = tenantIndex.getIndexesByKeys(new Object[] { "t-7" });

    assertThat(pruned).as("a lookup on the partition key must prune to a single bucket's sub-index").hasSize(1);
  }

  /** A lookup on any other index cannot be pruned, so it has to fan out over every bucket. */
  @Test
  void secondaryIndexLookupFansOutAcrossEveryBucket() {
    createType(true);
    populate();

    final TypeIndex codeIndex = indexOn("code");
    final List<? extends Index> fannedOut = codeIndex.getIndexesByKeys(new Object[] { "c-7" });

    assertThat(fannedOut).as("a lookup on a non-partition index must search every bucket's sub-index")
        .hasSize(BUCKETS);
  }

  /**
   * The property check is multiset equality, so a repeated property cannot stand in for a missing one: hashing
   * {@code [org, org]} sums a different pair of values than placement did. No index declares a repeated property,
   * so this is asserted straight against the strategy rather than through a query.
   */
  @Test
  void aRepeatedLookupPropertyDoesNotPassForACompositePartition() {
    final String typeName = "PartitionedCompositeDup";
    createCompositeType(typeName);

    final PartitionedBucketSelectionStrategy strategy =
        (PartitionedBucketSelectionStrategy) database.getSchema().getType(typeName).getBucketSelectionStrategy();

    assertThat(strategy.getBucketIdByKeys(List.of("org", "region"), new Object[] { "o-1", "r-1" }, false))
        .as("the partition properties themselves must still resolve a bucket").isNotNegative();
    assertThat(strategy.getBucketIdByKeys(List.of("region", "org"), new Object[] { "r-1", "o-1" }, false))
        .as("a permutation hashes the same sum, so it must still resolve a bucket").isNotNegative();
    assertThat(strategy.getBucketIdByKeys(List.of("org", "org"), new Object[] { "o-1", "o-1" }, false))
        .as("a repeated property is not the partition set and must decline").isEqualTo(-1);
    assertThat(strategy.getBucketIdByKeys(null, new Object[] { "o-1", "r-1" }, false))
        .as("unverifiable properties must decline").isEqualTo(-1);
  }

  /**
   * Composite partition key. The full key must still prune and still find every row; a partial key is rejected
   * by the index contract itself, which is what keeps the "hash fewer values than placement did" case from ever
   * reaching the strategy through this entry point.
   */
  private void createCompositeType(final String typeName) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".org STRING");
      database.command("sql", "CREATE PROPERTY " + typeName + ".region STRING");
      database.command("sql", "CREATE INDEX ON " + typeName + "(org,region) UNIQUE");
      database.command("sql", "ALTER TYPE " + typeName + " BucketSelectionStrategy `partitioned('org','region')`");
    });
  }

  @Test
  void compositePartitionKeyLookupFindsTheRows() {
    final String typeName = "PartitionedComposite";
    createCompositeType(typeName);

    database.transaction(() -> {
      for (int i = 0; i < TOTAL; i++)
        database.command("sql", "INSERT INTO " + typeName + " SET org = 'o-" + i + "', region = 'r-" + i + "'");
    });

    final TypeIndex idx = database.getSchema().getType(typeName).getPolymorphicIndexByProperties("org", "region");

    int foundFull = 0;
    database.begin();
    try {
      for (int i = 0; i < TOTAL; i++)
        if (idx.get(new Object[] { "o-" + i, "r-" + i }).hasNext())
          foundFull++;
    } finally {
      database.rollback();
    }

    assertThat(foundFull).as("rows found by a FULL composite partition-key lookup").isEqualTo(TOTAL);

    // The full key covers the partition properties, so pruning still applies.
    assertThat(idx.getIndexesByKeys(new Object[] { "o-7", "r-7" }))
        .as("a full composite partition-key lookup must prune to a single bucket's sub-index").hasSize(1);

    // A partial key never reaches the strategy: the index contract rejects it first.
    assertThatThrownBy(() -> idx.get(new Object[] { "o-7" })).isInstanceOf(IllegalArgumentException.class);
  }
}
