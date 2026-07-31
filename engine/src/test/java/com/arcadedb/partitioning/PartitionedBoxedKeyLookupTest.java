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
import com.arcadedb.database.Document;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalDocumentType;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5595. {@code PartitionedBucketSelectionStrategy} derives the bucket from {@code Object.hashCode()} on both
 * sides, but the two sides used to see differently boxed objects: placement hashes the value AFTER the schema coerced
 * it to the declared property type, while a lookup hashed whatever the caller passed. {@code Long.hashCode(v)} is
 * {@code (int) (v ^ (v >>> 32))} and {@code Integer.hashCode(v)} is {@code v}, so the two agree only for positive
 * values below 2^31: every negative value, and every value above the int range, pruned to a bucket the record was
 * never placed in and the lookup silently found nothing.
 * <p>
 * The lookup key is now converted to the declared property type before hashing, which is what placement hashes.
 * Placement itself is untouched, so no existing database needs a repartition.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionedBoxedKeyLookupTest extends TestHelper {

  private static final int BUCKETS = 8;

  /** Values chosen so Long and Integer hash codes diverge: only 5 collides by luck. */
  private static final long[] LONG_VALUES = { -5L, -1L, -12345L, 5L, 123L, -2147483648L, 8589934592L };

  private void createLongType(final String typeName, final boolean partitioned) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".id LONG");
      database.command("sql", "CREATE INDEX ON " + typeName + "(id) UNIQUE");
      if (partitioned)
        database.command("sql", "ALTER TYPE " + typeName + " BucketSelectionStrategy `partitioned('id')`");
    });
  }

  private void populateLongType(final String typeName) {
    database.transaction(() -> {
      for (final long value : LONG_VALUES)
        database.newDocument(typeName).set("id", value).save();
    });
  }

  /**
   * The core of the issue: the very same numeric key, handed to the index as an {@code Integer} instead of the
   * {@code Long} the schema stores, must resolve the same record.
   */
  @Test
  void aLongKeyLookedUpAsAnIntegerFindsTheRecord() {
    final String typeName = "PartitionedBoxedLong";
    createLongType(typeName, true);
    populateLongType(typeName);

    final TypeIndex index = database.getSchema().getType(typeName).getPolymorphicIndexByProperties("id");

    database.begin();
    try {
      for (final long value : LONG_VALUES) {
        assertThat(index.get(new Object[] { value }).hasNext())
            .as("record %d found with the natively typed Long key", value).isTrue();

        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
          // OUT OF THE INT RANGE: THERE IS NO NUMERICALLY EQUAL INTEGER TO LOOK IT UP WITH
          continue;

        assertThat(index.get(new Object[] { (int) value }).hasNext())
            .as("record %d found with the numerically equal Integer key", value).isTrue();
      }
    } finally {
      database.rollback();
    }
  }

  /**
   * What the fix actually delivers, which "the record was found" cannot prove: fanning out over every bucket also
   * finds every record, so the tests above would still pass if the strategy regressed to answering -1 unconditionally
   * and never pruned again - a silent performance cliff. This pins the real invariant instead: a lookup key of a
   * different boxed type resolves the SAME bucket placement chose, and still narrows the search to one sub-index.
   */
  @Test
  void anIntegerLookupKeyResolvesTheBucketPlacementChose() {
    final String typeName = "PartitionedBoxedBucketAgreement";
    createLongType(typeName, true);
    populateLongType(typeName);

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
    final PartitionedBucketSelectionStrategy strategy =
        (PartitionedBucketSelectionStrategy) type.getBucketSelectionStrategy();
    final TypeIndex index = type.getPolymorphicIndexByProperties("id");

    database.begin();
    try {
      for (final long value : LONG_VALUES) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
          continue;

        final Document record = index.get(new Object[] { value }).next().asDocument();
        final int placementBucket = strategy.getBucketIdByRecord(record, false);
        final int lookupBucket = strategy.getBucketIdByKeys(List.of("id"), new Object[] { (int) value }, false);

        assertThat(lookupBucket).as("an Integer lookup key for %d must still prune, not decline", value)
            .isNotEqualTo(-1);
        assertThat(lookupBucket).as("Integer lookup for %d must resolve the bucket placement chose", value)
            .isEqualTo(placementBucket);

        assertThat(index.getIndexesByKeys(new Object[] { (int) value }))
            .as("an Integer lookup for %d must still narrow to a single bucket's sub-index", value).hasSize(1);
      }
    } finally {
      database.rollback();
    }
  }

  /**
   * The SQL planner prunes buckets through the same strategy ({@code SelectExecutionPlanner}), and a SQL integer
   * literal small enough to fit an int arrives as an {@code Integer}. This is how the bug surfaces in a plain query.
   */
  @Test
  void aSqlEqualityOnALongPartitionKeyFindsTheRecord() {
    final String typeName = "PartitionedBoxedLongSql";
    createLongType(typeName, true);
    populateLongType(typeName);

    for (final long value : LONG_VALUES) {
      final ResultSet rs = database.query("sql", "SELECT FROM " + typeName + " WHERE id = " + value);
      assertThat(rs.hasNext()).as("SQL equality on id = " + value).isTrue();
    }
  }

  /**
   * Same query through a bound parameter, which the planner cannot early-calculate: it reaches the strategy through
   * the index path instead. The parameter is deliberately an {@code Integer}.
   */
  @Test
  void aSqlParameterOfADifferentBoxedTypeFindsTheRecord() {
    final String typeName = "PartitionedBoxedLongParam";
    createLongType(typeName, true);
    populateLongType(typeName);

    for (final long value : LONG_VALUES) {
      if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
        continue;
      final ResultSet rs = database.query("sql", "SELECT FROM " + typeName + " WHERE id = :id",
          Map.of("id", (int) value));
      assertThat(rs.hasNext()).as("SQL parameter lookup on id = " + value).isTrue();
    }
  }

  /** The mirror case: an INTEGER property looked up with a Long key. */
  @Test
  void anIntegerKeyLookedUpAsALongFindsTheRecord() {
    final String typeName = "PartitionedBoxedInteger";
    final int[] values = { -5, -1, -12345, 5, 123, Integer.MIN_VALUE };

    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".id INTEGER");
      database.command("sql", "CREATE INDEX ON " + typeName + "(id) UNIQUE");
      database.command("sql", "ALTER TYPE " + typeName + " BucketSelectionStrategy `partitioned('id')`");
      for (final int value : values)
        database.newDocument(typeName).set("id", value).save();
    });

    final TypeIndex index = database.getSchema().getType(typeName).getPolymorphicIndexByProperties("id");

    int found = 0;
    database.begin();
    try {
      for (final int value : values)
        if (index.get(new Object[] { (long) value }).hasNext())
          found++;
    } finally {
      database.rollback();
    }

    assertThat(found).as("records found with a Long key against an INTEGER property").isEqualTo(values.length);
  }

  /**
   * A UNIQUE partition index must keep rejecting a duplicate whose key arrives boxed differently: the commit-time
   * duplicate check reads through the same pruned path.
   */
  @Test
  void aUniquePartitionIndexRejectsADuplicateBoxedDifferently() {
    final String typeName = "PartitionedBoxedUnique";
    createLongType(typeName, true);
    populateLongType(typeName);

    int accepted = 0;
    for (final long value : LONG_VALUES) {
      if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
        continue;
      try {
        database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET id = ?", (int) value));
        accepted++;
      } catch (final Exception e) {
        // THE CONSTRAINT HELD
      }
    }

    assertThat(accepted).as("duplicates accepted through a differently boxed key").isZero();
  }

  /**
   * Sibling of the boxed-type mismatch: a {@code COLLATE CI} index treats two spellings as one key, but placement
   * hashed the value in the case the writer used, so the two spellings landed in different buckets. There is no
   * lookup-side normalisation that can reconcile that - the fix is to stop pruning such a partition altogether.
   * <p>
   * The bucket count is deliberately NOT a power of two. Flipping the case of an ASCII letter shifts the Java string
   * hash by a multiple of 32, so with 8 or 16 buckets the two spellings land on the same bucket by arithmetic
   * accident and the defect stays invisible; with 3 they diverge for every value below.
   * <p>
   * The collation is switched on AFTER the strategy is attached, because since issue #5603 asking for this
   * combination outright is refused. Which is the point of keeping this test: the index behind a partition can be
   * dropped and recreated at any time, so the runtime decline still has to hold on its own.
   */
  @Test
  void aCaseInsensitivePartitionIndexStillFindsEverySpelling() {
    final String typeName = "PartitionedCaseInsensitive";
    final String[] values = { "ArcadeDB", "MiXeD CaSe", "Tenant", "Alpha", "Beta", "Gamma" };

    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(3).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".name STRING");
      database.command("sql", "CREATE INDEX ON " + typeName + " (name) UNIQUE");
      database.command("sql", "ALTER TYPE " + typeName + " BucketSelectionStrategy `partitioned('name')`");
      for (final String value : values)
        database.newDocument(typeName).set("name", value).save();
    });

    database.transaction(() -> {
      database.command("sql", "DROP INDEX `" + typeName + "[name]`");
      database.command("sql", "CREATE INDEX ON " + typeName + " (name COLLATE CI) UNIQUE");
    });

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
    final TypeIndex index = type.getPolymorphicIndexByProperties("name");

    // Asserted directly, not just through "the record was found": fanning out is the only correct answer here, and
    // a decline is the only way to get it.
    final PartitionedBucketSelectionStrategy strategy =
        (PartitionedBucketSelectionStrategy) type.getBucketSelectionStrategy();
    assertThat(strategy.getBucketIdByKeys(List.of("name"), new Object[] { values[0] }, false))
        .as("a case-insensitive partition index must decline to prune").isEqualTo(-1);

    int found = 0;
    database.begin();
    try {
      for (final String value : values)
        if (index.get(new Object[] { value.toLowerCase(Locale.ROOT) }).hasNext())
          found++;
    } finally {
      database.rollback();
    }

    assertThat(found).as("records found through the case-insensitive spelling of the partition key")
        .isEqualTo(values.length);
  }

  /** Control: the same schema on the default round-robin strategy, which never prunes. */
  @Test
  void aRoundRobinTypeIsUnaffected() {
    final String typeName = "RoundRobinBoxedLong";
    createLongType(typeName, false);
    populateLongType(typeName);

    final TypeIndex index = database.getSchema().getType(typeName).getPolymorphicIndexByProperties("id");

    int found = 0;
    database.begin();
    try {
      for (final long value : LONG_VALUES) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
          continue;
        if (index.get(new Object[] { (int) value }).hasNext())
          found++;
      }
    } finally {
      database.rollback();
    }

    assertThat(found).as("round-robin never prunes, so every Integer lookup already worked").isEqualTo(6);
  }
}
