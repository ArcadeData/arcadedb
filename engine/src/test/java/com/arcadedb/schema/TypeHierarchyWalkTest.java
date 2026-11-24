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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The walks over the type hierarchy visit each type once, whatever the number of paths that lead to it: every type of a deep lattice,
 * where each type extends the two types of the level above, is linked, loaded, asked and written in the time it takes to visit it.
 * <p>
 * The {@link Timeout} is a hang detector, in a separate thread so that a regression is reported instead of waited for.
 */
class TypeHierarchyWalkTest extends TestHelper {
  private static final int LEVELS            = 30;
  private static final int HANG_DETECTOR_SEC = 120;

  private static String name(final int level, final char side) {
    return "T" + level + side;
  }

  /** Two types per level, each extending both types of the level above, one bucket each. */
  private void createLattice(final Database db) {
    db.command("sql", "CREATE VERTEX TYPE " + name(0, 'a') + " BUCKETS 1");
    db.command("sql", "CREATE VERTEX TYPE " + name(0, 'b') + " BUCKETS 1");
    for (int level = 1; level < LEVELS; level++)
      for (final char side : new char[] { 'a', 'b' })
        db.command("sql", "CREATE VERTEX TYPE " + name(level, side) + " EXTENDS " + name(level - 1, 'a') + ", " + name(level - 1, 'b')
            + " BUCKETS 1");
  }

  private static int totalTypes() {
    return 2 * LEVELS;
  }

  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aLatticeIsLinkedAndEveryAnswerIsGivenOncePerType() {
    createLattice(database);
    database.command("sql", "CREATE PROPERTY " + name(0, 'a') + ".p INTEGER");

    final DocumentType deepest = database.getSchema().getType(name(LEVELS - 1, 'a'));
    final DocumentType root = database.getSchema().getType(name(0, 'a'));

    assertThat(deepest.instanceOf(name(0, 'a'))).isTrue();
    assertThat(deepest.instanceOf(name(0, 'b'))).isTrue();
    assertThat(deepest.instanceOf("Missing")).isFalse();
    assertThat(deepest.isSubTypeOf(name(0, 'b'))).isTrue();
    assertThat(deepest.isSubTypeOf("Missing")).isFalse();
    assertThat(root.isSuperTypeOf(deepest.getName())).isTrue();
    assertThat(root.isSuperTypeOf("Missing")).isFalse();

    assertThat(deepest.getPolymorphicPropertyIfExists("missing")).isNull();
    assertThat(deepest.getPolymorphicPropertyIfExists("p")).isNotNull();
    assertThat(deepest.existsPolymorphicProperty("missing")).isFalse();
    assertThat(deepest.existsPolymorphicProperty("p")).isTrue();
    assertThat(deepest.getPolymorphicPropertyNames()).containsExactly("p");
    assertThat(deepest.getPolymorphicProperties()).hasSize(1);
    assertThat(deepest.getAllIndexes(true)).isEmpty();
    assertThat(deepest.getPolymorphicIndexByProperties("p")).isNull();
    assertThat(deepest.getPolymorphicColumnarIndex("p")).isNull();

    // every type owns one bucket, and the root reaches the bucket of every type below it once
    assertThat(root.getBuckets(true)).hasSize(totalTypes() - 1);
    assertThat(distinctFileIds(root.getBuckets(true))).hasSize(totalTypes() - 1);

    database.transaction(() -> database.newVertex(deepest.getName()).set("p", 1).save());
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + root.getName())) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + name(0, 'b') + " WHERE p = 1")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aLatticeIsLoadedAgainWhenTheDatabaseIsReopened() {
    createLattice(database);
    database.transaction(() -> database.newVertex(name(LEVELS - 1, 'b')).save());

    reopenDatabase();

    final DocumentType deepest = database.getSchema().getType(name(LEVELS - 1, 'b'));
    assertThat(deepest.getSuperTypes()).extracting(DocumentType::getName).containsExactly(name(LEVELS - 2, 'a'), name(LEVELS - 2, 'b'));
    assertThat(deepest.instanceOf(name(0, 'a'))).isTrue();
    assertThat(deepest.isSubTypeOf("Missing")).isFalse();
    assertThat(database.getSchema().getType(name(0, 'a')).getBuckets(true)).hasSize(totalTypes() - 1);
    assertThat(database.countType(name(0, 'b'), true)).isEqualTo(1L);
  }

  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aLinkIsRemovedAndAddedBackInTheMiddleOfALattice() {
    createLattice(database);
    final DocumentType root = database.getSchema().getType(name(0, 'a'));
    final DocumentType other = database.getSchema().getType(name(0, 'b'));
    final int middle = LEVELS / 2;
    final String middleType = name(middle, 'a');

    database.command("sql", "ALTER TYPE " + middleType + " SUPERTYPE -" + name(middle - 1, 'b'));
    assertThat(database.getSchema().getType(middleType).getSuperTypes()).extracting(DocumentType::getName)
        .containsExactly(name(middle - 1, 'a'));
    // the other type of the top level keeps every type below it, the removed link being one of many paths
    assertThat(other.getBuckets(true)).hasSize(totalTypes() - 1);
    assertThat(distinctFileIds(root.getBuckets(true))).hasSize(totalTypes() - 1);

    database.command("sql", "ALTER TYPE " + middleType + " SUPERTYPE +" + name(middle - 1, 'b'));
    assertThat(database.getSchema().getType(middleType).getSuperTypes()).hasSize(2);
    assertThat(root.getBuckets(true)).hasSize(totalTypes() - 1);
    assertThat(distinctFileIds(root.getBuckets(true))).hasSize(totalTypes() - 1);
    assertThat(database.getSchema().getType(name(LEVELS - 1, 'b')).instanceOf(name(middle - 1, 'b'))).isTrue();
  }

  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aBucketIsAddedToTheLowestTypeOfALattice() {
    createLattice(database);
    final DocumentType root = database.getSchema().getType(name(0, 'a'));
    final DocumentType deepest = database.getSchema().getType(name(LEVELS - 1, 'a'));

    database.command("sql", "CREATE BUCKET extra_bucket");
    database.command("sql", "ALTER TYPE " + deepest.getName() + " BUCKET +extra_bucket");

    assertThat(deepest.getBuckets(false)).hasSize(2);
    assertThat(root.getBuckets(true)).hasSize(totalTypes());
    assertThat(distinctFileIds(root.getBuckets(true))).hasSize(totalTypes());

    database.command("sql", "ALTER TYPE " + deepest.getName() + " BUCKET -extra_bucket");
    assertThat(root.getBuckets(true)).hasSize(totalTypes() - 1);
  }

  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void theLabelsOfANodeAreListedOnceEachThroughOpenCypher() {
    createLattice(database);
    database.transaction(() -> database.newVertex(name(LEVELS - 1, 'a')).save());

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + name(0, 'b') + ") RETURN labels(n) AS labels")) {
      assertThat(rs.hasNext()).isTrue();
      final List<?> labels = rs.next().getProperty("labels");
      // every type of the lattice but the other type of the lowest level
      assertThat(labels).hasSize(totalTypes() - 1);
      assertThat(new HashSet<>(labels)).hasSize(totalTypes() - 1);
    }
  }

  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aPropertyIsDroppedAndTheTopOfALatticeIsTruncated() {
    createLattice(database);
    database.command("sql", "CREATE PROPERTY " + name(0, 'a') + ".p INTEGER");
    database.command("sql", "CREATE PROPERTY " + name(0, 'b') + ".q INTEGER");
    database.transaction(() -> database.newVertex(name(LEVELS - 1, 'a')).set("p", 1).save());

    database.command("sql", "DROP PROPERTY " + name(0, 'a') + ".p FORCE");
    assertThat(database.getSchema().getType(name(LEVELS - 1, 'b')).existsPolymorphicProperty("p")).isFalse();
    assertThat(database.getSchema().getType(name(LEVELS - 1, 'b')).existsPolymorphicProperty("q")).isTrue();

    database.command("sql", "TRUNCATE TYPE " + name(0, 'b') + " POLYMORPHIC UNSAFE");
    assertThat(database.countType(name(0, 'b'), true)).isZero();
    assertThat(database.countType(name(LEVELS - 1, 'a'), false)).isZero();
  }

  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void anIndexOnTheTopOfALatticeServesEveryTypeBelowIt() {
    database.command("sql", "CREATE VERTEX TYPE " + name(0, 'a') + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + name(0, 'a') + ".p INTEGER");
    database.command("sql", "CREATE INDEX ON " + name(0, 'a') + " (p) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE " + name(0, 'b') + " BUCKETS 1");
    for (int level = 1; level < LEVELS; level++)
      for (final char side : new char[] { 'a', 'b' })
        database.command("sql", "CREATE VERTEX TYPE " + name(level, side) + " EXTENDS " + name(level - 1, 'a') + ", "
            + name(level - 1, 'b') + " BUCKETS 1");

    final String deepest = name(LEVELS - 1, 'b');
    assertThat(database.getSchema().getType(deepest).getAllIndexes(true)).hasSize(1);

    database.transaction(() -> {
      database.newVertex(deepest).set("p", 3).save();
      database.newVertex(name(LEVELS / 2, 'a')).set("p", 3).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + name(0, 'a') + " WHERE p = 3")) {
      assertThat(rs.stream().count()).isEqualTo(2L);
    }

    reopenDatabase();

    assertThat(database.getSchema().getType(deepest).getAllIndexes(true)).hasSize(1);
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + name(0, 'a') + " WHERE p = 3")) {
      assertThat(rs.stream().count()).isEqualTo(2L);
    }
  }

  /**
   * A type reached through two paths hands the same index over once: the lookups the record write path makes list each index of a
   * bucket one time.
   */
  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aTypeReachedThroughTwoPathsListsEachIndexOfABucketOnce() {
    database.command("sql", "CREATE VERTEX TYPE A BUCKETS 1");
    database.command("sql", "CREATE PROPERTY A.p INTEGER");
    database.command("sql", "CREATE INDEX ON A (p) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE B EXTENDS A BUCKETS 1");
    database.command("sql", "CREATE VERTEX TYPE C EXTENDS A BUCKETS 1");
    database.command("sql", "CREATE VERTEX TYPE D EXTENDS B, C BUCKETS 1");

    final DocumentType d = database.getSchema().getType("D");
    final int bucketOfA = database.getSchema().getType("A").getFirstBucketId();
    final int bucketOfD = d.getFirstBucketId();

    assertThat(d.getPolymorphicBucketIndexByBucketId(bucketOfA, null)).hasSize(1);
    assertThat(d.getPolymorphicBucketIndexMaintainedInTransaction(bucketOfA)).hasSize(1);
    final List<IndexInternal> ofD = d.getPolymorphicBucketIndexByBucketId(bucketOfD, null);
    assertThat(ofD).isNotEmpty();
    assertThat(identities(ofD)).hasSameSizeAs(ofD);
    assertThat(d.getAllIndexes(true)).hasSize(1);

    database.transaction(() -> {
      database.newVertex("D").set("p", 7).save();
      database.newVertex("A").set("p", 7).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM A WHERE p = 7")) {
      assertThat(rs.stream().count()).isEqualTo(2L);
    }
  }

  /** The types above and below a type are listed once each, in the order a depth-first walk meets them. */
  @Test
  void theTypesAboveAndBelowATypeAreListedOnceEach() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B EXTENDS A");
    database.command("sql", "CREATE VERTEX TYPE C EXTENDS A");
    database.command("sql", "CREATE VERTEX TYPE D EXTENDS B, C");

    final Schema schema = database.getSchema();
    assertThat(TypeHierarchy.allSuperTypes(schema.getType("D"))).extracting(DocumentType::getName).containsExactly("B", "A", "C");
    assertThat(TypeHierarchy.allSubTypes(schema.getType("A"))).extracting(DocumentType::getName).containsExactly("B", "D", "C");
    assertThat(TypeHierarchy.allSuperTypes(schema.getType("A"))).isEmpty();
    assertThat(TypeHierarchy.allSubTypes(schema.getType("D"))).isEmpty();

    assertThat(schema.getType("D").getPolymorphicPropertyNames()).isEmpty();
    assertThat(schema.getType("D").isSubTypeOf("a")).isTrue();
    assertThat(schema.getType("A").isSuperTypeOf("d")).isTrue();
    assertThat(schema.getType("B").isSuperTypeOf("C")).isFalse();
  }

  private static Set<Integer> distinctFileIds(final List<Bucket> buckets) {
    final Set<Integer> ids = new HashSet<>();
    for (final Bucket bucket : buckets)
      ids.add(bucket.getFileId());
    return ids;
  }

  private static Set<IndexInternal> identities(final List<IndexInternal> indexes) {
    final Set<IndexInternal> set = Collections.newSetFromMap(new IdentityHashMap<>());
    set.addAll(indexes);
    return set;
  }
}
