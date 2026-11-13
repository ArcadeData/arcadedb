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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBulkLoader;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: index schema ddl.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class IndexSchemaDdlTest {
  /**
   * Regression for #774: add bucket with index.
   *
   * Regression test for issue #774: When a bucket is added to a type AFTER indexes have been created,
   * the new bucket should be automatically indexed.
   * <p>
   * This issue was fixed in commit 0c80759ec1 (2023-11-14) as part of the remote schema API implementation.
   * The fix ensures that when addBucket() is called, all existing type indexes are automatically
   * created for the new bucket, and when removeBucket() is called, indexes on that bucket are dropped.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AddBucketWithIndex extends TestHelper {

    @Test
    void addBucketAfterIndexCreation() {
      // Step 1: Create a document type with an indexed property
      database.transaction(() -> {
        final DocumentType dtOrders = database.getSchema().createDocumentType("Order");
        dtOrders.createProperty("p1", Type.STRING);
        dtOrders.createProperty("p2", Type.STRING);
        // Create index on p1
        dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p1");
      });

      // Step 2: Add a new bucket to the type AFTER index creation
      database.transaction(() -> {
        final Bucket bucket = database.getSchema().createBucket("O202203");
        database.getSchema().getType("Order").addBucket(bucket);
      });

      // Step 3: Insert records - one into the new bucket, one into the type
      database.transaction(() -> {
        database.command("sql", "INSERT INTO BUCKET:O202203 SET p1 = 'PENDING', p2 = 'PENDING'");
        database.command("sql", "INSERT INTO Order SET p1 = 'PENDING', p2 = 'PENDING'");
      });

      // Step 4: Query using indexed property p1 - should return BOTH records
      database.transaction(() -> {
        try (final ResultSet resultSet = database.query("sql", "SELECT from Order WHERE p1 = 'PENDING'")) {
          int count = 0;
          while (resultSet.hasNext()) {
            final Result result = resultSet.next();
            assertThat(result.<String>getProperty("p1")).isEqualTo("PENDING");
            assertThat(result.<String>getProperty("p2")).isEqualTo("PENDING");
            count++;
          }
          // Verify that the query returns both records (one from the new bucket, one from the default bucket)
          assertThat(count).as("Query with indexed property should return both records").isEqualTo(2);
        }
      });

      // Step 5: Query from the specific bucket using indexed property - should return 1 record
      database.transaction(() -> {
        try (final ResultSet resultSet = database.query("sql", "SELECT from bucket:O202203 WHERE p1 = 'PENDING'")) {
          int count = 0;
          while (resultSet.hasNext()) {
            final Result result = resultSet.next();
            assertThat(result.<String>getProperty("p1")).isEqualTo("PENDING");
            assertThat(result.<String>getProperty("p2")).isEqualTo("PENDING");
            count++;
          }
          // Verify that the query from the specific bucket returns 1 record
          assertThat(count).as("Query from bucket with indexed property should return 1 record").isEqualTo(1);
        }
      });

      // Step 6: Query using non-indexed property p2 - this does a full scan of all buckets
      database.transaction(() -> {
        try (final ResultSet resultSet = database.query("sql", "SELECT from Order WHERE p2 = 'PENDING'")) {
          int count = 0;
          while (resultSet.hasNext()) {
            final Result result = resultSet.next();
            assertThat(result.<String>getProperty("p1")).isEqualTo("PENDING");
            assertThat(result.<String>getProperty("p2")).isEqualTo("PENDING");
            count++;
          }
          // Verify that queries with non-indexed properties also return both records
          assertThat(count).as("Query with non-indexed property should return both records").isEqualTo(2);
        }
      });
    }
  }

  /**
   * Regression for #7892: remove super type detaches indexes.
   *
   * Regression test for issue #7892: {@code removeSuperType} undid only the polymorphic BUCKET side of a linkage
   * (issue #6935), never the INDEX side. The sub-indexes {@code addSuperType} creates over the subtype's buckets stay
   * attached to the ancestor's {@link TypeIndex}, so after the link was severed that wrapper kept answering with
   * records that were no longer of its type: {@code lookupByKey} handed them back, {@code countEntries()} counted them,
   * and the ancestor's UNIQUE constraint stayed enforced across them, refusing a key that was genuinely free.
   * <p>
   * {@code SELECT} was spared because the planner filters index results by type downstream, which is why #6935's fix
   * looked complete. The attachment also survived a reopen, since {@code schema.json} still listed the component under
   * the ancestor.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RemoveSuperTypeDetachesIndexes extends TestHelper {

    @Test
    void theAncestorsIndexStopsCoveringTheDetachedSubtype() {
      final DocumentType a = database.getSchema().createDocumentType("A7892");
      final DocumentType b = database.getSchema().createDocumentType("B7892").addSuperType("A7892");
      a.createProperty("n", Type.INTEGER);
      a.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "n");

      database.transaction(() -> database.newDocument("B7892").set("n", 1).save());

      assertThat(database.lookupByKey("A7892", "n", 1).hasNext()).isTrue();

      b.removeSuperType("A7892");

      assertThat(count("A7892")).as("this is what #6935 already fixed").isZero();
      assertThat(database.lookupByKey("A7892", "n", 1).hasNext())
          .as("a type-scoped lookup must not hand back a record of a foreign type").isFalse();
      assertThat(database.getSchema().getIndexByName("A7892[n]").countEntries()).isZero();

      // AND THE KEY IS FREE AGAIN: THE ANCESTOR'S UNIQUE CONSTRAINT NO LONGER SPANS THE DETACHED SUBTREE
      database.transaction(() -> database.newDocument("A7892").set("n", 1).save());
      assertThat(database.lookupByKey("A7892", "n", 1).hasNext()).isTrue();

      // THE SUB-INDEX COMPONENT IS GONE FROM THE SCHEMA TOO, NOT MERELY UNHOOKED IN MEMORY
      assertThat(indexNames()).noneMatch(n -> n.startsWith("B7892_"));

      reopenDatabase();
      assertThat(indexNames()).noneMatch(n -> n.startsWith("B7892_"));
      assertThat(database.getSchema().getIndexByName("A7892[n]").countEntries()).isEqualTo(1);
    }

    @Test
    void aGrandchildsBucketsAreDetachedFromTheTopAncestorToo() {
      // The propagated set is getAllIndexes(true), so a grandchild's own addSuperType attaches its bucket indexes to
      // the TOP ancestor's wrapper - the whole detached subtree is affected, not only its root.
      database.command("sql", "CREATE DOCUMENT TYPE A2_7892");
      database.command("sql", "CREATE PROPERTY A2_7892.n INTEGER");
      database.command("sql", "CREATE INDEX ON A2_7892 (n) UNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE B2_7892 EXTENDS A2_7892");
      database.command("sql", "CREATE DOCUMENT TYPE C2_7892 EXTENDS B2_7892");
      database.transaction(() -> database.command("sql", "INSERT INTO C2_7892 SET n = 7"));

      assertThat(database.lookupByKey("A2_7892", "n", 7).hasNext()).isTrue();

      database.getSchema().getType("B2_7892").removeSuperType("A2_7892");

      assertThat(database.lookupByKey("A2_7892", "n", 7).hasNext()).isFalse();
      assertThat(database.getSchema().getIndexByName("A2_7892[n]").countEntries()).isZero();
      assertThat(indexNames()).noneMatch(n -> n.startsWith("C2_7892_") || n.startsWith("B2_7892_"));
    }

    @Test
    void anIndexOwnedByTheGRANDPARENTOfTheSeveredLinkIsDetachedToo() {
      // Z6 <- A6 <- B6 <- C6, severing B6 -> A6. Z6[n] is propagated down the whole chain, so its wrapper holds
      // components over A6's, B6's and C6's buckets - and the two that go belong to types Z6 can no longer reach. This
      // is the collectSubIndexesNoLongerCovered() recursion into the FORMER SUPER TYPE'S own super types: without it
      // the walk would stop at A6 and leave the top of the chain indexing the detached subtree (PR #8091 review).
      database.command("sql", "CREATE DOCUMENT TYPE Z6_7892");
      database.command("sql", "CREATE PROPERTY Z6_7892.n INTEGER");
      database.command("sql", "CREATE INDEX ON Z6_7892 (n) UNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE A6_7892 EXTENDS Z6_7892");
      database.command("sql", "CREATE DOCUMENT TYPE B6_7892 EXTENDS A6_7892");
      database.command("sql", "CREATE DOCUMENT TYPE C6_7892 EXTENDS B6_7892");
      database.transaction(() -> database.command("sql", "INSERT INTO C6_7892 SET n = 6"));
      database.transaction(() -> database.command("sql", "INSERT INTO A6_7892 SET n = 60"));

      assertThat(database.lookupByKey("Z6_7892", "n", 6).hasNext()).isTrue();

      database.getSchema().getType("B6_7892").removeSuperType("A6_7892");

      assertThat(database.lookupByKey("Z6_7892", "n", 6).hasNext())
          .as("Z6 is no longer an ancestor of C6 either, so its index must not reach C6's bucket").isFalse();
      assertThat(database.lookupByKey("Z6_7892", "n", 60).hasNext()).as("A6 is still a subtype of Z6").isTrue();
      assertThat(database.getSchema().getIndexByName("Z6_7892[n]").countEntries()).isEqualTo(1);
      assertThat(coveredBuckets("Z6_7892[n]"))
          .containsAll(database.getSchema().getType("A6_7892").getBucketIds(false))
          .doesNotContainAnyElementsOf(database.getSchema().getType("B6_7892").getBucketIds(false))
          .doesNotContainAnyElementsOf(database.getSchema().getType("C6_7892").getBucketIds(false));
      assertThat(indexNames()).noneMatch(n -> n.startsWith("B6_7892_") || n.startsWith("C6_7892_"));
    }

    @Test
    void aBucketStillReachedThroughAnotherPathOfADiamondKeepsItsSubIndex() {
      // C3 EXTENDS BOTH B3 AND A3, AND B3 EXTENDS A3: SEVERING B3 -> A3 MUST NOT TAKE C3'S SUB-INDEX AWAY, BECAUSE C3
      // IS STILL A SUBTYPE OF A3.
      database.command("sql", "CREATE DOCUMENT TYPE A3_7892");
      database.command("sql", "CREATE PROPERTY A3_7892.n INTEGER");
      database.command("sql", "CREATE INDEX ON A3_7892 (n) UNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE B3_7892 EXTENDS A3_7892");
      database.command("sql", "CREATE DOCUMENT TYPE C3_7892 EXTENDS B3_7892, A3_7892");
      database.transaction(() -> database.command("sql", "INSERT INTO C3_7892 SET n = 3"));

      database.getSchema().getType("B3_7892").removeSuperType("A3_7892");

      assertThat(database.lookupByKey("A3_7892", "n", 3).hasNext()).as("C3 still extends A3 directly").isTrue();
      assertThat(database.getSchema().getIndexByName("A3_7892[n]").countEntries()).isEqualTo(1);
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO A3_7892 SET n = 3")))
          .isInstanceOf(DuplicatedKeyException.class);
      assertThat(coveredBuckets("A3_7892[n]"))
          .containsAll(database.getSchema().getType("C3_7892").getBucketIds(false))
          .doesNotContainAnyElementsOf(database.getSchema().getType("B3_7892").getBucketIds(false));
    }

    @Test
    void droppingAMiddleTypeLeavesTheSurvivingGrandchildIndexedByTheAncestor() {
      // DROP TYPE is the one caller that severs a super-type link without meaning the subtree to stop being indexed:
      // LocalSchema.dropType unlinks the doomed type from each super type only to re-parent the SURVIVING sub types
      // onto those same super types a few lines later, and re-links them with createIndexes=false because their
      // components are still attached. Dropping "what the ancestor no longer reaches" in that window reads a
      // relationship the schema is halfway through rewriting and takes the grandchild's component with it, with
      // nothing to put it back (PR #8091: caught as a regression of the fix on this page).
      database.command("sql", "CREATE DOCUMENT TYPE A7_7892");
      database.command("sql", "CREATE PROPERTY A7_7892.n INTEGER");
      database.command("sql", "CREATE INDEX ON A7_7892 (n) UNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE B7_7892 EXTENDS A7_7892");
      database.command("sql", "CREATE DOCUMENT TYPE C7_7892 EXTENDS B7_7892");
      database.transaction(() -> database.command("sql", "INSERT INTO C7_7892 SET n = 1"));

      assertThat(database.lookupByKey("A7_7892", "n", 1).hasNext()).isTrue();

      database.getSchema().dropType("B7_7892");

      assertThat(database.getSchema().getType("C7_7892").getSuperTypes()).extracting("name").containsExactly("A7_7892");
      assertThat(database.lookupByKey("A7_7892", "n", 1).hasNext())
          .as("C7 is still an A7 after B7 is dropped, so A7's index must still cover C7's bucket").isTrue();
      assertThat(coveredBuckets("A7_7892[n]"))
          .containsAll(database.getSchema().getType("C7_7892").getBucketIds(false));
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO C7_7892 SET n = 1")))
          .as("and the ancestor's UNIQUE constraint is still enforced over it")
          .isInstanceOf(DuplicatedKeyException.class);
    }

    @Test
    void relinkingTheSuperTypeBringsTheIndexBack() {
      database.command("sql", "CREATE DOCUMENT TYPE A4_7892");
      database.command("sql", "CREATE PROPERTY A4_7892.n INTEGER");
      database.command("sql", "CREATE INDEX ON A4_7892 (n) UNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE B4_7892 EXTENDS A4_7892");
      database.transaction(() -> database.command("sql", "INSERT INTO B4_7892 SET n = 4"));

      final DocumentType b = database.getSchema().getType("B4_7892");
      b.removeSuperType("A4_7892");
      assertThat(database.lookupByKey("A4_7892", "n", 4).hasNext()).isFalse();

      b.addSuperType("A4_7892");
      assertThat(database.lookupByKey("A4_7892", "n", 4).hasNext())
          .as("the re-created sub-index must be built over the records already in the bucket").isTrue();
      assertThat(database.getSchema().getIndexByName("A4_7892[n]").countEntries()).isEqualTo(1);
    }

    @Test
    void anAbstractAncestorLosingItsLastSubIndexLeavesNoEmptyWrapperBehind() {
      // A5 declares the index but owns no bucket of its own, so B5's is the ONLY component of A5[n]: dropping it must
      // take the wrapper away with it, or the next schema serialization asks an empty TypeIndex for its property names.
      database.command("sql", "CREATE DOCUMENT TYPE A5_7892");
      database.command("sql", "CREATE PROPERTY A5_7892.n INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE B5_7892 EXTENDS A5_7892");
      database.command("sql", "CREATE INDEX ON A5_7892 (n) UNIQUE");
      for (final Bucket bucket : database.getSchema().getType("A5_7892").getBuckets(false))
        database.getSchema().getType("A5_7892").removeBucket(bucket);

      database.getSchema().getType("B5_7892").removeSuperType("A5_7892");

      assertThat(indexNames()).noneMatch(n -> n.startsWith("B5_7892_"));
      reopenDatabase();
      assertThat(database.getSchema().existsType("A5_7892")).isTrue();
    }

    private List<Integer> coveredBuckets(final String typeIndexName) {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(typeIndexName);
      return Arrays.stream(index.getIndexesOnBuckets()).map(IndexInternal::getAssociatedBucketId).toList();
    }

    private List<String> indexNames() {
      return Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList();
    }

    private long count(final String typeName) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
        return rs.stream().count();
      }
    }
  }

  /**
   * Regression for #9291: sorted build runs.
   *
   * Regression tests for issue #9291 on the multi-run paths of the sorted build: a low-cardinality prefix grows the run past the
   * default limit, a high-cardinality tail then overflows the shared-key table (the run falls back to comparing keys), and a run of
   * mostly distinct keys never shares at all. The sorted index must hold what the default build holds, in the same key order, and the
   * {@code buildMode} directive of SQL must really select the sorted build.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SortedBuildRuns extends TestHelper {
    private static final int LOW_CARDINALITY_ROWS = 100_000;
    private static final int DISTINCT_ROWS        = 100_000;

    @Test
    void spilledRunsWithSharingThatEndsMidRunMatchTheDefaultBuild() {
      for (final String type : new String[] { "Sorted", "Plain" }) {
        database.command("sql", "CREATE DOCUMENT TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".k STRING");
        database.begin();
        for (int i = 0; i < LOW_CARDINALITY_ROWS + DISTINCT_ROWS; i++) {
          final String key = i < LOW_CARDINALITY_ROWS ? "low-%02d".formatted(i % 37) : "u-%08d".formatted(Math.floorMod(i * 7919, 1_000_003));
          database.newDocument(type).set("k", key).save();
          if (i % 20_000 == 19_999) {
            database.commit();
            database.begin();
          }
        }
        database.commit();
      }

      final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
      TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
      try {
        // A run of 120,000 entries holds the whole low-cardinality prefix and the first 20,000 distinct keys, so the 32,768-entry
        // probe is reached with sharing on; the second run is all distinct keys, which fill the shared-key table and end the sharing
        LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(120_000);
        database.getSchema().buildTypeIndex("Sorted", new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE)
            .withBuildMode(IndexBuildMode.SORTED).withBuildMemoryBudget(128L << 20).withUnique(false).create();
      } finally {
        LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(0);
        TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
      }
      database.command("sql", "CREATE INDEX ON Plain (k) NOTUNIQUE");

      assertThat(captured.get()).as("the sorted build ran").isNotNull();
      assertThat(captured.get().initialRuns()).as("the build spilled").isGreaterThan(0);
      assertThat(captured.get().logicalEntries()).isEqualTo(LOW_CARDINALITY_ROWS + DISTINCT_ROWS);

      final List<String> sorted = entries(database.getSchema().getType("Sorted").getPolymorphicIndexByProperties("k"));
      final List<String> plain = entries(database.getSchema().getType("Plain").getPolymorphicIndexByProperties("k"));
      assertThat(sorted).hasSize(LOW_CARDINALITY_ROWS + DISTINCT_ROWS);
      assertThat(keysOnly(sorted)).as("key order").isEqualTo(keysOnly(plain));
      assertThat(sorted.stream().sorted().toList()).as("entries").isEqualTo(plain.stream().sorted().toList());
    }

    @Test
    void defaultBuildModeDoesNotUseTheSortedBuild() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.k STRING");
      database.transaction(() -> database.newDocument("D").set("k", "a").save());
      final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
      TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
      try {
        database.command("sql", "CREATE INDEX ON D (k) NOTUNIQUE METADATA {\"buildMode\": \"DEFAULT\"}");
      } finally {
        TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
      }
      assertThat(captured.get()).isNull();
    }

    @Test
    void sortedBuildModeDirectiveSelectsTheSortedBuild() {
      database.command("sql", "CREATE DOCUMENT TYPE S");
      database.command("sql", "CREATE PROPERTY S.k STRING");
      database.transaction(() -> database.newDocument("S").set("k", "a").save());
      final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
      TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
      try {
        database.command("sql", "CREATE INDEX ON S (k) NOTUNIQUE METADATA {\"buildMode\": \"SORTED\"}");
      } finally {
        TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
      }
      assertThat(captured.get()).isNotNull();
    }

    private static List<String> keysOnly(final List<String> entries) {
      return entries.stream().map(e -> e.substring(0, e.indexOf('@'))).toList();
    }

    private static List<String> entries(final TypeIndex index) {
      final List<String> out = new ArrayList<>();
      final IndexCursor cursor = index.iterator(true);
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        out.add(Arrays.toString(cursor.getKeys()) + "@" + rid.getPosition());
      }
      return out;
    }
  }

  /**
   * Regression for #9331: index under construction.
   *
   * Issue #9331: the marker of an index being populated is counted, so two builds on the same properties keep the index out of
   * the queries until the last one is over, and a build on a subtype hierarchy marks the type the index belongs to.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IndexUnderConstruction extends TestHelper {
    @Test
    void theMarkerOutlivesTheFirstOfTwoBuildsOnTheSameProperties() {
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().createDocumentType("Marked");
      final List<String> properties = List.of("id");

      assertThat(type.isIndexUnderConstruction(properties, Schema.INDEX_TYPE.LSM_TREE)).isFalse();

      type.beginIndexConstruction(properties, Schema.INDEX_TYPE.LSM_TREE);
      type.beginIndexConstruction(properties, Schema.INDEX_TYPE.LSM_TREE);
      assertThat(type.isIndexUnderConstruction(properties, Schema.INDEX_TYPE.LSM_TREE)).isTrue();

      type.endIndexConstruction(properties, Schema.INDEX_TYPE.LSM_TREE);
      assertThat(type.isIndexUnderConstruction(properties, Schema.INDEX_TYPE.LSM_TREE)).as("the second build is still populating")
          .isTrue();
      assertThat(type.isIndexUnderConstruction(properties, Schema.INDEX_TYPE.COLUMNAR)).as("the marker is per index family").isFalse();

      type.endIndexConstruction(properties, Schema.INDEX_TYPE.LSM_TREE);
      assertThat(type.isIndexUnderConstruction(properties, Schema.INDEX_TYPE.LSM_TREE)).isFalse();
      assertThat(type.isIndexUnderConstruction(List.of("other"), Schema.INDEX_TYPE.LSM_TREE)).isFalse();
    }

    /** Pins the wording the stale-index detection reads: a change in the file manager's message must fail here, not in production. */
    @Test
    void theFileManagerMessageIsWhatTheStaleIndexDetectionMatches() {
      assertThatThrownBy(() -> ((DatabaseInternal) database).getFileManager().getFile(Integer.MAX_VALUE))
          .isInstanceOfSatisfying(IllegalArgumentException.class, e -> assertThat(TypeIndex.isFileNotFound(e)).isTrue());
      assertThat(TypeIndex.isFileNotFound(new IllegalArgumentException("bad key"))).isFalse();
      assertThat(TypeIndex.isFileNotFound(new IllegalArgumentException())).isFalse();
    }

    @Test
    void theMarkerBelongsToTheTypeBeingIndexed() {
      final LocalDocumentType first = (LocalDocumentType) database.getSchema().createDocumentType("First");
      final LocalDocumentType second = (LocalDocumentType) database.getSchema().createDocumentType("Second");
      final List<String> properties = List.of("id");

      first.beginIndexConstruction(properties, Schema.INDEX_TYPE.LSM_TREE);

      assertThat(first.isIndexUnderConstruction(properties, Schema.INDEX_TYPE.LSM_TREE)).isTrue();
      assertThat(second.isIndexUnderConstruction(properties, Schema.INDEX_TYPE.LSM_TREE)).as("another type with the same property names").isFalse();

      first.endIndexConstruction(properties, Schema.INDEX_TYPE.LSM_TREE);
    }

    @Test
    void anIndexOnAParentTypeIsReadyOnlyOnceItsBuildIsOver() {
      final DocumentType parent = database.getSchema().createDocumentType("Parent");
      parent.createProperty("id", Type.LONG);
      database.getSchema().createDocumentType("Child").addSuperType(parent);
      final LocalDocumentType local = (LocalDocumentType) parent;

      database.transaction(() -> {
        for (long i = 0; i < 20; i++)
          database.newDocument("Child").set("id", i).save();
      });

      final TypeIndex[] seenDuringBuild = new TypeIndex[1];
      final boolean[] readyDuringBuild = new boolean[] { true };
      parent.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, new String[] { "id" }, 262_144, (document, total) -> {
        seenDuringBuild[0] = parent.getPolymorphicIndexByProperties("id");
        readyDuringBuild[0] = seenDuringBuild[0] != null && seenDuringBuild[0].isReadyForQueries();
      });

      assertThat(seenDuringBuild[0]).as("the build callback ran and the index was already registered").isNotNull();
      assertThat(readyDuringBuild[0]).as("not offered to queries while it is being populated").isFalse();
      assertThat(local.isIndexUnderConstruction(List.of("id"), Schema.INDEX_TYPE.LSM_TREE)).isFalse();
      assertThat(parent.getPolymorphicIndexByProperties("id").isReadyForQueries()).isTrue();
    }
  }
}
