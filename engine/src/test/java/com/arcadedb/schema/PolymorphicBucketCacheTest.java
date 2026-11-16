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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the polymorphic bucket cache and bucket selection strategy of a type, one nested class per fixed issue.
 */
class PolymorphicBucketCacheTest {
  /**
   * Regression for #5297: the polymorphic bucket cache of a super type must stay in sync when buckets
   * are added to (or removed from) a sub type that is already linked to it. Before the fix, only a schema reload
   * repaired the cache, so on a HA cluster the leader (which mutates the schema in place) reported a different
   * `count(*)` on the base type than the followers (which rebuild the schema from the replicated schema.json).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PolymorphicCount extends TestHelper {

    private long countViaSQL(final String typeName) {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM " + typeName)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    @Test
    void bucketAddedToSubTypeAfterInheritanceIsCountedOnSuperType() {
      database.getSchema().createVertexType("V");
      database.getSchema().createVertexType("Tenant").addSuperType("V");

      // ADD A SECOND BUCKET TO THE SUB TYPE *AFTER* THE INHERITANCE HAS BEEN DECLARED
      database.command("sql", "ALTER TYPE Tenant BUCKET +Tenant_extra");

      database.transaction(() -> {
        for (int i = 0; i < 20; i++)
          database.newVertex("Tenant").set("id", i).save();
      });

      assertThat(database.getSchema().getType("Tenant").getBuckets(false)).hasSize(2);
      // V's own bucket + the 2 buckets of Tenant
      assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(3);

      assertThat(database.countType("Tenant", true)).isEqualTo(20);
      assertThat(database.countType("V", true)).isEqualTo(20);
      assertThat(countViaSQL("V")).isEqualTo(20);

      // THE STALE CACHE HID THE RECORDS FROM EVERY POLYMORPHIC READ, NOT JUST FROM count(*)
      try (final ResultSet rs = database.query("sql", "SELECT FROM V")) {
        assertThat(rs.stream().count()).isEqualTo(20);
      }
    }

    /**
     * Models the HA asymmetry reported in the issue: the leader keeps mutating its in-memory schema while the followers
     * rebuild theirs from the replicated schema.json. Reopening the database here replays exactly the followers' path,
     * so both counts must agree.
     */
    @Test
    void inMemorySchemaCountMatchesTheReloadedSchemaCount() {
      database.getSchema().createVertexType("V");
      database.getSchema().createVertexType("Tenant").addSuperType("V");
      database.getSchema().createVertexType("Agent").addSuperType("V");

      database.command("sql", "ALTER TYPE Tenant BUCKET +Tenant_extra");
      database.command("sql", "ALTER TYPE Agent BUCKET +Agent_extra");

      database.transaction(() -> {
        for (int i = 0; i < 12; i++)
          database.newVertex("Tenant").set("id", i).save();
        for (int i = 0; i < 31; i++)
          database.newVertex("Agent").set("id", i).save();
      });

      final long beforeReload = countViaSQL("V");

      reopenDatabase();

      assertThat(countViaSQL("V")).isEqualTo(43);
      assertThat(beforeReload).isEqualTo(43);
    }

    @Test
    void bucketRemovedFromSubTypeAfterInheritanceIsDroppedFromSuperType() {
      database.getSchema().createVertexType("V");
      final DocumentType tenant = database.getSchema().buildVertexType().withName("Tenant").withTotalBuckets(2).create();
      tenant.addSuperType("V");

      assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(3);

      database.command("sql", "ALTER TYPE Tenant BUCKET -Tenant_1");

      assertThat(database.getSchema().getType("Tenant").getBuckets(false)).hasSize(1);
      assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(2);
    }

    @Test
    void grantingMoreBucketsOnExistingTypeKeepsSuperTypeInSync() {
      database.getSchema().createVertexType("V");
      // FIRST CALL: CREATES THE TYPE WITH 1 BUCKET AND LINKS IT TO V
      database.getSchema().buildVertexType().withName("Tenant").withTotalBuckets(1).withSuperType("V").withIgnoreIfExists(true)
          .create();
      // SECOND CALL (IDEMPOTENT PROVISIONING): GROWS THE TYPE TO 4 BUCKETS, V MUST SEE THEM ALL
      database.getSchema().buildVertexType().withName("Tenant").withTotalBuckets(4).withSuperType("V").withIgnoreIfExists(true)
          .create();

      database.transaction(() -> {
        for (int i = 0; i < 40; i++)
          database.newVertex("Tenant").set("id", i).save();
      });

      assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(5);
      assertThat(countViaSQL("V")).isEqualTo(40);
    }

    @Test
    void multiLevelHierarchyDoesNotDoubleCountIntermediateBuckets() {
      database.getSchema().createVertexType("V");
      database.getSchema().createVertexType("Account").addSuperType("V");
      database.getSchema().createVertexType("Customer").addSuperType("Account");

      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          database.newVertex("Account").set("id", i).save();
        for (int i = 0; i < 7; i++)
          database.newVertex("Customer").set("id", i).save();
      });

      assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(3);
      assertThat(database.getSchema().getType("Account").getBuckets(true)).hasSize(2);
      assertThat(countViaSQL("Customer")).isEqualTo(7);
      assertThat(countViaSQL("Account")).isEqualTo(12);
      assertThat(countViaSQL("V")).isEqualTo(12);
    }
  }

  /**
   * Regression for #6678: {@code LocalDocumentType.cachedPolymorphicBuckets}/{@code cachedPolymorphicBucketIds} are published
   * by copy-on-write reassignment ({@code addBucketInternal}, {@code removeBucket}, {@code updatePolymorphicBucketsCache})
   * but read lock-free on the query-planning hot path ({@code getBuckets(true)}/{@code getBucketIds(true)}). A plain
   * (non-{@code volatile}) field gives the Java Memory Model no happens-before edge between a schema-mutation thread's
   * reassignment and a planning thread's read, so the planner is not guaranteed to ever observe a concurrent
   * {@code ALTER TYPE ... BUCKET} - the same publication gap the sibling {@code LocalSchema.bucketId2TypeMap} pattern
   * exists to avoid.
   * <p>
   * The fix makes both fields {@code volatile}. Issue #7033 extends it to the non-polymorphic siblings {@code buckets}/
   * {@code bucketIds}: same copy-on-write reassignment on the adjacent lines, same lock-free accessor (the other branch
   * of the same ternary), read by {@code SelectExecutionPlanner}'s partition pruning and {@code FetchFromSchemaTypesStep}
   * through {@code getBuckets(false)}/{@code getBucketIds(false)}. The first test pins the fix itself, so a future edit
   * cannot silently drop the modifier from any of the four. The second is a functional regression for the concrete
   * scenario: adding/removing a bucket must be reflected by both branches of {@code getBuckets}/{@code getBucketIds}
   * immediately afterward.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PolymorphicBucketCacheVisibility extends TestHelper {

    @Test
    void bucketListFieldsMustBeVolatileForCrossThreadPlanningReads() throws Exception {
      for (final String fieldName : new String[] { "buckets", "cachedPolymorphicBuckets", "bucketIds", "cachedPolymorphicBucketIds" }) {
        final Field field = LocalDocumentType.class.getDeclaredField(fieldName);
        assertThat(Modifier.isVolatile(field.getModifiers()))
            .as(fieldName + " is copy-on-write reassigned under schema mutation and read lock-free by query planning - it "
                + "must be volatile so planning threads have a happens-before edge against the writer (issues #6678, #7033)")
            .isTrue();
      }
    }

    @Test
    void addingThenRemovingABucketIsReflectedByThePolymorphicCache() {
      database.transaction(() -> database.getSchema().createDocumentType("Product", 1));

      database.transaction(() -> {
        final DocumentType type = database.getSchema().getType("Product");
        final int before = type.getBuckets(true).size();

        final Bucket newBucket = database.getSchema().createBucket("Product_extra");
        type.addBucket(newBucket);

        assertThat(type.getBuckets(true)).hasSize(before + 1).contains(newBucket);
        assertThat(type.getBucketIds(true)).contains(newBucket.getFileId());
        assertThat(type.getBuckets(false)).hasSize(before + 1).contains(newBucket);
        assertThat(type.getBucketIds(false)).contains(newBucket.getFileId());

        type.removeBucket(newBucket);

        assertThat(type.getBuckets(true)).hasSize(before).doesNotContain(newBucket);
        assertThat(type.getBucketIds(true)).doesNotContain(newBucket.getFileId());
        assertThat(type.getBuckets(false)).hasSize(before).doesNotContain(newBucket);
        assertThat(type.getBucketIds(false)).doesNotContain(newBucket.getFileId());
      });
    }
  }

  /**
   * Regression for #6935: {@code removeSuperType} withdrew only the type's OWN buckets from the former
   * ancestor's polymorphic cache, while linking had contributed the whole polymorphic subtree. A grandchild's buckets
   * therefore stayed in the ancestor after the link was severed, and {@code SELECT FROM <ancestor>} kept returning the
   * grandchild's records. A two-level hierarchy hid it because for a leaf type the two lists coincide.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RemoveSuperTypeGrandchild extends TestHelper {

    @Test
    void removingTheMiddleLinkWithdrawsTheGrandchildFromTheAncestor() {
      database.command("sql", "CREATE DOCUMENT TYPE A2");
      database.command("sql", "CREATE DOCUMENT TYPE B2 EXTENDS A2");
      database.command("sql", "CREATE DOCUMENT TYPE C2 EXTENDS B2");
      database.transaction(() -> database.command("sql", "INSERT INTO C2 SET n = 1"));

      final DocumentType a2 = database.getSchema().getType("A2");
      final DocumentType b2 = database.getSchema().getType("B2");
      final DocumentType c2 = database.getSchema().getType("C2");
      final List<Bucket> c2Buckets = c2.getBuckets(false);

      assertThat(count("A2")).isEqualTo(1);
      assertThat(a2.getBuckets(true)).containsAll(c2Buckets);

      b2.removeSuperType("A2");

      assertThat(a2.getSubTypes()).isEmpty();
      assertThat(b2.getSuperTypes()).isEmpty();
      assertThat(a2.getBuckets(true)).as("A2 has no relationship with C2 any more").doesNotContainAnyElementsOf(c2Buckets);
      assertThat(a2.getBucketIds(true)).doesNotContainAnyElementsOf(c2.getBucketIds(false));
      assertThat(count("A2")).as("a polymorphic scan of A2 must not read C2's bucket").isZero();

      // B2 STILL SEES ITS OWN SUBTREE
      assertThat(b2.getBuckets(true)).containsAll(c2Buckets);
      assertThat(count("B2")).isEqualTo(1);
      assertThat(count("C2")).isEqualTo(1);

      // AND RE-LINKING BRINGS THE WHOLE SUBTREE BACK, ONCE
      b2.addSuperType("A2");
      assertThat(a2.getBuckets(true)).containsAll(c2Buckets).doesNotHaveDuplicates();
      assertThat(count("A2")).isEqualTo(1);
    }

    @Test
    void removingOneLinkOfADiamondKeepsTheBucketsStillReachableThroughTheOther() {
      // C3 EXTENDS BOTH B3 AND A3, AND B3 EXTENDS A3: SEVERING B3 -> A3 MUST NOT BLIND A3 TO C3, WHICH IS STILL ITS SUBTYPE
      database.command("sql", "CREATE DOCUMENT TYPE A3");
      database.command("sql", "CREATE DOCUMENT TYPE B3 EXTENDS A3");
      database.command("sql", "CREATE DOCUMENT TYPE C3 EXTENDS B3, A3");
      database.transaction(() -> database.command("sql", "INSERT INTO C3 SET n = 1"));

      final DocumentType a3 = database.getSchema().getType("A3");
      final DocumentType b3 = database.getSchema().getType("B3");
      final DocumentType c3 = database.getSchema().getType("C3");

      assertThat(a3.getBuckets(true)).containsAll(c3.getBuckets(false)).doesNotHaveDuplicates();
      assertThat(count("A3")).isEqualTo(1);

      b3.removeSuperType("A3");

      assertThat(a3.getSubTypes()).containsExactly(c3);
      assertThat(a3.getBuckets(true)).as("C3 still extends A3 directly").containsAll(c3.getBuckets(false));
      assertThat(a3.getBuckets(true)).doesNotContainAnyElementsOf(b3.getBuckets(false));
      assertThat(count("A3")).isEqualTo(1);

      c3.removeSuperType("A3");
      assertThat(a3.getBuckets(true)).doesNotContainAnyElementsOf(c3.getBuckets(false));
      assertThat(count("A3")).isZero();
    }

    @Test
    void theCacheSurvivesAReopen() {
      database.command("sql", "CREATE DOCUMENT TYPE A4");
      database.command("sql", "CREATE DOCUMENT TYPE B4 EXTENDS A4");
      database.command("sql", "CREATE DOCUMENT TYPE C4 EXTENDS B4");
      database.transaction(() -> database.command("sql", "INSERT INTO C4 SET n = 1"));

      database.getSchema().getType("B4").removeSuperType("A4");
      assertThat(count("A4")).isZero();

      reopenDatabase();
      assertThat(count("A4")).isZero();
      assertThat(count("B4")).isEqualTo(1);
    }

    /** Enumerates the rows a polymorphic scan of the type returns, so the assertion sees the bucket list and not a counter. */
    private long count(final String typeName) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
        return rs.stream().count();
      }
    }
  }

  /**
   * Regression for #6380: removing a bucket re-binds the bucket selection strategy of the type.
   * <p>
   * {@code removeBucketInternal} shrank the bucket list but never re-bound the type's bucket-selection strategy, so the
   * strategy's cached {@code total} stayed at the pre-removal bucket count. Round-robin selection keeps handing out
   * indexes up to the stale {@code total}, and once that reaches the old last index the type's own {@code buckets.get(...)}
   * throws {@link IndexOutOfBoundsException} because the bucket at that index is gone.
   * <p>
   * {@code addBucketInternal} already rebinds the strategy after growing the list (symmetric fix, issue #6380).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RemoveBucketStrategyRefresh extends TestHelper {

    @Test
    void insertAfterRemovingBucketDoesNotThrowIndexOutOfBounds() {
      database.transaction(() -> database.getSchema().createDocumentType("Product", 3));

      // Detach one of the three buckets, leaving a still-populated (well, still-usable) 2-bucket type behind.
      database.transaction(() -> {
        final DocumentType type = database.getSchema().getType("Product");
        final Bucket lastBucket = type.getBuckets(false).get(type.getBuckets(false).size() - 1);
        type.removeBucket(lastBucket);
      });

      // Round-robin's cursor starts at -1: three inserts push it through indexes 0, 1 and then the stale index 2, which
      // is exactly the index that used to be valid before the bucket was removed. Without the fix, the third insert
      // throws IndexOutOfBoundsException instead of wrapping back to index 0 against the now-correct 2-bucket total.
      assertThatCode(() -> database.transaction(() -> {
        for (int i = 0; i < 6; i++) {
          final MutableDocument doc = database.newDocument("Product");
          doc.set("id", i);
          doc.save();
        }
      })).doesNotThrowAnyException();
    }
  }

  /**
   * Regression for #7119: {@code LocalDocumentType.bucketSelectionStrategy} is the fifth copy-on-write member of the class, next
   * to the four bucket lists issues #6678/#7033 made {@code volatile}. It is reassigned by
   * {@code setBucketSelectionStrategy} and read lock-free by {@code getBucketIdByRecord}/{@code getBucketIndexByKeys}
   * on the record-write path and by the planner's partition pruning, so it needs the same happens-before edge. It was
   * also published before it was bound: the field was assigned first and {@code setType(this)} called afterwards, so a
   * concurrent insert could reach a strategy whose bucket count was still 0 or whose type was still null.
   * <p>
   * The first test pins the modifier, like the sibling {@link PolymorphicBucketCacheVisibility} does for
   * the four lists. The second uses a strategy that records what the type was publishing at the moment it was bound:
   * with the fix it is still the previous strategy, never the half-bound one. The last two check the rollback contract
   * the old try/catch provided survives the reordering, once per exit of the block it wrapped: a strategy that throws
   * while binding, and a real {@link PartitionedBucketSelectionStrategy} the suitability check refuses after binding
   * succeeded, are both never published.
   */
  @Nested
  class BucketSelectionStrategyPublication extends TestHelper {

    @Test
    void bucketSelectionStrategyMustBeVolatileForLockFreeReaders() throws Exception {
      final Field field = LocalDocumentType.class.getDeclaredField("bucketSelectionStrategy");
      assertThat(Modifier.isVolatile(field.getModifiers()))
          .as("bucketSelectionStrategy is copy-on-write reassigned by setBucketSelectionStrategy and read lock-free by "
              + "the record-write path and the planner - it must be volatile like its four sibling bucket lists (issue #7119)")
          .isTrue();
    }

    @Test
    void strategyIsBoundBeforeItIsPublished() {
      database.transaction(() -> database.getSchema().createDocumentType("Product", 2));

      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Product");
      final BucketSelectionStrategy previous = type.getBucketSelectionStrategy();
      final RecordingStrategy recording = new RecordingStrategy();

      database.transaction(() -> type.setBucketSelectionStrategy(recording));

      assertThat(recording.publishedWhenBound)
          .as("setType(this) must run before the field is assigned, so the type still publishes the previous strategy")
          .isSameAs(previous);
      assertThat(recording.bucketsWhenBound).isEqualTo(2);
      assertThat(type.getBucketSelectionStrategy()).isSameAs(recording);

      // LEAVE THE TYPE ON A BUILT-IN STRATEGY SO THE PERSISTED SCHEMA DOES NOT NAME A TEST CLASS
      database.transaction(() -> type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy()));
    }

    @Test
    void strategyRefusedWhileBindingIsNeverPublished() {
      database.transaction(() -> database.getSchema().createDocumentType("Product", 2));

      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Product");
      final BucketSelectionStrategy previous = type.getBucketSelectionStrategy();

      assertThatThrownBy(() -> database.transaction(() -> type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy() {
        @Override
        public void setType(final LocalDocumentType type) {
          throw new IllegalStateException("refused");
        }
      }))).isInstanceOf(IllegalStateException.class).hasMessage("refused");

      assertThat(type.getBucketSelectionStrategy()).isSameAs(previous);
    }

    @Test
    void strategyRefusedBySuitabilityCheckIsNeverPublished() {
      database.transaction(() -> database.getSchema().createDocumentType("Product", 2));

      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Product");
      final BucketSelectionStrategy previous = type.getBucketSelectionStrategy();

      // NO UNIQUE AUTOMATIC INDEX ON `id`, SO checkSuitability() REPORTS A BLOCKER AND THE ASSIGNMENT IS REFUSED AFTER
      // setType(this) HAS ALREADY BOUND THE STRATEGY - THE OTHER EXIT OF THE BLOCK THAT USED TO NEED THE ROLLBACK
      assertThatThrownBy(() -> database.transaction(
          () -> type.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(List.of("id")))))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("cannot find a unique automatic index");

      assertThat(type.getBucketSelectionStrategy()).isSameAs(previous);
    }

    @Test
    void strategyBucketCountMustBeVolatileForTheInPlaceRebind() throws Exception {
      final Field field = ThreadBucketSelectionStrategy.class.getDeclaredField("total");
      assertThat(Modifier.isVolatile(field.getModifiers()))
          .as("addBucketInternal()/removeBucket() rebind the strategy that is ALREADY published, by calling setType() on "
              + "it and making no volatile write afterwards, so the bucket count it caches must carry its own "
              + "happens-before edge to the lock-free readers of getBucketIdByRecord() (issue #7119)")
          .isTrue();
    }

    @Test
    void inPlaceRebindTracksABucketCountThatGrows() {
      database.transaction(() -> database.getSchema().createDocumentType("Product", 2));

      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Product");
      final ThreadBucketSelectionStrategy strategy = new ThreadBucketSelectionStrategy();
      database.transaction(() -> type.setBucketSelectionStrategy(strategy));
      assertThat(readTotal(strategy)).isEqualTo(2);

      database.transaction(
          () -> type.addBucket(database.getSchema().createBucket("Product_extra")));

      assertThat(readTotal(strategy))
          .as("the strategy published on the type is rebound in place when the bucket list grows")
          .isEqualTo(3);
      // AND THE PLACEMENT IT HANDS OUT IS STILL AN INDEX INTO THE CURRENT LIST
      assertThat(strategy.getBucketIdByRecord(null, false)).isBetween(0, 2);

      // LEAVE THE TYPE ON A BUILT-IN STRATEGY SO THE PERSISTED SCHEMA DOES NOT NAME A TEST-LOCAL SETUP
      database.transaction(() -> type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy()));
    }

    private static int readTotal(final ThreadBucketSelectionStrategy strategy) {
      try {
        final Field total = ThreadBucketSelectionStrategy.class.getDeclaredField("total");
        total.setAccessible(true);
        return total.getInt(strategy);
      } catch (final ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * Public with a public no-arg constructor so that, should the schema be reloaded while it is assigned, the strategy
     * can be re-instantiated by name the way {@code setBucketSelectionStrategy(String, Object...)} does.
     */
    public static class RecordingStrategy extends RoundRobinBucketSelectionStrategy {
      BucketSelectionStrategy publishedWhenBound;
      int                     bucketsWhenBound;

      @Override
      public void setType(final LocalDocumentType type) {
        publishedWhenBound = type.getBucketSelectionStrategy();
        bucketsWhenBound = type.getBuckets(false).size();
        super.setType(type);
      }

      @Override
      public String getName() {
        return getClass().getName();
      }
    }
  }

  /**
   * Regression for #7299: every copy-on-write member of the type is published by a volatile write.
   * <p>
   * Issue #7299, and the end of a series: #6678 made two members of {@link LocalDocumentType} volatile, #7033 added
   * two more, #7119 a fifth, and each pass fixed only the member its own issue happened to name.
   * <p>
   * The members of this class are reassigned under the schema mutation lock and read LOCK-FREE by query planning -
   * {@code instanceOf} is called by openCypher label resolution with no database lock held - so every one of them
   * needs the publication edge a volatile write provides. The reflective test below is what closes the series: a
   * seventh field added without it fails here rather than in a sixth issue.
   * <p>
   * Issue #7866: the sweep first reflected over {@code LocalDocumentType.class.getDeclaredFields()} alone, so by
   * construction it could not see a subclass. {@link LocalEdgeType} held two plain booleans, {@code lightweight} and
   * {@code unique}, that {@code ALTER TYPE ... WITH} reassigns while the edge-creation path reads them lock-free - and
   * for a LIGHTWEIGHT type {@code unique} is the only enforcement of the constraint. The sweep now covers the whole
   * family: every class of the schema package that extends {@link LocalDocumentType}, found by scanning the package
   * rather than by a hand-kept list, so a new subclass is swept the day it is added.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaCopyOnWritePublication {
    private Database database;

    @BeforeEach
    public void setUp() {
      database = new DatabaseFactory("./target/databases/test-issue-7299").create();
    }

    @AfterEach
    public void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    /**
     * The sweep, run by the machine rather than by the next reader of the class, over every class of the family.
     */
    @Test
    public void everyMutableReferenceMemberIsPublishedSafely() throws Exception {
      final List<String> offenders = new ArrayList<>();

      for (final Class<?> type : localDocumentTypeFamily())
        for (final Field field : type.getDeclaredFields()) {
          final int modifiers = field.getModifiers();
          if (Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers) || Modifier.isVolatile(modifiers))
            continue;
          offenders.add(type.getSimpleName() + "." + field.getName() + " (" + field.getType().getSimpleName() + ")");
        }

      assertThat(offenders).as(
              "Members of the LocalDocumentType family that a schema mutation reassigns while a lock-free reader walks "
                  + "them must be final (immutable or a concurrent collection) or volatile, so the reader gets a "
                  + "happens-before edge against the write. Add the keyword rather than relaxing this assertion "
                  + "(issues #7299, #7866).")
          .isEmpty();
    }

    /**
     * Issue #9989: the sweep above accepted {@code final} as a proxy for "immutable or concurrent", which a {@code final
     * HashMap} satisfies while being exactly what it exists to refuse: the reference never changes, the map under it does,
     * and a lock-free reader walking it gets no happens-before edge against the writer. So a final member is now judged by
     * what it HOLDS, read from live instances: a map or a collection must be a concurrent or an unmodifiable one. And the
     * sweep reaches the property family too, {@link AbstractProperty} and {@link LocalProperty}, whose constraints
     * {@code ALTER PROPERTY} reassigns while {@code DocumentValidator} reads them on every write with no lock held.
     */
    @Test
    public void everyMemberOfTheTypeAndPropertyFamiliesIsPublishedSafely() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Doc9989");
      database.command("sql", "CREATE VERTEX TYPE Vertex9989");
      database.command("sql", "CREATE EDGE TYPE Edge9989");
      database.command("sql", "CREATE TIMESERIES TYPE Sensor9989 TIMESTAMP ts FIELDS (v DOUBLE)");
      database.command("sql", "CREATE PROPERTY Doc9989.p STRING (regexp '[a-z]+', min 1, max 5, mandatory true)");
      database.getSchema().getType("Doc9989").getProperty("p").setCustomValue("k", "v");

      final List<Object> instances = new ArrayList<>();
      for (final String name : List.of("Doc9989", "Vertex9989", "Edge9989", "Sensor9989"))
        instances.add(database.getSchema().getType(name));
      instances.add(database.getSchema().getType("Doc9989").getProperty("p"));

      final List<String> offenders = new ArrayList<>();
      final Set<Field> swept = new HashSet<>();
      for (final Object instance : instances)
        for (Class<?> type = instance.getClass(); type != null && type != Object.class; type = type.getSuperclass())
          for (final Field field : type.getDeclaredFields()) {
            final int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) || !swept.add(field))
              continue;
            final String named = type.getSimpleName() + "." + field.getName() + " (" + field.getType().getSimpleName() + ")";
            if (!Modifier.isFinal(modifiers)) {
              if (!Modifier.isVolatile(modifiers))
                offenders.add(named + ": neither final nor volatile");
              continue;
            }
            field.setAccessible(true);
            final Object value = field.get(instance);
            if ((value instanceof Map || value instanceof Collection) && !isConcurrentOrUnmodifiable(value))
              offenders.add(named + ": final, but holds a " + value.getClass().getName());
          }

      assertThat(swept).as("the sweep reaches the property family").anyMatch(f -> f.getDeclaringClass() == AbstractProperty.class);
      assertThat(offenders).as(
          "Members that a schema mutation changes while a lock-free reader walks them must be volatile, or final and holding "
              + "a concurrent or an unmodifiable collection: a final reference to a HashMap publishes nothing (issues #7299, "
              + "#7866, #9989).").isEmpty();
    }

    /**
     * Issue #9989: the custom values of a type and of a property are published as a whole map, so a reader holding the map
     * keeps seeing what it read, and null keeps meaning "not set" on both.
     */
    @Test
    public void customValuesArePublishedAsAWholeMap() {
      database.command("sql", "CREATE DOCUMENT TYPE Custom9989");
      database.command("sql", "CREATE PROPERTY Custom9989.p STRING");
      final DocumentType type = database.getSchema().getType("Custom9989");
      final Property property = type.getProperty("p");

      type.setCustomValue("a", "1");
      property.setCustomValue("a", "1");
      final Set<String> typeKeys = type.getCustomKeys();
      final Set<String> propertyKeys = property.getCustomKeys();

      type.setCustomValue("b", "2");
      property.setCustomValue("b", "2");
      assertThat(typeKeys).containsExactly("a");
      assertThat(propertyKeys).containsExactly("a");
      assertThat(type.getCustomKeys()).containsExactlyInAnyOrder("a", "b");
      assertThat(property.getCustomKeys()).containsExactlyInAnyOrder("a", "b");

      // null removes, and asking for a null key answers null
      type.setCustomValue("a", null);
      property.setCustomValue("a", null);
      assertThat(type.getCustomValue("a")).isNull();
      assertThat(property.getCustomValue("a")).isNull();
      assertThat(type.getCustomValue(null)).isNull();
      assertThat(property.getCustomValue(null)).isNull();

      // and the values survive a reopen, which reloads the type's map as a whole
      database.close();
      database = new DatabaseFactory("./target/databases/test-issue-7299").open();
      assertThat(database.getSchema().getType("Custom9989").getCustomValue("b")).isEqualTo("2");
      assertThat(database.getSchema().getType("Custom9989").getProperty("p").getCustomValue("b")).isEqualTo("2");
      assertThat(database.getSchema().getType("Custom9989").getCustomKeys()).containsExactly("b");
    }

    private static boolean isConcurrentOrUnmodifiable(final Object value) {
      final Class<?> type = value.getClass();
      final String name = type.getName();
      return type.getPackageName().equals("java.util.concurrent") || name.startsWith("java.util.Collections$Unmodifiable")
          || name.startsWith("java.util.ImmutableCollections$") || name.startsWith("java.util.Collections$Empty")
          || name.startsWith("java.util.Collections$Singleton");
    }

    /**
     * Issue #7866: the discovery the sweep rests on must actually find the subclasses, or the sweep above degrades
     * back into the single-class check it replaced and passes for the wrong reason.
     */
    @Test
    public void theSweepReachesEverySubclassOfTheFamily() throws Exception {
      assertThat(localDocumentTypeFamily()).contains(LocalDocumentType.class, LocalVertexType.class, LocalEdgeType.class,
          LocalTimeSeriesType.class);
    }

    /**
     * Issue #7866, named explicitly: the two members the issue reported, which the first sweep could not see.
     */
    @Test
    public void edgeTypeFlagsSettableThroughAlterTypeAreVolatile() throws NoSuchFieldException {
      assertThat(Modifier.isVolatile(LocalEdgeType.class.getDeclaredField("lightweight").getModifiers())).isTrue();
      assertThat(Modifier.isVolatile(LocalEdgeType.class.getDeclaredField("unique").getModifiers())).isTrue();
    }

    /**
     * Every class of {@code com.arcadedb.schema} assignable to {@link LocalDocumentType}, read from the directory the
     * package was compiled into. Only the package of the class itself is scanned: the family has no member elsewhere,
     * and a class outside it cannot reach the package-private state the sweep is protecting.
     */
    private static List<Class<?>> localDocumentTypeFamily() throws URISyntaxException, ClassNotFoundException {
      final String packageName = LocalDocumentType.class.getPackageName();
      final URL url = LocalDocumentType.class.getResource(LocalDocumentType.class.getSimpleName() + ".class");
      assertThat(url).isNotNull();
      assertThat(url.getProtocol()).as("the sweep scans the compiled package directory, not a jar").isEqualTo("file");

      final File[] files = new File(url.toURI()).getParentFile().listFiles((dir, name) -> name.endsWith(".class"));
      assertThat(files).isNotNull();

      final List<Class<?>> family = new ArrayList<>();
      for (final File file : files) {
        final String className = packageName + "." + file.getName().substring(0, file.getName().length() - ".class".length());
        final Class<?> type = Class.forName(className, false, LocalDocumentType.class.getClassLoader());
        if (LocalDocumentType.class.isAssignableFrom(type))
          family.add(type);
      }
      return family;
    }

    /**
     * Issue #7866: restoring a TimeSeries type from JSON used to clear and refill the live downsampling tier list in
     * place, under the feet of the maintenance scheduler that iterates it with no lock held. A reader that already
     * holds the list must keep seeing the tiers it read; the restored ones arrive as a new, fully built list.
     */
    @Test
    public void restoringDownsamplingTiersDoesNotMutateTheListReadersHold() {
      database.command("sql", "CREATE TIMESERIES TYPE Sensor7866 TIMESTAMP ts FIELDS (v DOUBLE)");
      database.command("sql", "ALTER TIMESERIES TYPE Sensor7866 ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS");
      final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Sensor7866");

      final List<DownsamplingTier> heldByReader = type.getDownsamplingTiers();
      assertThat(heldByReader).containsExactly(new DownsamplingTier(7L * 86_400_000L, 3_600_000L));

      final JSONObject json = type.toJSON();
      json.put("downsamplingTiers", new JSONArray());
      type.fromJSON(json);

      assertThat(type.getDownsamplingTiers()).isEmpty();
      assertThat(heldByReader).containsExactly(new DownsamplingTier(7L * 86_400_000L, 3_600_000L));

      // And what the getter hands out is not a back door into the published list.
      assertThatThrownBy(() -> heldByReader.add(new DownsamplingTier(1L, 1L))).isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * The half of the issue that needs no concurrency at all.
     */
    @Test
    public void setAliasesDoesNotPublishTheCallersSet() {
      database.getSchema().createDocumentType("Invoice");
      final LocalDocumentType invoice = (LocalDocumentType) database.getSchema().getType("Invoice");

      final Set<String> callerOwned = new HashSet<>(Set.of("Bill"));
      invoice.setAliases(callerOwned);

      // The caller keeps its set and goes on using it. That must not reach live schema state.
      callerOwned.add("Smuggled");

      assertThat(invoice.getAliases()).containsExactly("Bill");
      assertThat(invoice.instanceOf("Smuggled")).isFalse();
      assertThat(invoice.instanceOf("Bill")).isTrue();

      // And what getAliases() hands out is not a back door into it either.
      assertThatThrownBy(() -> invoice.getAliases().add("Smuggled")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void aliasesStillBehaveAsBefore() {
      database.getSchema().createDocumentType("Order");
      final DocumentType order = database.getSchema().getType("Order");

      order.setAliases(Set.of("PurchaseOrder", "PO"));
      assertThat(order.getAliases()).containsExactlyInAnyOrder("PurchaseOrder", "PO");
      assertThat(database.getSchema().getType("PO")).isSameAs(order);

      // Replacing the set deregisters the previous aliases, and the type stops answering to them.
      order.setAliases(Set.of("PO"));
      assertThat(order.getAliases()).containsExactly("PO");
      assertThat(order.instanceOf("PurchaseOrder")).isFalse();
      assertThat(database.getSchema().existsType("PurchaseOrder")).isFalse();

      order.setAliases(Set.of());
      assertThat(order.getAliases()).isEmpty();
      assertThat(database.getSchema().existsType("PO")).isFalse();
    }

    /**
     * The field #7119's closing argument rests on: the volatile write in {@code super.setType} happens BEFORE this
     * one, so it orders nothing here and cannot be borrowed as the publication edge.
     */
    @Test
    public void thePartitionStrategyTypeReferenceIsPublishedSafely() throws NoSuchFieldException {
      final Field type = PartitionedBucketSelectionStrategy.class.getDeclaredField("type");
      assertThat(Modifier.isVolatile(type.getModifiers())).isTrue();
    }
  }
}
