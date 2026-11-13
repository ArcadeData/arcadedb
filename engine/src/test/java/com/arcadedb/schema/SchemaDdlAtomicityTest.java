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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.AlterBucketStatement;
import com.arcadedb.query.sql.parser.AlterDatabaseStatement;
import com.arcadedb.query.sql.parser.AlterGraphAnalyticalViewStatement;
import com.arcadedb.query.sql.parser.AlterMaterializedViewStatement;
import com.arcadedb.query.sql.parser.AlterPropertyStatement;
import com.arcadedb.query.sql.parser.AlterTimeSeriesTypeStatement;
import com.arcadedb.query.sql.parser.AlterTypeStatement;
import com.arcadedb.query.sql.parser.CompactIndexStatement;
import com.arcadedb.query.sql.parser.CompactTimeSeriesTypeStatement;
import com.arcadedb.query.sql.parser.CreateBucketStatement;
import com.arcadedb.query.sql.parser.CreateContinuousAggregateStatement;
import com.arcadedb.query.sql.parser.CreateDocumentTypeStatement;
import com.arcadedb.query.sql.parser.CreateEdgeTypeStatement;
import com.arcadedb.query.sql.parser.CreateGraphAnalyticalViewStatement;
import com.arcadedb.query.sql.parser.CreateIndexStatement;
import com.arcadedb.query.sql.parser.CreateMaterializedViewStatement;
import com.arcadedb.query.sql.parser.CreatePropertyStatement;
import com.arcadedb.query.sql.parser.CreateTimeSeriesTypeStatement;
import com.arcadedb.query.sql.parser.CreateTriggerStatement;
import com.arcadedb.query.sql.parser.CreateVertexTypeStatement;
import com.arcadedb.query.sql.parser.DDLStatement;
import com.arcadedb.query.sql.parser.DropBucketStatement;
import com.arcadedb.query.sql.parser.DropContinuousAggregateStatement;
import com.arcadedb.query.sql.parser.DropGraphAnalyticalViewStatement;
import com.arcadedb.query.sql.parser.DropIndexStatement;
import com.arcadedb.query.sql.parser.DropMaterializedViewStatement;
import com.arcadedb.query.sql.parser.DropPropertyStatement;
import com.arcadedb.query.sql.parser.DropTriggerStatement;
import com.arcadedb.query.sql.parser.DropTypeStatement;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.RebuildGraphAnalyticalViewStatement;
import com.arcadedb.query.sql.parser.RebuildIndexStatement;
import com.arcadedb.query.sql.parser.RebuildTypeStatement;
import com.arcadedb.query.sql.parser.RefreshContinuousAggregateStatement;
import com.arcadedb.query.sql.parser.RefreshMaterializedViewStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.TruncateBucketStatement;
import com.arcadedb.query.sql.parser.TruncateTypeStatement;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Regression tests, one nested class per fixed issue.
 */
class SchemaDdlAtomicityTest {

  /**
   * Regression for #10421: a schema statement that changes several parts of the schema graph kept what it had already done when it was
   * refused part way. {@code DROP TYPE} unlinked the sub types and dropped the indexes one by one before it reached the one that
   * refused, {@code ALTER TYPE ... BUCKET -} detached the bucket before it dropped its sub-indexes, {@code ALTER TYPE ... SUPERTYPE -}
   * severed the link before it dropped the sub-indexes the link had created - and the schema was saved in that state, so the
   * statement was refused AND half done, in memory and on disk.
   * <p>
   * A refusal leaves the schema as it was now: every statement asks what can refuse - the file of an index that cannot be deleted, a
   * file lock another thread holds - before its first change, and takes the locks of every file it is going to drop up front.
   * <p>
   * The failure is injected as {@code TypeRenameTest.RenamedTypeInheritedIndexTest} does, with a file system fault: the file of a sub-index
   * is moved aside and a non-empty directory takes its path, which {@code Files.delete} refuses.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaStatementsAreAtomicTest extends TestHelper {

    /** A directory with something in it where a file of the schema is: it cannot be deleted. */
    private record Fault(Path file, Path aside) {
      void remove() throws IOException {
        Files.deleteIfExists(file.resolve("blocker"));
        Files.deleteIfExists(file);
        Files.move(aside, file);
      }
    }

    private Fault injectFault(final IndexInternal subIndex) throws IOException {
      assumeFalse(System.getProperty("os.name", "").toLowerCase().contains("win"), "an open file cannot be moved on Windows");
      final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getFilePath());
      final Path aside = file.resolveSibling(file.getFileName() + ".aside");
      Files.move(file, aside);
      Files.createDirectory(file);
      Files.writeString(file.resolve("blocker"), "x");
      return new Fault(file, aside);
    }

    // ---- DROP TYPE

    @Test
    void aDropTypeThatCannotDeleteAFileIsRefusedBeforeItChangesAnything() throws IOException {
      createSuperAndSubType();
      insert("T5", "a");
      insert("T10", "b");
      final Fault fault = injectFault(subIndexOn("T10"));
      try {
        assertThatThrownBy(() -> database.command("sql", "DROP TYPE T5")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Nothing was changed");

        assertSchemaAsCreated();
      } finally {
        fault.remove();
      }

      reopenDatabase();

      assertSchemaAsCreated();
      assertThat(count("T5", "a")).isEqualTo(1L);
      assertThat(count("T5", "b")).isEqualTo(1L);

      // and the statement completes once nothing refuses it
      database.command("sql", "DROP TYPE T5");

      assertThat(database.getSchema().existsType("T5")).isFalse();
      assertThat(database.getSchema().getType("T10").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("T10").getAllIndexes(true)).isEmpty();
      assertThat(database.countType("T10", true)).isEqualTo(1L);

      reopenDatabase();

      assertThat(database.getSchema().existsType("T5")).isFalse();
      assertThat(database.getSchema().getType("T10").getSuperTypes()).isEmpty();
      assertThat(database.countType("T10", true)).isEqualTo(1L);
    }

    @Test
    void aDropTypeOfAVertexTypeThatCannotDeleteAFileIsRefusedBeforeItChangesAnything() throws IOException {
      database.command("sql", "CREATE VERTEX TYPE V1");
      database.command("sql", "CREATE PROPERTY V1.p STRING");
      database.command("sql", "CREATE INDEX ON V1 (p) NOTUNIQUE");
      database.command("sql", "CREATE EDGE TYPE E1");
      database.transaction(() -> {
        final var a = database.newVertex("V1").set("p", "a").save();
        final var b = database.newVertex("V1").set("p", "b").save();
        a.newEdge("E1", b);
      });
      final TypeIndex index = database.getSchema().getType("V1").getAllIndexes(false).iterator().next();
      final Fault fault = injectFault(index.getIndexesOnBuckets()[0]);
      try {
        assertThatThrownBy(() -> database.command("sql", "DROP TYPE V1 UNSAFE")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Nothing was changed");

        assertThat(database.getSchema().existsType("V1")).isTrue();
        assertThat(database.getSchema().getType("V1").getAllIndexes(false)).hasSize(1);
        assertThat(database.getSchema().getType("V1").getBuckets(false)).isNotEmpty();
      } finally {
        fault.remove();
      }

      reopenDatabase();

      assertThat(database.getSchema().existsType("V1")).isTrue();
      assertThat(database.query("sql", "SELECT FROM V1 WHERE p = 'a'").stream().count()).isEqualTo(1L);
      assertThat(database.query("sql", "SELECT expand(out('E1')) FROM V1 WHERE p = 'a'").stream().count()).isEqualTo(1L);
    }

    @Test
    @Tag("slow")
    void aDropTypeThatCannotTakeTheLockOfAFileIsRefusedBeforeItChangesAnything() throws Exception {
      createSuperAndSubType();
      insert("T5", "a");
      insert("T10", "b");

      // another thread holds the lock of the file of the sub-index on the bucket of T10, which DROP TYPE T5 has to take
      final List<Integer> locked = new ArrayList<>(subIndexOn("T10").getFileIds());
      final CountDownLatch holding = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final Thread holder = new Thread(() -> ((DatabaseInternal) database).executeLockingFiles(locked, () -> {
        holding.countDown();
        try {
          release.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return null;
      }));
      holder.start();
      assertThat(holding.await(10, TimeUnit.SECONDS)).isTrue();
      try {
        assertThatThrownBy(() -> database.command("sql", "DROP TYPE T5")).isInstanceOf(NeedRetryException.class);

        assertSchemaAsCreated();
      } finally {
        release.countDown();
        holder.join(10_000);
      }

      reopenDatabase();

      assertSchemaAsCreated();
      database.command("sql", "DROP TYPE T5");
      assertThat(database.getSchema().existsType("T5")).isFalse();
      assertThat(database.getSchema().getType("T10").getSuperTypes()).isEmpty();
    }

    // ---- ALTER TYPE ... BUCKET -

    @Test
    void aBucketRemovalThatCannotDeleteAFileIsRefusedBeforeItChangesAnything() throws IOException {
      database.command("sql", "CREATE DOCUMENT TYPE T5 BUCKETS 2");
      database.command("sql", "CREATE PROPERTY T5.p STRING");
      database.command("sql", "CREATE INDEX ON T5 (p) NOTUNIQUE");
      final DocumentType type = database.getSchema().getType("T5");
      final Bucket removed = type.getBuckets(false).getLast();
      final List<Integer> bucketIdsBefore = type.getBucketIds(false);
      final IndexInternal subIndex = subIndexOn(type, removed);
      final Fault fault = injectFault(subIndex);
      try {
        assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T5 BUCKET -" + removed.getName())).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Nothing was changed");

        assertThat(type.hasBucket(removed.getName())).isTrue();
        assertThat(type.getBucketIds(false)).isEqualTo(bucketIdsBefore);
        assertThat(database.getSchema().getTypeByBucketId(removed.getFileId())).isSameAs(type);
        assertThat(subIndexOn(type, removed)).isSameAs(subIndex);
        assertThat(database.getSchema().existsIndex(subIndex.getName())).isTrue();
      } finally {
        fault.remove();
      }

      reopenDatabase();

      final DocumentType reopened = database.getSchema().getType("T5");
      assertThat(reopened.getBucketIds(false)).isEqualTo(bucketIdsBefore);
      assertThat(reopened.getAllIndexes(false).iterator().next().getIndexesOnBuckets()).hasSize(2);

      // and the statement completes once nothing refuses it
      database.command("sql", "ALTER TYPE T5 BUCKET -" + removed.getName());

      assertThat(reopened.getBucketIds(false)).hasSize(1);
      assertThat(reopened.getAllIndexes(false).iterator().next().getIndexesOnBuckets()).hasSize(1);
    }

    // ---- ALTER TYPE ... SUPERTYPE -

    @Test
    void aSuperTypeRemovalThatCannotDeleteAFileKeepsTheLink() throws IOException {
      createSuperAndSubType();
      insert("T5", "a");
      insert("T10", "b");
      final Fault fault = injectFault(subIndexOn("T10"));
      try {
        assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T10 SUPERTYPE -T5")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Nothing was changed");

        assertSchemaAsCreated();
      } finally {
        fault.remove();
      }

      reopenDatabase();

      assertSchemaAsCreated();

      database.command("sql", "ALTER TYPE T10 SUPERTYPE -T5");

      assertThat(database.getSchema().getType("T10").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("T5").getSubTypes()).isEmpty();
      assertThat(count("T5", "b")).isZero();
      assertThat(count("T5", "a")).isEqualTo(1L);
    }

    // ---- ALTER TYPE ... NAME

    @Test
    void aRefusedRenameLeavesTheBucketMapsAsTheyWere() throws IOException {
      createSuperAndSubType();
      insert("T10", "x");
      final Bucket bucket = database.getSchema().getType("T10").getBuckets(false).getFirst();
      final String bucketName = bucket.getName();
      final IndexInternal inherited = subIndexOn("T10");
      final Path blocked = Path.of(((DatabaseInternal) database).getFileManager().getFile(inherited.getFileId()).getFilePath());
      final Path parked = blocked.resolveSibling(blocked.getFileName().toString().replace("T10_0_", "T11_0_"));
      Files.createDirectory(parked);
      try {
        assertThatThrownBy(() -> database.getSchema().getType("T10").rename("T11")).isInstanceOf(SchemaException.class);

        assertThat(database.getSchema().existsType("T10")).isTrue();
        assertThat(database.getSchema().existsType("T11")).isFalse();
        assertThat(database.getSchema().existsBucket(bucketName)).isTrue();
        assertThat(database.getSchema().getBucketByName(bucketName)).isSameAs(bucket);
        assertThat(database.getSchema().getTypeByBucketId(bucket.getFileId()).getName()).isEqualTo("T10");
        assertThat(database.getSchema().getTypeByBucketName(bucketName).getName()).isEqualTo("T10");
      } finally {
        Files.deleteIfExists(parked);
      }

      reopenDatabase();

      assertThat(database.getSchema().existsType("T10")).isTrue();
      assertThat(count("T5", "x")).isEqualTo(1L);
    }

    // ---- helpers

    /** T5 and T10 EXTENDS T5 with the index of T5 over the buckets of both, as {@link #createSuperAndSubType} built them. */
    private void assertSchemaAsCreated() {
      final DocumentType t5 = database.getSchema().getType("T5");
      final DocumentType t10 = database.getSchema().getType("T10");
      assertThat(t10.getSuperTypes()).containsExactly(t5);
      assertThat(t5.getSubTypes()).containsExactly(t10);
      assertThat(t5.getAllIndexes(false)).hasSize(1);
      final TypeIndex index = t5.getAllIndexes(false).iterator().next();
      assertThat(index.getIndexesOnBuckets()).hasSize(2);
      assertThat(t10.getAllIndexes(true)).containsExactly(index);
      for (final IndexInternal subIndex : index.getIndexesOnBuckets())
        assertThat(database.getSchema().existsIndex(subIndex.getName())).isTrue();
      assertThat(database.getSchema().getTypeByBucketId(t10.getBuckets(false).getFirst().getFileId())).isSameAs(t10);
      assertThat(t5.getBucketIds(true)).hasSize(2);
      assertThat(t10.getBucketIds(true)).hasSize(1);
      assertThat(t10.getPolymorphicPropertyNames()).contains("p");
      assertThat(t10.isSubTypeOf("T5")).isTrue();
    }

    private void createSuperAndSubType() {
      database.command("sql", "CREATE DOCUMENT TYPE T5");
      database.command("sql", "CREATE PROPERTY T5.p STRING");
      database.command("sql", "CREATE INDEX ON T5 (p) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE T10 EXTENDS T5");
    }

    private void insert(final String typeName, final String value) {
      database.transaction(() -> database.newDocument(typeName).set("p", value).save());
    }

    private long count(final String typeName, final String value) {
      return database.query("sql", "SELECT FROM " + typeName + " WHERE p = ?", value).stream().count();
    }

    private IndexInternal subIndexOn(final String typeName) {
      final DocumentType type = database.getSchema().getType(typeName);
      return subIndexOn(type, type.getBuckets(false).getFirst());
    }

    private IndexInternal subIndexOn(final DocumentType type, final Bucket bucket) {
      for (final DocumentType candidate : database.getSchema().getTypes())
        for (final TypeIndex index : candidate.getAllIndexes(false))
          for (final IndexInternal subIndex : index.getIndexesOnBuckets())
            if (subIndex.getAssociatedBucketId() == bucket.getFileId())
              return subIndex;
      throw new AssertionError("no sub-index on the bucket " + bucket.getName() + " of " + type.getName());
    }
  }

  /**
   * Regression for #10734: {@code DROP INDEX *} dropped the indexes one {@code dropIndex()} call at a time. Each call is all or nothing (issue
   * #10589), the statement was not: a file of the Nth index that could not be deleted refused the statement with "Nothing was
   * changed" after the indexes before it were gone, and one of them could be left covering a subset of its buckets, across a reopen.
   * <p>
   * The rule is per STATEMENT: a schema statement that is refused changes nothing. This census reads the DDL statements from the
   * parser's own classes, so a statement added there fails {@link #everyDdlStatementIsClassified()} until it says whether it can
   * delete the file of an index; every one that can is run against a file that cannot be deleted, on each index it touches in
   * turn, and the whole schema - its JSON and every index's sub-indexes and entries - is compared with what it was, after the
   * refusal and again after a reopen. The message alone proves nothing: the per-call probe emits it whether or not the statement
   * changed something before it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RefusedDdlChangesNothingCensusTest extends TestHelper {

    private static final int BUCKETS = 4;
    private static final int RECORDS = 12;

    /** A statement that deletes index files, and how to find the sub-index whose file refuses, among those it deletes. */
    private record Scenario(String statement, Function<DatabaseInternal, List<IndexInternal>> refusingCandidates) {
    }

    /** The statements of the parser that can delete the file of an index, with the cases each is run with. */
    private static final Map<Class<?>, List<Scenario>> DELETES_INDEX_FILES = new HashMap<>();
    /** The statements that cannot, and why: what they delete, if anything, is not an index file. */
    private static final Map<Class<?>, String>          DELETES_NO_INDEX_FILE = new HashMap<>();

    static {
      DELETES_INDEX_FILES.put(DropIndexStatement.class, List.of(//
          new Scenario("DROP INDEX `B[p]`", db -> subIndexes(db, "B[p]")),//
          new Scenario("DROP INDEX *", db -> {
            final List<IndexInternal> all = new ArrayList<>(subIndexes(db, "A[p]"));
            all.addAll(subIndexes(db, "B[p]"));
            return all;
          })));
      DELETES_INDEX_FILES.put(DropPropertyStatement.class, List.of(new Scenario("DROP PROPERTY B.p FORCE", db -> subIndexes(db, "B[p]"))));
      DELETES_INDEX_FILES.put(DropTypeStatement.class, List.of(//
          new Scenario("DROP TYPE B", db -> subIndexes(db, "B[p]")),//
          new Scenario("DROP TYPE C", db -> subIndexesOnBucketsOf(db, "A[p]", "C"))));
      DELETES_INDEX_FILES.put(AlterTypeStatement.class, List.of(//
          new Scenario("ALTER TYPE B BUCKET -B_1", db -> subIndexesOnBucket(db, "B[p]", "B_1")),//
          new Scenario("ALTER TYPE C SUPERTYPE -A", db -> subIndexesOnBucketsOf(db, "A[p]", "C"))));
      DELETES_INDEX_FILES.put(TruncateTypeStatement.class, List.of(new Scenario("TRUNCATE TYPE B", db -> subIndexes(db, "B[p]"))));
      DELETES_INDEX_FILES.put(RebuildIndexStatement.class, List.of(new Scenario("REBUILD INDEX `B[p]`", db -> subIndexes(db, "B[p]"))));

      DELETES_NO_INDEX_FILE.put(AlterBucketStatement.class, "changes a bucket's name or settings");
      DELETES_NO_INDEX_FILE.put(AlterDatabaseStatement.class, "changes a database setting");
      DELETES_NO_INDEX_FILE.put(AlterGraphAnalyticalViewStatement.class, "changes the definition of a graph analytical view");
      DELETES_NO_INDEX_FILE.put(AlterMaterializedViewStatement.class, "changes the refresh mode of a materialized view");
      DELETES_NO_INDEX_FILE.put(AlterPropertyStatement.class, "changes a property's attributes, never its indexes");
      DELETES_NO_INDEX_FILE.put(AlterTimeSeriesTypeStatement.class, "changes the policies of a time series type");
      DELETES_NO_INDEX_FILE.put(CompactIndexStatement.class,
          "swaps an index's content for an equivalent compacted file, no statement-level refusal of a schema change");
      DELETES_NO_INDEX_FILE.put(CompactTimeSeriesTypeStatement.class, "rewrites the shards of a time series engine");
      DELETES_NO_INDEX_FILE.put(CreateBucketStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateContinuousAggregateStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateDocumentTypeStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateEdgeTypeStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateVertexTypeStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateGraphAnalyticalViewStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateIndexStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateMaterializedViewStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreatePropertyStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateTimeSeriesTypeStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(CreateTriggerStatement.class, "creates");
      DELETES_NO_INDEX_FILE.put(DropBucketStatement.class, "refuses a bucket a type owns, and a bucket no type owns has no index");
      DELETES_NO_INDEX_FILE.put(DropContinuousAggregateStatement.class,
          "drops its backing type through Schema.dropType, the DROP TYPE path the census runs (issue #10590)");
      DELETES_NO_INDEX_FILE.put(DropMaterializedViewStatement.class,
          "drops its backing type through Schema.dropType, the DROP TYPE path the census runs (issue #10590)");
      DELETES_NO_INDEX_FILE.put(DropGraphAnalyticalViewStatement.class, "deletes the files of a graph analytical view");
      DELETES_NO_INDEX_FILE.put(DropTriggerStatement.class, "unregisters a trigger");
      DELETES_NO_INDEX_FILE.put(RebuildGraphAnalyticalViewStatement.class, "rebuilds a graph analytical view");
      DELETES_NO_INDEX_FILE.put(RebuildTypeStatement.class, "rewrites records, the indexes follow each record");
      DELETES_NO_INDEX_FILE.put(RefreshContinuousAggregateStatement.class, "rewrites the rows of a continuous aggregate");
      DELETES_NO_INDEX_FILE.put(RefreshMaterializedViewStatement.class, "rewrites the rows of a materialized view in place");
      DELETES_NO_INDEX_FILE.put(TruncateBucketStatement.class, "deletes records, the indexes follow each record");
    }

    /** A directory with something in it where a file of the schema is: it cannot be deleted. */
    private record Fault(Path file, Path aside) {
      void remove() throws IOException {
        Files.deleteIfExists(file.resolve("blocker"));
        Files.deleteIfExists(file);
        Files.move(aside, file);
      }
    }

    @Test
    void everyDdlStatementIsClassified() throws Exception {
      final Set<Class<?>> classified = new HashSet<>(DELETES_INDEX_FILES.keySet());
      assertThat(classified).as("a statement is in one list only").doesNotContainAnyElementsOf(DELETES_NO_INDEX_FILE.keySet());
      classified.addAll(DELETES_NO_INDEX_FILE.keySet());

      assertThat(ddlStatementsOfTheParser()).as(
              "every DDL statement of the parser says whether it can delete an index file: a new one goes in DELETES_INDEX_FILES with a "
                  + "scenario, or in DELETES_NO_INDEX_FILE with the reason")
          .containsExactlyInAnyOrderElementsOf(classified);
    }

    // one fixture per statement and per index file it can be refused by: several seconds in all
    @Test
    @Tag("slow")
    void aRefusedStatementThatDeletesIndexFilesChangesNothing() throws IOException {
      for (final List<Scenario> scenarios : DELETES_INDEX_FILES.values())
        for (final Scenario scenario : scenarios) {
          createFixture();
          final int candidates = scenario.refusingCandidates().apply((DatabaseInternal) database).size();
          assertThat(candidates).as("%s deletes at least one index file", scenario.statement()).isPositive();
          dropFixture();

          for (int refusing = 0; refusing < candidates; refusing++)
            refusedStatementChangesNothing(scenario, refusing);
        }
    }

    /**
     * {@code REBUILD INDEX *} is the bulk repair: it rebuilds index after index, records the ones it cannot rebuild and goes on, so a
     * refusal is not the refusal of the statement. What it owes is the per-index rule: the index it could not rebuild is left whole,
     * and so is every other.
     */
    @Test
    void aRebuildOfEveryIndexThatCannotRebuildOneLeavesEveryIndexWhole() throws IOException {
      createFixture();
      final Map<String, String> before = indexesSnapshot();
      final Fault fault = injectFault(subIndexes((DatabaseInternal) database, "B[p]").get(1));
      try {
        try (final ResultSet rs = database.command("sql", "REBUILD INDEX *")) {
          final Result row = rs.next();
          assertThat(row.<List<String>>getProperty("failedIndexes")).isNotEmpty();
        }
        assertThat(indexesSnapshot()).isEqualTo(before);
      } finally {
        fault.remove();
      }
      reopenDatabase();
      assertThat(indexesSnapshot()).isEqualTo(before);
      dropFixture();
    }

    @Test
    void aRebuildOfEveryIndexKeepsEveryIndexWhole() {
      createFixture();
      final Map<String, String> before = indexesSnapshot();
      try (final ResultSet rs = database.command("sql", "REBUILD INDEX *")) {
        assertThat(rs.next().<List<String>>getProperty("failedIndexes")).isNull();
      }
      assertThat(indexesSnapshot()).isEqualTo(before);
      reopenDatabase();
      assertThat(indexesSnapshot()).isEqualTo(before);
      dropFixture();
    }

    @Test
    void dropIndexStarDropsEveryIndexOnceNothingRefuses() {
      createFixture();
      try (final ResultSet rs = database.command("sql", "DROP INDEX *")) {
        final List<String> names = new ArrayList<>();
        rs.forEachRemaining(r -> names.add(r.getProperty("indexName")));
        assertThat(names).contains("A[p]", "B[p]");
      }
      assertThat(database.getSchema().getIndexes()).isEmpty();
      for (final String type : new String[] { "A", "B", "C" })
        assertThat(database.getSchema().getType(type).getAllIndexes(true)).isEmpty();
      assertThat(database.countType("A", true)).isEqualTo(RECORDS * 2L);

      reopenDatabase();
      assertThat(database.getSchema().getIndexes()).isEmpty();
      dropFixture();
    }

    // ---- helpers

    private void refusedStatementChangesNothing(final Scenario scenario, final int refusing) throws IOException {
      createFixture();
      final String before = snapshot();
      final IndexInternal refusingSubIndex = scenario.refusingCandidates().apply((DatabaseInternal) database).get(refusing);
      final Fault fault = injectFault(refusingSubIndex);
      try {
        assertThatThrownBy(() -> database.command("sql", scenario.statement())).as("%s refused by %s", scenario.statement(),
            refusingSubIndex.getName()).hasStackTraceContaining("Nothing was changed");

        assertThat(snapshot()).as("%s refused by %s", scenario.statement(), refusingSubIndex.getName()).isEqualTo(before);
      } finally {
        fault.remove();
      }

      reopenDatabase();
      assertThat(snapshot()).as("%s refused by %s, after a reopen", scenario.statement(), refusingSubIndex.getName()).isEqualTo(before);
      dropFixture();
    }

    /** The whole schema as it is persisted, and every index with its sub-indexes and the entries it holds. */
    private String snapshot() {
      final JSONObject json = database.getSchema().getEmbedded().toJSON();
      json.remove("schemaVersion");
      return json.toString() + "\n" + indexesSnapshot();
    }

    private Map<String, String> indexesSnapshot() {
      final Map<String, String> indexes = new TreeMap<>();
      for (final DocumentType type : database.getSchema().getTypes())
        for (final TypeIndex index : type.getAllIndexes(false)) {
          final List<Integer> buckets = new ArrayList<>();
          for (final IndexInternal subIndex : index.getIndexesOnBuckets()) {
            buckets.add(subIndex.getAssociatedBucketId());
            assertThat(database.getSchema().existsIndex(subIndex.getName())).as("sub-index %s is registered", subIndex.getName()).isTrue();
          }
          buckets.sort(Integer::compare);
          indexes.put(index.getName(), "buckets=" + buckets + " entries=" + index.countEntries());
        }
      return indexes;
    }

    private Fault injectFault(final IndexInternal subIndex) throws IOException {
      assumeFalse(System.getProperty("os.name", "").toLowerCase().contains("win"), "an open file cannot be moved on Windows");
      final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getFilePath());
      final Path aside = file.resolveSibling(file.getFileName() + ".aside");
      Files.move(file, aside);
      Files.createDirectory(file);
      Files.writeString(file.resolve("blocker"), "x");
      return new Fault(file, aside);
    }

    /** A and B with an index each, C a sub type of A: the index of A has sub-indexes on the buckets of C too. */
    private void createFixture() {
      database.command("sql", "CREATE DOCUMENT TYPE A BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY A.p STRING");
      database.command("sql", "CREATE INDEX ON A (p) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE B BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY B.p STRING");
      database.command("sql", "CREATE INDEX ON B (p) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS A BUCKETS 2");
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++) {
          database.newDocument("A").set("p", "a" + i).save();
          database.newDocument("B").set("p", "b" + i).save();
          database.newDocument("C").set("p", "c" + i).save();
        }
      });
    }

    private void dropFixture() {
      for (final String type : new String[] { "C", "A", "B" })
        if (database.getSchema().existsType(type))
          database.command("sql", "DROP TYPE " + type + " UNSAFE");
    }

    private static List<IndexInternal> subIndexes(final DatabaseInternal db, final String indexName) {
      return List.of(((TypeIndex) db.getSchema().getIndexByName(indexName)).getIndexesOnBuckets());
    }

    private static List<IndexInternal> subIndexesOnBucket(final DatabaseInternal db, final String indexName, final String bucketName) {
      final int bucketId = db.getSchema().getBucketByName(bucketName).getFileId();
      return subIndexes(db, indexName).stream().filter(s -> s.getAssociatedBucketId() == bucketId).toList();
    }

    private static List<IndexInternal> subIndexesOnBucketsOf(final DatabaseInternal db, final String indexName, final String typeName) {
      final List<Integer> bucketIds = db.getSchema().getType(typeName).getBucketIds(false);
      return subIndexes(db, indexName).stream().filter(s -> bucketIds.contains(s.getAssociatedBucketId())).toList();
    }

    /** The concrete {@link DDLStatement} classes of the parser package, read from its compiled classes rather than from a list. */
    private static Set<Class<?>> ddlStatementsOfTheParser() throws Exception {
      final String packageName = DDLStatement.class.getPackageName();
      final URI uri = DDLStatement.class.getResource(DDLStatement.class.getSimpleName() + ".class").toURI();
      final Set<Class<?>> statements = new HashSet<>();
      if ("jar".equals(uri.getScheme())) {
        try (final FileSystem fs = FileSystems.newFileSystem(uri, Map.of())) {
          collectDdlStatements(fs.getPath(packageName.replace('.', '/')), packageName, statements);
        }
      } else
        collectDdlStatements(Path.of(uri).getParent(), packageName, statements);
      return statements;
    }

    private static void collectDdlStatements(final Path directory, final String packageName, final Set<Class<?>> statements)
        throws Exception {
      final List<String> classNames;
      try (final Stream<Path> files = Files.list(directory)) {
        classNames = files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".class") && !n.contains("$"))
            .map(n -> packageName + "." + n.substring(0, n.length() - ".class".length())).toList();
      }
      for (final String className : classNames) {
        final Class<?> c = Class.forName(className, false, DDLStatement.class.getClassLoader());
        if (DDLStatement.class.isAssignableFrom(c) && !Modifier.isAbstract(c.getModifiers()))
          statements.add(c);
      }
    }
  }

  /**
   * Regression for #10590: {@code DROP MATERIALIZED VIEW} and {@code DROP CONTINUOUS AGGREGATE} unregistered the view (and cancelled its
   * refresh scheduler, and took its listeners off the source types) BEFORE they dropped the backing type, which is the step that can
   * refuse. When it refused, the statement failed with a message saying "Nothing was changed" and left a view that no longer
   * existed beside a backing type that did, holding the last rows the view computed and never updating again; no statement could
   * repair it but {@code DROP TYPE} of the backing type.
   * <p>
   * A refused schema statement changes nothing (#10421): every refusal the drop of the backing type can give - a file that cannot be
   * deleted, another view or aggregate built from the backing type - is now asked before the first change.
   * <p>
   * The file fault is injected as {@code SchemaDdlAtomicityTest.SchemaStatementsAreAtomicTest} does: the file of a bucket of the backing type is
   * moved aside and a non-empty directory takes its path, which {@code Files.delete} refuses.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DropViewRefusalChangesNothingTest extends TestHelper {

    /** A directory with something in it where a file of the schema is: it cannot be deleted. */
    private record Fault(Path file, Path aside) {
      void remove() throws IOException {
        Files.deleteIfExists(file.resolve("blocker"));
        Files.deleteIfExists(file);
        Files.move(aside, file);
      }
    }

    private Fault injectFault(final String backingTypeName) throws IOException {
      assumeFalse(System.getProperty("os.name", "").toLowerCase().contains("win"), "an open file cannot be moved on Windows");
      final Bucket bucket = database.getSchema().getType(backingTypeName).getBuckets(false).getLast();
      final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(bucket.getFileId()).getFilePath());
      final Path aside = file.resolveSibling(file.getFileName() + ".aside");
      Files.move(file, aside);
      Files.createDirectory(file);
      Files.writeString(file.resolve("blocker"), "x");
      return new Fault(file, aside);
    }

    // ---- DROP MATERIALIZED VIEW

    @Test
    void aDropOfAViewWhoseBackingFileCannotBeDeletedIsRefusedBeforeItChangesAnything() throws IOException {
      createSource();
      database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT k, sum(v) AS s FROM S GROUP BY k");
      final String schemaBefore = schemaWithoutVersion();
      assertThat(viewRows("MV")).isEqualTo(3L);

      final Fault fault = injectFault("MV");
      try {
        assertThatThrownBy(() -> database.command("sql", "DROP MATERIALIZED VIEW MV")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Nothing was changed");

        assertThat(database.getSchema().existsMaterializedView("MV")).isTrue();
        assertThat(database.getSchema().existsType("MV")).isTrue();
        assertThat(schemaWithoutVersion()).isEqualTo(schemaBefore);
        assertThat(viewRows("MV")).isEqualTo(3L);
      } finally {
        fault.remove();
      }

      reopenDatabase();

      assertThat(database.getSchema().existsMaterializedView("MV")).isTrue();
      assertThat(viewRows("MV")).isEqualTo(3L);

      // and the statement completes once nothing refuses it
      database.command("sql", "DROP MATERIALIZED VIEW MV");

      assertThat(database.getSchema().existsMaterializedView("MV")).isFalse();
      assertThat(database.getSchema().existsType("MV")).isFalse();
    }

    @Test
    void aRefusedDropOfAnIncrementalViewKeepsItsListenersOnTheSource() throws IOException {
      createSource();
      database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT k, v FROM S REFRESH INCREMENTAL");
      assertThat(viewRows("MV")).isEqualTo(10L);

      final Fault fault = injectFault("MV");
      try {
        assertThatThrownBy(() -> database.command("sql", "DROP MATERIALIZED VIEW MV")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Nothing was changed");

        // the view is still maintained from the source: a refused drop must not leave a view that never updates again
        database.transaction(() -> database.newDocument("S").set("k", "z").set("v", 99).save());
        assertThat(viewRows("MV")).isEqualTo(11L);
      } finally {
        fault.remove();
      }

      database.command("sql", "DROP MATERIALIZED VIEW MV");
      assertThat(database.getSchema().existsType("MV")).isFalse();
    }

    @Test
    void aViewAnotherViewIsBuiltFromCannotBeDroppedAndStaysRegistered() {
      createSource();
      database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT k, v FROM S");
      database.command("sql", "CREATE MATERIALIZED VIEW MV2 AS SELECT k, v FROM MV");
      final String schemaBefore = schemaWithoutVersion();

      // the backing type of MV is a source type of MV2: dropping it is refused, and the refusal comes after the view was unregistered
      assertThatThrownBy(() -> database.command("sql", "DROP MATERIALIZED VIEW MV")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("MV2");

      assertThat(database.getSchema().existsMaterializedView("MV")).isTrue();
      assertThat(database.getSchema().existsType("MV")).isTrue();
      assertThat(schemaWithoutVersion()).isEqualTo(schemaBefore);

      // dropping the dependant first lets the drop through
      database.command("sql", "DROP MATERIALIZED VIEW MV2");
      database.command("sql", "DROP MATERIALIZED VIEW MV");
      assertThat(database.getSchema().existsType("MV")).isFalse();
    }

    @Test
    void aPlainDropStillRemovesTheViewAndItsBackingType() {
      // the drop of the backing type is not refused by the view being dropped, which still names that type when the drop is asked
      createSource();
      database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT k, v FROM S");

      database.command("sql", "DROP MATERIALIZED VIEW MV");

      assertThat(database.getSchema().existsMaterializedView("MV")).isFalse();
      assertThat(database.getSchema().existsType("MV")).isFalse();
      assertThat(database.getSchema().existsType("S")).isTrue();
    }

    // ---- DROP CONTINUOUS AGGREGATE

    @Test
    void aDropOfAnAggregateWhoseBackingFileCannotBeDeletedIsRefusedBeforeItChangesAnything() throws IOException {
      createTimeSeriesSource();
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE CA AS SELECT ts.timeBucket('1h', ts) AS h, sum(v) AS s FROM TS GROUP BY h");
      final String schemaBefore = schemaWithoutVersion();
      assertThat(viewRows("CA")).isEqualTo(1L);

      final Fault fault = injectFault("CA");
      try {
        assertThatThrownBy(() -> database.command("sql", "DROP CONTINUOUS AGGREGATE CA")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Nothing was changed");

        assertThat(database.getSchema().existsContinuousAggregate("CA")).isTrue();
        assertThat(database.getSchema().existsType("CA")).isTrue();
        assertThat(schemaWithoutVersion()).isEqualTo(schemaBefore);
        assertThat(viewRows("CA")).isEqualTo(1L);
      } finally {
        fault.remove();
      }

      reopenDatabase();

      assertThat(database.getSchema().existsContinuousAggregate("CA")).isTrue();
      assertThat(viewRows("CA")).isEqualTo(1L);

      // and the statement completes once nothing refuses it
      database.command("sql", "DROP CONTINUOUS AGGREGATE CA");

      assertThat(database.getSchema().existsContinuousAggregate("CA")).isFalse();
      assertThat(database.getSchema().existsType("CA")).isFalse();
    }

    @Test
    void anAggregateAViewIsBuiltFromCannotBeDroppedAndStaysRegistered() {
      createTimeSeriesSource();
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE CA AS SELECT ts.timeBucket('1h', ts) AS h, sum(v) AS s FROM TS GROUP BY h");
      database.command("sql", "CREATE MATERIALIZED VIEW MVC AS SELECT h, s FROM CA");
      final String schemaBefore = schemaWithoutVersion();

      assertThatThrownBy(() -> database.command("sql", "DROP CONTINUOUS AGGREGATE CA")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("MVC");

      assertThat(database.getSchema().existsContinuousAggregate("CA")).isTrue();
      assertThat(database.getSchema().existsType("CA")).isTrue();
      assertThat(schemaWithoutVersion()).isEqualTo(schemaBefore);

      database.command("sql", "DROP MATERIALIZED VIEW MVC");
      database.command("sql", "DROP CONTINUOUS AGGREGATE CA");
      assertThat(database.getSchema().existsType("CA")).isFalse();
    }

    // ---- helpers

    /** The schema as it is written, without the version it bumps at every save. */
    private String schemaWithoutVersion() {
      final JSONObject json = ((LocalSchema) database.getSchema()).toJSON();
      json.remove("schemaVersion");
      return json.toString();
    }

    private long viewRows(final String name) {
      return database.query("sql", "SELECT FROM " + name).stream().count();
    }

    private void createSource() {
      database.command("sql", "CREATE DOCUMENT TYPE S BUCKETS 2");
      database.command("sql", "CREATE PROPERTY S.k STRING");
      database.command("sql", "CREATE PROPERTY S.v INTEGER");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("S").set("k", "k" + (i % 3)).set("v", i).save();
      });
    }

    private void createTimeSeriesSource() {
      database.command("sql", "CREATE TIMESERIES TYPE TS TIMESTAMP ts TAGS (sensor STRING) FIELDS (v DOUBLE)");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.command("sql", "INSERT INTO TS SET ts = ?, sensor = 'a', v = ?", 1_700_000_000_000L + i * 1000L, i * 1.0);
      });
    }
  }

  /**
   * Regression for #6990: a DDL script has to replicate as ONE Raft entry, not one per statement.
   * <p>
   * The Raft entry boundary is the outermost {@code recordFileChanges} session on the calling thread, and every DDL
   * entry point - {@code TypeBuilder.create()}, {@code createProperty()}, {@code TypeIndexBuilder.create()} - opens one.
   * Nesting is the only batching that exists, and nothing opened an outer frame across statements: a script is not a
   * session, and neither is a transaction. So {@code CREATE TYPE; CREATE PROPERTY; CREATE PROPERTY; CREATE INDEX} was
   * four entries, four serialized copies of the full schema and four synchronous quorum round trips, each taken with the
   * database write lock held. #6982 measured that at roughly three hours for a 1209-type schema.
   * <p>
   * WHAT IS COUNTED HERE, and why it is the right number even though no Raft server is running. These tests interpose a
   * counting {@link DatabaseInternal} on {@code LocalDatabase.setWrappedDatabaseInstance} - the same seam the HA server
   * uses to install {@code RaftReplicatedDatabase} - and count only the OUTERMOST {@code recordFileChanges} calls, i.e.
   * the ones taken while no session is open on the thread. That is precisely the set of calls
   * {@code RaftReplicatedDatabase.recordFileChanges} does not delegate away, and each one of them publishes exactly one
   * {@code SCHEMA_ENTRY}. Counting them in the engine keeps the assertion exact and deterministic; the end-to-end
   * follower-side count lives in {@code HaSchemaDeltaReplicationIT}.
   */
  @Nested
  class BulkSchemaScopeTest extends TestHelper {

    /**
     * Sessions opened by the last {@link #countSchemaSessions(Runnable)} call, readable even when the work threw. That is
     * the only way to measure a script that fails part way, which is the case with the most to say about the scope.
     */
    private int lastSessionCount = -1;

    /**
     * Runs {@code work} with the counting database installed (see {@link SchemaSessionCounter}), and returns how many outermost
     * sessions it opened. A failure propagates; the count is still available in {@link #lastSessionCount}.
     */
    private int countSchemaSessions(final Runnable work) {
      final SchemaSessionCounter.Count seen = new SchemaSessionCounter.Count();
      try {
        return SchemaSessionCounter.count((LocalDatabase) database, seen, work);
      } finally {
        lastSessionCount = seen.sessions();
      }
    }

    /**
     * The headline: the four-statement script from the issue publishes once, and the same four statements sent one at a
     * time still publish four times. The control run is what makes the first number mean something - without it a
     * regression that stopped recording sessions altogether would read as a pass.
     */
    @Test
    void aDdlOnlyScriptOpensOneSessionWhileTheSameStatementsSentSeparatelyOpenFour() {
      final int batched = countSchemaSessions(() -> database.command("sqlscript", """
          CREATE VERTEX TYPE Foo;
          CREATE PROPERTY Foo.a STRING;
          CREATE PROPERTY Foo.b STRING;
          CREATE INDEX ON Foo (a) UNIQUE;
          """));

      assertThat(batched).as("a DDL-only script must publish as ONE schema entry, not one per statement").isEqualTo(1);

      final int separate = countSchemaSessions(() -> {
        database.command("sql", "CREATE VERTEX TYPE Bar");
        database.command("sql", "CREATE PROPERTY Bar.a STRING");
        database.command("sql", "CREATE PROPERTY Bar.b STRING");
        database.command("sql", "CREATE INDEX ON Bar (a) UNIQUE");
      });

      assertThat(separate).as("statements sent one at a time still open one session each - the batching is the script's")
          .isEqualTo(4);

      // The batch has to produce the same schema as the four separate statements, or the entry it saved is worthless.
      for (final String typeName : new String[] { "Foo", "Bar" }) {
        final DocumentType type = database.getSchema().getType(typeName);
        assertThat(type.getPropertyNames()).contains("a", "b");
        assertThat(type.getPropertyIfExists("a").getType()).isEqualTo(Type.STRING);
        assertThat(database.getSchema().getIndexByName(typeName + "[a]")).isNotNull();
      }
    }

    /**
     * The batch survives a reopen: one entry has to carry a schema that is actually on disk, not one that only ever
     * existed in the leader's heap.
     */
    @Test
    void theBatchedSchemaIsPersisted() {
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Persisted;
          CREATE PROPERTY Persisted.a STRING;
          CREATE PROPERTY Persisted.b INTEGER;
          CREATE INDEX ON Persisted (a) NOTUNIQUE;
          """);

      reopenDatabase();

      final DocumentType type = database.getSchema().getType("Persisted");
      assertThat(type.getPropertyNames()).contains("a", "b");
      assertThat(type.getPropertyIfExists("b").getType()).isEqualTo(Type.INTEGER);
      assertThat(database.getSchema().existsIndex("Persisted[a]")).isTrue();
    }

    /**
     * The failure contract. Schema DDL has no rollback, so a batch that throws part way cannot be undone on the node
     * that ran it; what the scope must guarantee is that the two sides do not disagree. The prefix that succeeded is
     * published - one session, so one entry, and it is saved to disk - and the exception still reaches the caller.
     * <p>
     * Letting the exception escape the session instead would abort it before it published anything: the followers would
     * hold none of the batch while this node kept the prefix in its in-memory schema, which is a divergence the
     * per-statement path never produces.
     */
    @Test
    void aScriptThatFailsHalfWayPublishesThePrefixItCompletedAndStillThrows() {
      assertThatThrownBy(() -> countSchemaSessions(() -> database.command("sqlscript", """
          CREATE VERTEX TYPE Half;
          CREATE PROPERTY Half.a STRING;
          CREATE PROPERTY NoSuchTypeHere.b STRING;
          CREATE PROPERTY Half.c STRING;
          """)))
          .as("the statement that failed is still what the caller is told about")
          .hasMessageContaining("NoSuchTypeHere");

      assertThat(lastSessionCount)
          .as("the prefix that succeeded is published by the ONE session the script opened, not by one per statement")
          .isEqualTo(1);

      // Exactly what a script of four separate statements would have left, on every node: the prefix, published once.
      reopenDatabase();
      final DocumentType type = database.getSchema().getType("Half");
      assertThat(type.getPropertyNames()).as("the prefix that succeeded is durable").contains("a");
      assertThat(type.getPropertyNames()).as("nothing after the failing statement ran").doesNotContain("c");
    }

    /**
     * A script is batched only when it is schema definition and nothing else: one DML statement in it and every
     * statement goes back to opening its own session, because the write lock cannot be held across a data load.
     */
    @Test
    void aScriptThatMixesInDmlIsNotBatched() {
      final int sessions = countSchemaSessions(() -> database.command("sqlscript", """
          CREATE DOCUMENT TYPE Mixed;
          CREATE PROPERTY Mixed.a STRING;
          BEGIN;
          INSERT INTO Mixed SET a = 'x';
          COMMIT;
          """));

      assertThat(sessions).as("a script carrying DML keeps one session per DDL statement").isEqualTo(2);
      assertThat(database.query("sql", "SELECT FROM Mixed").stream().count()).isEqualTo(1);
    }

    /**
     * A single-statement script is left exactly as it was: it already opened one session, and wrapping it would add a
     * frame that changes nothing.
     */
    @Test
    void aSingleStatementScriptIsUnchanged() {
      final int sessions = countSchemaSessions(() -> database.command("sqlscript", "CREATE DOCUMENT TYPE Lonely;"));
      assertThat(sessions).isEqualTo(1);
      assertThat(database.getSchema().existsType("Lonely")).isTrue();
    }

    /**
     * The escape hatch works: with {@code arcadedb.schemaBulkDDLScript} off, the same script is back to one session per
     * statement.
     */
    @Test
    void theBatchingCanBeTurnedOff() {
      final boolean previous = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.SCHEMA_BULK_DDL_SCRIPT);
      database.getConfiguration().setValue(GlobalConfiguration.SCHEMA_BULK_DDL_SCRIPT, false);
      try {
        final int sessions = countSchemaSessions(() -> database.command("sqlscript", """
            CREATE DOCUMENT TYPE Disabled;
            CREATE PROPERTY Disabled.a STRING;
            CREATE PROPERTY Disabled.b STRING;
            """));
        assertThat(sessions).as("with the batching disabled every statement opens its own session").isEqualTo(3);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.SCHEMA_BULK_DDL_SCRIPT, previous);
      }
    }

    /**
     * The programmatic surface the issue asked for, for importers and the Java API: a batch of builder calls that
     * publishes once.
     */
    @Test
    void bulkChangeOpensOneSessionForABatchOfBuilderCalls() {
      final int sessions = countSchemaSessions(() -> database.getSchema().bulkChange(() -> {
        for (int i = 0; i < 5; i++) {
          final DocumentType type = database.getSchema().buildDocumentType().withName("Prog" + i).withTotalBuckets(1)
              .create();
          type.createProperty("name", Type.STRING);
          database.getSchema().buildTypeIndex("Prog" + i, new String[] { "name" })
              .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
        }
      }));

      assertThat(sessions).as("15 DDL calls, one published schema entry").isEqualTo(1);
      for (int i = 0; i < 5; i++) {
        assertThat(database.getSchema().existsType("Prog" + i)).isTrue();
        assertThat(database.getSchema().existsIndex("Prog" + i + "[name]")).isTrue();
      }
    }

    /**
     * {@code bulkChange} rethrows what the batch threw, unchanged, after publishing the prefix.
     */
    @Test
    void bulkChangeRethrowsTheBatchFailureAfterPublishingThePrefix() {
      assertThatThrownBy(() -> countSchemaSessions(() -> database.getSchema().bulkChange(() -> {
        database.getSchema().createDocumentType("Kept");
        throw new IllegalStateException("boom from the batch");
      })))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("boom from the batch");

      assertThat(lastSessionCount).as("the failing batch still published through exactly one session").isEqualTo(1);

      reopenDatabase();
      assertThat(database.getSchema().existsType("Kept")).as("the prefix is published, not discarded").isTrue();
    }

    /**
     * The statements that must never be folded into a batch. They are excluded by the DEFAULT rather than by an
     * override, which is the point: batching is opt in, so a statement is swept into a scope only if it says it may be.
     * COMPACT INDEX is the loudest of them - compaction refuses to start while a recording session is active (the #4063
     * guard), so inside a scope it would compact nothing and report success - and CREATE MATERIALIZED VIEW is the one an
     * opt-out default actually missed: it runs the same full population REFRESH does, reached through a CREATE verb.
     */
    @Test
    void recordLevelDdlIsNotBatchable() {
      final DatabaseInternal db = (DatabaseInternal) database;

      assertThat(new CompactIndexStatement().isBulkSchemaScopeSafe(db)).isFalse();
      assertThat(new RebuildIndexStatement().isBulkSchemaScopeSafe(db)).isFalse();
      assertThat(new TruncateTypeStatement().isBulkSchemaScopeSafe(db)).isFalse();
      assertThat(new RefreshMaterializedViewStatement().isBulkSchemaScopeSafe(db)).isFalse();
      assertThat(new CreateMaterializedViewStatement().isBulkSchemaScopeSafe(db))
          .as("creating a materialized view does a full initial population of its backing type").isFalse();
      assertThat(new CreateContinuousAggregateStatement().isBulkSchemaScopeSafe(db))
          .as("creating a continuous aggregate aggregates the whole source type first").isFalse();

      assertThat(new CreateVertexTypeStatement().isBulkSchemaScopeSafe(db)).isTrue();
      assertThat(new CreatePropertyStatement().isBulkSchemaScopeSafe(db)).isTrue();
    }

    /**
     * The default is the safety property, so it is asserted directly: a {@link DDLStatement} that says nothing is NOT
     * batched. Written against an anonymous subclass rather than an existing statement so that opting one of those in
     * later cannot quietly disarm it.
     */
    @Test
    void aStatementThatSaysNothingIsNotBatched() {
      final DDLStatement silent = new DDLStatement() {
        @Override
        public ResultSet executeDDL(final CommandContext context) {
          throw new UnsupportedOperationException("not executed");
        }

        @Override
        public Statement copy() {
          throw new UnsupportedOperationException("not copied");
        }
      };

      assertThat(silent.isBulkSchemaScopeSafe((DatabaseInternal) database))
          .as("batching is opt in: silence means the statement keeps its own session").isFalse();
    }

    /**
     * {@code ALTER TYPE ... WITH repartition = true} does not merely alter the type: it runs
     * {@code RebuildTypeStatement}'s scan-and-move loop directly, and inside the caller's transaction that loop takes
     * the branch with no intermediate batch commits. It is a rebuild, so it is excluded like one - and a bare
     * {@code ALTER TYPE} still is not.
     */
    @Test
    void anAlterTypeThatRepartitionsIsNotBatchable() {
      final DatabaseInternal db = (DatabaseInternal) database;

      final AlterTypeStatement bare = new AlterTypeStatement();
      assertThat(bare.isBulkSchemaScopeSafe(db)).as("an ordinary ALTER TYPE is schema definition").isTrue();

      final AlterTypeStatement repartitioning = new AlterTypeStatement();
      repartitioning.settings.put(new Identifier("repartition"), null);
      assertThat(repartitioning.isBulkSchemaScopeSafe(db))
          .as("WITH repartition runs REBUILD TYPE's scan-and-move loop, so it is excluded like REBUILD TYPE").isFalse();

      // And the script-level effect, which is what exercises the parse and the routing rather than only the flag. The
      // two scripts below have the SAME shape and the same statement count; the only difference is the repartition
      // clause on the last one, so the difference in the counts is attributable to it and to nothing else.
      database.command("sql", "CREATE DOCUMENT TYPE RepartParent");
      database.command("sql", "CREATE DOCUMENT TYPE Repart");
      database.command("sql", "CREATE DOCUMENT TYPE RepartControl");

      final int batched = countSchemaSessions(() -> database.command("sqlscript", """
          CREATE PROPERTY RepartControl.a STRING;
          CREATE PROPERTY RepartControl.b STRING;
          ALTER TYPE RepartControl SUPERTYPE +RepartParent;
          """));
      assertThat(batched).as("control: an ALTER TYPE with no repartition clause batches with the rest").isEqualTo(1);

      final int notBatched = countSchemaSessions(() -> database.command("sqlscript", """
          CREATE PROPERTY Repart.a STRING;
          CREATE PROPERTY Repart.b STRING;
          ALTER TYPE Repart WITH repartition = true;
          """));
      assertThat(notBatched)
          .as("a script carrying WITH repartition = true opens a session per statement, as it did before this feature")
          .isGreaterThan(batched);

      assertThat(database.getSchema().getType("Repart").getPropertyNames()).contains("a", "b");
      assertThat(database.getSchema().getType("RepartControl").getPropertyNames()).contains("a", "b");
    }

    /**
     * {@code CREATE INDEX} is the statement whose cost depends entirely on WHEN it runs: on a type the same script just
     * created there is nothing to scan, and on a pre-existing one it is a rebuild wearing a different verb.
     */
    @Test
    void createIndexIsBatchableOnlyOnATypeTheScriptItselfCreates() {
      final DatabaseInternal db = (DatabaseInternal) database;

      final CreateIndexStatement onNewType = new CreateIndexStatement();
      onNewType.typeName = new Identifier("NotYetThere");
      assertThat(onNewType.isBulkSchemaScopeSafe(db))
          .as("a type that does not exist yet has no records to scan").isTrue();

      database.command("sql", "CREATE VERTEX TYPE AlreadyThere");
      final CreateIndexStatement onExistingType = new CreateIndexStatement();
      onExistingType.typeName = new Identifier("AlreadyThere");
      assertThat(onExistingType.isBulkSchemaScopeSafe(db))
          .as("indexing a pre-existing type scans it, so it is excluded like REBUILD INDEX").isFalse();

      final CreateIndexStatement noTarget = new CreateIndexStatement();
      assertThat(noTarget.isBulkSchemaScopeSafe(db)).as("an unresolvable target answers conservatively").isFalse();

      final CreateIndexStatement variableTarget = new CreateIndexStatement();
      variableTarget.typeName = new Identifier("$runtimeType");
      assertThat(variableTarget.isBulkSchemaScopeSafe(db))
          .as("a type name bound only at execution time answers conservatively").isFalse();
    }

    /**
     * The script-level consequence of the rule above, both ways round, measured on the same run.
     */
    @Test
    void aScriptIndexingAPreExistingTypeIsNotBatchedWhileOneCreatingItIs() {
      final int fromScratch = countSchemaSessions(() -> database.command("sqlscript", """
          CREATE VERTEX TYPE FromScratch;
          CREATE PROPERTY FromScratch.a STRING;
          CREATE INDEX ON FromScratch (a) NOTUNIQUE;
          """));
      assertThat(fromScratch).as("a script that creates the type it indexes has nothing to scan").isEqualTo(1);

      database.command("sql", "CREATE VERTEX TYPE PreExisting");
      final int onExisting = countSchemaSessions(() -> database.command("sqlscript", """
          CREATE PROPERTY PreExisting.a STRING;
          CREATE INDEX ON PreExisting (a) NOTUNIQUE;
          """));
      assertThat(onExisting)
          .as("indexing a type that was already there runs a build, so the script keeps one session per statement")
          .isEqualTo(2);

      assertThat(database.getSchema().existsIndex("PreExisting[a]")).isTrue();
      assertThat(database.getSchema().existsIndex("FromScratch[a]")).isTrue();
    }
  }
}
