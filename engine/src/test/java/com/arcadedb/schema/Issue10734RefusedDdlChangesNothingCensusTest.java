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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
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
import com.arcadedb.query.sql.parser.RebuildGraphAnalyticalViewStatement;
import com.arcadedb.query.sql.parser.RebuildIndexStatement;
import com.arcadedb.query.sql.parser.RebuildTypeStatement;
import com.arcadedb.query.sql.parser.RefreshContinuousAggregateStatement;
import com.arcadedb.query.sql.parser.RefreshMaterializedViewStatement;
import com.arcadedb.query.sql.parser.TruncateBucketStatement;
import com.arcadedb.query.sql.parser.TruncateTypeStatement;
import com.arcadedb.serializer.json.JSONObject;
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
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Issue #10734: {@code DROP INDEX *} dropped the indexes one {@code dropIndex()} call at a time. Each call is all or nothing (issue
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
class Issue10734RefusedDdlChangesNothingCensusTest extends TestHelper {

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

  @Test
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
