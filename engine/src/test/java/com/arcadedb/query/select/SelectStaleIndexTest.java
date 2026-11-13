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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SelectExecutionPlanner;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectStaleIndexTest {

  /**
   * Regression for #9331 and #9332: reads must keep answering while another thread creates and drops the index.
   * Issues #9331 and #9332: a SQL {@code SELECT}, the native {@code database.select()} and {@code lookupByKey()} must keep
   * answering while another thread creates and drops the very index they query. The previous coverage churned indexes on
   * properties the readers never used, so no reader ever resolved the index being dropped.
   * <ul>
   * <li>#9331: {@code FetchFromIndexStep} reads the index when the step starts, after the planner validated it, and
   * the native executor read the properties and the type of candidates with no guard;</li>
   * <li>#9332: {@code holdsFoldedKeys} read the metadata of an index twice, and a drop between the two reads turned the
   * null check into a NullPointerException.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IndexDdlDuringReads extends TestHelper {
    private static final int READERS = 8;
    private static final int ROUNDS  = 150;
    private static final int ROWS    = 200;

    @Test
    void holdsFoldedKeysAnswersFalseForAnIndexWithoutMetadataInsteadOfFailing() {
      final DocumentType type = database.getSchema().createDocumentType("Folded");
      type.createProperty("x", Type.LONG);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "x");
      assertThat(SelectExecutionPlanner.holdsFoldedKeys(index)).isFalse();

      index.drop();

      // The index went away with its sub-indexes: its metadata is null, which used to be read twice
      assertThat(((IndexInternal) index).getMetadata()).isNull();
      assertThat(SelectExecutionPlanner.holdsFoldedKeys(index)).isFalse();
      assertThat(SelectExecutionPlanner.isIndexCaseInsensitive(index, 0)).isFalse();
    }

    @Test
    void anIndexStillBeingBuiltIsNotUsedByAnyQuerySurface() {
      database.command("sql", "CREATE DOCUMENT TYPE Person BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.transaction(() -> {
        for (long i = 0; i < ROWS; i++)
          database.newDocument("Person").set("id", i).save();
      });

      final long[] found = new long[3];
      final AtomicBoolean probed = new AtomicBoolean();
      // Called for every record the build indexes: the index is registered by then, bucket by bucket, and holds only part of the
      // records. Nothing may answer from it until the build is over, so every id is found through the other path
      database.getSchema().getType("Person").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, new String[] { "id" }, 262_144,
          (document, totalIndexed) -> {
            if (totalIndexed < 10 || !probed.compareAndSet(false, true))
              return;
            for (long id = 0; id < ROWS; id++) {
              found[0] += countRows(database.query("sql", "SELECT FROM Person WHERE id = ?", id));
              found[1] += database.select().fromType("Person").where().property("id").eq().value(id).documents().stream().count();
              try {
                final var cursor = database.lookupByKey("Person", new String[] { "id" }, new Object[] { id });
                while (cursor.hasNext()) {
                  cursor.next();
                  found[2]++;
                }
              } catch (final IllegalArgumentException e) {
                // no index yet: the documented answer, the caller scans
                found[2]++;
              }
            }
          });

      assertThat(probed.get()).as("the build callback must have run").isTrue();
      assertThat(found).as("every id found through SQL, select() and lookupByKey while the index was half built")
          .containsExactly(ROWS, ROWS, ROWS);
      assertThat(countRows(database.query("sql", "SELECT FROM Person WHERE id = 7"))).isEqualTo(1);
      assertThat(database.lookupByKey("Person", new String[] { "id" }, new Object[] { 7L }).hasNext()).isTrue();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void sqlSelectNativeSelectAndLookupByKeyKeepAnsweringWhileTheQueriedIndexIsDroppedAndRecreated() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.transaction(() -> {
        for (long i = 0; i < ROWS; i++)
          database.newDocument("Person").set("id", i).save();
      });

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicLong answered = new AtomicLong();
      final AtomicLong wrongRowCounts = new AtomicLong();
      final Map<String, AtomicLong> failures = new ConcurrentHashMap<>();
      final List<Thread> readers = new ArrayList<>();
      for (int t = 0; t < READERS; t++) {
        final Random random = new Random(t);
        final int flavour = t % 3;
        final Thread thread = new Thread(() -> {
          while (!stop.get()) {
            final long id = random.nextInt(ROWS);
            try {
              final int rows = switch (flavour) {
                case 0 -> countRows(database.query("sql", "SELECT FROM Person WHERE id = ?", id));
                case 1 -> {
                  int n = 0;
                  try (final var it = database.select().fromType("Person").where().property("id").eq().value(id).documents()) {
                    while (it.hasNext()) {
                      final Document ignored = it.next();
                      n++;
                    }
                  }
                  yield n;
                }
                default -> {
                  // lookupByKey answers "no index" with an IllegalArgumentException while the index is not there: expected
                  int n = 0;
                  try (final var cursor = database.lookupByKey("Person", new String[] { "id" }, new Object[] { id })) {
                    while (cursor.hasNext()) {
                      cursor.next();
                      n++;
                    }
                  } catch (final IllegalArgumentException e) {
                    if (!e.getMessage().startsWith("No index has been created"))
                      throw e;
                    n = 1;
                  }
                  yield n;
                }
              };
              if (rows != 1)
                wrongRowCounts.incrementAndGet();
              answered.incrementAndGet();
            } catch (final Throwable e) {
              failures.computeIfAbsent(
                  "flavour " + flavour + " " + e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0],
                  k -> new AtomicLong()).incrementAndGet();
            }
          }
        });
        readers.add(thread);
        thread.start();
      }

      try {
        for (int i = 0; i < ROUNDS; i++) {
          database.command("sql", "CREATE INDEX ON Person (id) NOTUNIQUE");
          database.command("sql", "DROP INDEX `Person[id]`");
        }
      } finally {
        stop.set(true);
        for (final Thread thread : readers)
          thread.join();
      }

      assertThat(answered.get()).isGreaterThan(0);
      assertThat(failures).as("statements answered: " + answered.get()).isEmpty();
      // A query that was already reading the index in the instant it was dropped can miss the rows of the sub-indexes gone by then:
      // it was planned before the drop completed (see TypeIndex#drop). That is a handful in tens of thousands, never the half-built
      // index of the other test, which answered most of them wrongly
      assertThat(wrongRowCounts.get()).as("answers that missed their row, of " + answered.get())
          .isLessThan(Math.max(5, answered.get() / 200));
    }

    private static int countRows(final ResultSet rs) {
      try (rs) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
        return rows;
      }
    }
  }

  /**
   * Regression for #9615: a read that returned no row yet failed while the index was dropped.
   * Issue #9615: a SQL range or ORDER BY {@code SELECT} that had returned no row yet still failed to the caller while another
   * thread dropped the index it read. The retry of #9331 plans the statement again when its index went away before the first
   * row, but the "File with id n was not found" of a sub-index file deleted by the drop was turned into the retry's
   * {@code IndexException} only once the whole index was invalid, which the drop does last, and the steps read the uniqueness
   * of the index on every entry, after the first row was computed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StaleIndexBeforeFirstRow extends TestHelper {
    private static final int  ROWS    = 5_000;
    private static final int  ROUNDS  = 40;
    private static final long RANGE   = 50;

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void rangeAndOrderBySelectsNeverFailBeforeTheirFirstRowWhileTheIndexIsDropped() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Person BUCKETS 8");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.transaction(() -> {
        for (long i = 0; i < ROWS; i++)
          database.newDocument("Person").set("id", i).save();
      });

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicLong answered = new AtomicLong();
      final AtomicLong shortAnswers = new AtomicLong();
      final Map<String, AtomicLong> failuresBeforeFirstRow = new ConcurrentHashMap<>();
      final List<Thread> readers = new ArrayList<>();
      for (int t = 0; t < 8; t++) {
        final Random random = new Random(t);
        final int shape = t < 4 ? 0 : t < 6 ? 1 : 2;
        final Thread thread = new Thread(() -> {
          while (!stop.get()) {
            final long from = random.nextInt(ROWS - (int) RANGE);
            final long expected = shape == 0 ? RANGE : 1;
            int rows = 0;
            try (final ResultSet rs = switch (shape) {
              case 0 -> database.query("sql", "SELECT FROM Person WHERE id >= ? AND id < ?", from, from + RANGE);
              case 1 -> database.query("sql", "SELECT FROM Person WHERE id = ?", from);
              default -> database.query("sql", "SELECT FROM Person WHERE id = ? ORDER BY id", from);
            }) {
              while (rs.hasNext()) {
                rs.next();
                rows++;
              }
              if (rows != expected)
                shortAnswers.incrementAndGet();
              answered.incrementAndGet();
            } catch (final Throwable e) {
              // An index dropped after rows were handed over surfaces as an exception by design (see SelectStatement): only a
              // statement that has returned nothing can be, and must be, planned again
              if (rows == 0)
                failuresBeforeFirstRow.computeIfAbsent("shape " + shape + " " + describe(e), k -> new AtomicLong()).incrementAndGet();
            }
          }
        });
        readers.add(thread);
        thread.start();
      }

      try {
        for (int i = 0; i < ROUNDS; i++) {
          database.command("sql", "CREATE INDEX ON Person (id) NOTUNIQUE");
          database.command("sql", "DROP INDEX `Person[id]`");
        }
      } finally {
        stop.set(true);
        for (final Thread thread : readers)
          thread.join();
      }

      assertThat(answered.get()).isGreaterThan(0);
      assertThat(failuresBeforeFirstRow).as("statements answered: " + answered.get() + ", short: " + shortAnswers.get()).isEmpty();
    }

    /**
     * The last attempt of a statement whose index kept going away plans without the key indexes of its types: a range, an equality
     * and an ORDER BY on the indexed property are then answered by a scan, with the same rows.
     */
    @Test
    void theLastAttemptPlansWithoutKeyIndexesAndAnswersTheSameRows() {
      database.command("sql", "CREATE DOCUMENT TYPE Person BUCKETS 8");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.command("sql", "CREATE INDEX ON Person (id) NOTUNIQUE");
      database.transaction(() -> {
        for (long i = 0; i < 500; i++)
          database.newDocument("Person").set("id", i).save();
      });

      for (final String sql : new String[] { "SELECT FROM Person WHERE id >= 100 AND id < 150", "SELECT FROM Person WHERE id = 42",
          "SELECT FROM Person WHERE id >= 10 AND id < 20 ORDER BY id DESC" }) {
        final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);

        final BasicCommandContext withIndex = new BasicCommandContext();
        withIndex.setDatabase(database);
        final InternalExecutionPlan indexed = statement.createExecutionPlanNoCache(withIndex);
        assertThat(indexed.prettyPrint(0, 2)).as(sql).contains("FETCH FROM INDEX");

        final BasicCommandContext withoutIndex = new BasicCommandContext();
        withoutIndex.setDatabase(database);
        final InternalExecutionPlan scan = new SelectExecutionPlanner(statement).withoutKeyIndexes()
            .createExecutionPlan(withoutIndex, false);
        assertThat(scan.prettyPrint(0, 2)).as(sql).doesNotContain("FETCH FROM INDEX");

        assertThat(ids(new LocalResultSet(scan))).as(sql).containsExactlyElementsOf(ids(new LocalResultSet(indexed)));
      }
    }

    /**
     * A FULL_TEXT index stays plannable on the last attempt: {@code CONTAINSTEXT} matches analyzer tokens through the index and
     * would mean something else on a scan, so dropping the index from the plan would change the answer, not only its cost.
     */
    @Test
    void theLastAttemptKeepsAFullTextIndex() {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE INDEX ON Article (title) FULL_TEXT");
      database.transaction(() -> {
        database.newDocument("Article").set("title", "graph database engine").save();
        database.newDocument("Article").set("title", "document store").save();
      });

      final String sql = "SELECT FROM Article WHERE title CONTAINSTEXT 'graph'";
      final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final InternalExecutionPlan plan = new SelectExecutionPlanner(statement).withoutKeyIndexes().createExecutionPlan(context, false);
      assertThat(plan.prettyPrint(0, 2)).contains("FETCH FROM INDEX");

      int rows = 0;
      try (final ResultSet rs = new LocalResultSet(plan)) {
        while (rs.hasNext()) {
          assertThat(rs.next().<String>getProperty("title")).isEqualTo("graph database engine");
          rows++;
        }
      }
      assertThat(rows).isEqualTo(1);
    }

    private static List<Long> ids(final ResultSet rs) {
      final List<Long> ids = new ArrayList<>();
      try (rs) {
        while (rs.hasNext())
          ids.add(rs.next().<Long>getProperty("id"));
      }
      if (ids.size() > 1 && ids.getFirst() > ids.getLast())
        return ids; // the ORDER BY DESC case: the order is part of the answer
      ids.sort(null);
      return ids;
    }

    private static String describe(final Throwable e) {
      final StringWriter trace = new StringWriter();
      e.printStackTrace(new PrintWriter(trace));
      final String[] lines = trace.toString().split("\n");
      final StringBuilder b = new StringBuilder();
      for (int i = 0; i < Math.min(lines.length, 14); i++)
        b.append(lines[i]).append('\n');
      return b.toString();
    }
  }

  /**
   * Regression for #9812: the native select fell back to a scan only after the drop finished.
   * Issue #9812: the native {@code database.select()} falls back to a scan when the index it reads is dropped under it, but its
   * catch asked {@code isValid()}, which {@code TypeIndex.drop()} clears last: during the whole window in which the sub-index
   * files are already gone the "File with id n was not found" reached the caller. The equality lookup is read whole before the
   * first row, so it must also never answer short when a drop took sub-indexes away under it without an error (#9651).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NativeSelectStaleIndex extends TestHelper {
    private static final int  ROWS   = 5_000;
    private static final int  ROUNDS = 40;
    private static final long RANGE  = 50;

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void nativeSelectsNeverFailBeforeTheirFirstRowWhileTheIndexIsDropped() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Person BUCKETS 8");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.transaction(() -> {
        for (long i = 0; i < ROWS; i++)
          database.newDocument("Person").set("id", i).save();
      });

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicLong answered = new AtomicLong();
      final AtomicLong shortPointAnswers = new AtomicLong();
      final Map<String, AtomicLong> failuresBeforeFirstRow = new ConcurrentHashMap<>();
      final List<Thread> readers = new ArrayList<>();
      for (int t = 0; t < 6; t++) {
        final Random random = new Random(t);
        final boolean range = t < 3;
        final Thread thread = new Thread(() -> {
          while (!stop.get()) {
            final long from = random.nextInt(ROWS - (int) RANGE);
            int rows = 0;
            try {
              final Iterator<? extends Document> it = range ?
                  database.select().fromType("Person").where().property("id").ge().value(from).and().property("id").lt()
                      .value(from + RANGE).documents() :
                  database.select().fromType("Person").where().property("id").eq().value(from).documents();
              while (it.hasNext()) {
                it.next();
                rows++;
              }
              if (!range && rows != 1)
                shortPointAnswers.incrementAndGet();
              answered.incrementAndGet();
            } catch (final Throwable e) {
              // An index dropped after rows were handed over cannot be read again from the start: only a select that has
              // returned nothing can, and must, answer without it
              if (rows == 0)
                failuresBeforeFirstRow.computeIfAbsent((range ? "range " : "point ") + e, k -> new AtomicLong()).incrementAndGet();
            }
          }
        });
        readers.add(thread);
        thread.start();
      }

      try {
        for (int i = 0; i < ROUNDS; i++) {
          database.command("sql", "CREATE INDEX ON Person (id) NOTUNIQUE");
          database.command("sql", "DROP INDEX `Person[id]`");
        }
      } finally {
        stop.set(true);
        for (final Thread thread : readers)
          thread.join();
      }

      assertThat(answered.get()).isGreaterThan(0);
      assertThat(failuresBeforeFirstRow).as("selects answered: " + answered.get()).isEmpty();
      assertThat(shortPointAnswers.get()).as("point lookups that answered no row for an id every record set holds").isZero();
    }
  }

  /**
   * Regression for #9858: a stale vector candidate escaped as RecordNotFoundException.
   * Issue #9858, the native Select API twin of {@code db.index.vector.queryNodes}: {@code select().nearestTo()} loaded every
   * candidate the vector index answered with and let a {@code RecordNotFoundException} escape when a commit that overlapped
   * the search had deleted one of them. A stale candidate must be skipped and the answer topped up to the {@code k} nearest
   * LIVE records; a post-filter {@code where()} keeps applying to those {@code k}, as it does when nothing is stale.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorStaleCandidate extends TestHelper {
    private static final int     COUNT  = 60;
    private static final float[] ORIGIN = { 0f, 0f };

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // the tests leave index entries pointing at records they removed from the bucket, on purpose
      return false;
    }

    @Test
    void staleCandidatesAreSkippedAndToppedUp() {
      createType("Doc", 1);
      removeFromBucketOnly("pid < 3");

      assertThat(pids(database.select().fromType("Doc").nearestTo("vector", ORIGIN, 5).vertices())).containsExactly(3, 4, 5,
          6, 7);
    }

    @Test
    void aWholeWindowOfStaleCandidatesIsToppedUpOverSeveralBuckets() {
      createType("Doc", 4);
      removeFromBucketOnly("pid < 10");

      assertThat(pids(database.select().fromType("Doc").nearestTo("vector", ORIGIN, 10).vertices())).containsExactly(10, 11,
          12, 13, 14, 15, 16, 17, 18, 19);
    }

    @Test
    void anIndexWithFewerLiveRecordsThanKAnswersWhatItHas() {
      createType("Doc", 1);
      removeFromBucketOnly("pid >= 4");

      assertThat(pids(database.select().fromType("Doc").nearestTo("vector", ORIGIN, 10).vertices())).containsExactly(0, 1, 2,
          3);
    }

    @Test
    void thePostFilterStillAppliesToTheKNearestLiveRecords() {
      createType("Doc", 1);
      removeFromBucketOnly("pid < 4");

      // The 10 nearest live records are 4..13: the filter keeps their even half, and never reaches past them.
      assertThat(pids(database.select().fromType("Doc").nearestTo("vector", ORIGIN, 10).where().property("even").eq().value(true)
          .vectorVertices())).containsExactly(4, 6, 8, 10, 12);
    }

    private void createType(final String typeName, final int buckets) {
      database.command("sql", "CREATE VERTEX TYPE " + typeName + " BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY " + typeName + ".pid INTEGER");
      database.command("sql", "CREATE PROPERTY " + typeName + ".vector ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON " + typeName + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
      database.transaction(() -> {
        for (int i = 0; i < COUNT; i++)
          database.newVertex(typeName).set("pid", i, "even", i % 2 == 0, "vector", new float[] { i, 0f }).save();
      });
    }

    /** Deletes the matching records from their bucket only, leaving their vector index entries behind. */
    private void removeFromBucketOnly(final String where) {
      final List<RID> stale = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Doc WHERE " + where)) {
        rs.forEachRemaining(r -> stale.add(r.getProperty("rid")));
      }
      assertThat(stale).isNotEmpty();

      // Build every graph first: a lazy build validates each record and would drop the ones removed below.
      final IndexInternal[] bucketIndexes = ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets();
      for (final IndexInternal bucketIndex : bucketIndexes)
        ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(ORIGIN, 10);

      database.transaction(() -> {
        for (final RID rid : stale)
          ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
      });

      // Precondition: the indexes still answer with the removed records, otherwise these tests prove nothing.
      final List<RID> fromIndexes = new ArrayList<>();
      for (final IndexInternal bucketIndex : bucketIndexes)
        ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(ORIGIN, COUNT).forEach(p -> fromIndexes.add(p.getFirst()));
      assertThat(fromIndexes).containsAll(stale);
    }

    private static List<Integer> pids(final List<SelectVectorResult<Vertex>> results) {
      final List<Integer> pids = new ArrayList<>(results.size());
      for (final SelectVectorResult<Vertex> result : results) {
        final Document document = result.getDocument();
        pids.add(document.getInteger("pid"));
      }
      return pids;
    }
  }
}
