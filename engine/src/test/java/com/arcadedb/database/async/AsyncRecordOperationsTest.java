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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.RawRecordCallback;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for AsyncRecordOperations. Each nested class keeps the fixture of the test it was folded from.
 */
class AsyncRecordOperationsTest {

  /**
   * Regression test for issue #7002: {@code async().updateRecord()} never validated the document, so an asynchronous
   * update persisted a record violating a MANDATORY/NOTNULL constraint with no error reaching the caller or the error
   * callback. The synchronous update is the control.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncUpdateValidation extends TestHelper {

    private static final String TYPE = "Issue7002P";

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("name", Type.STRING).setMandatory(true).setNotNull(true);
      type.createProperty("v", Type.INTEGER);
    }

    @Test
    void asyncUpdateRejectsARecordViolatingItsSchemaAndReportsItToTheErrorCallback() {
      final RID rid = createRecord();

      // CONTROL: THE SYNCHRONOUS UPDATE REFUSES THE SAME EDIT
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = rid.asDocument().modify();
        doc.set("v", 2);
        doc.remove("name");
        doc.save();
      })).isInstanceOf(ValidationException.class);

      final AtomicInteger okCalls = new AtomicInteger();
      final AtomicReference<Throwable> reported = new AtomicReference<>();
      final AtomicReference<Throwable> executorWide = new AtomicReference<>();
      database.async().onError(executorWide::set);

      final MutableDocument doc = rid.asDocument().modify();
      doc.set("v", 3);
      doc.remove("name");
      database.async().updateRecord(doc, record -> okCalls.incrementAndGet(), reported::set);
      database.async().waitCompletion();

      assertThat(reported.get()).as("the violation must reach the task's error callback").isInstanceOf(ValidationException.class);
      assertThat(executorWide.get()).as("and the executor-wide onError").isInstanceOf(ValidationException.class);
      assertThat(okCalls.get()).as("a rejected update has no success to report").isZero();

      // THE DATABASE STILL HOLDS THE RECORD THE SCHEMA ALLOWS: NAME PRESENT, THE EDIT NEVER LANDED
      final Document stored = database.lookupByRID(rid, true).asDocument();
      assertThat(stored.getString("name")).isEqualTo("original");
      assertThat(stored.getInteger("v")).isEqualTo(1);
    }

    @Test
    void asyncUpdateStillAppliesAValidEdit() {
      final RID rid = createRecord();

      final AtomicReference<Throwable> reported = new AtomicReference<>();
      final MutableDocument doc = rid.asDocument().modify();
      doc.set("v", 5);
      database.async().updateRecord(doc, null, reported::set);
      database.async().waitCompletion();

      assertThat(reported.get()).isNull();
      assertThat(database.lookupByRID(rid, true).asDocument().getInteger("v")).isEqualTo(5);
    }

    private RID createRecord() {
      final AtomicReference<RID> rid = new AtomicReference<>();
      database.transaction(() -> rid.set(database.newDocument(TYPE).set("name", "original").set("v", 1).save().getIdentity()));
      return rid.get();
    }
  }

  /**
   * Regression test for issue #7003: {@code async().deleteRecord()} fired every before/after-delete listener twice,
   * once from the async task and once more from the {@code deleteRecordNoLock} path it delegates to. Both the
   * database-level and the type-level registries are counted, and the veto path is pinned as well: a vetoed delete must
   * neither delete the record nor report success.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncDeleteListenerDoubleFire extends TestHelper {

    private static final String TYPE = "Issue7003Doc";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE);
    }

    @Test
    void everyDeleteListenerFiresExactlyOncePerAsyncDelete() {
      final AtomicInteger dbBefore = new AtomicInteger();
      final AtomicInteger dbAfter = new AtomicInteger();
      final AtomicInteger typeBefore = new AtomicInteger();
      final AtomicInteger typeAfter = new AtomicInteger();

      final DocumentType type = database.getSchema().getType(TYPE);
      database.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
        dbBefore.incrementAndGet();
        return true;
      }).registerListener((AfterRecordDeleteListener) record -> dbAfter.incrementAndGet());
      type.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
        typeBefore.incrementAndGet();
        return true;
      }).registerListener((AfterRecordDeleteListener) record -> typeAfter.incrementAndGet());

      final RID rid = createRecord();

      final AtomicInteger okCalls = new AtomicInteger();
      final AtomicReference<Throwable> error = new AtomicReference<>();
      database.async().deleteRecord(rid.getRecord(), record -> okCalls.incrementAndGet(), error::set);
      database.async().waitCompletion();

      assertThat(error.get()).isNull();
      assertThat(okCalls.get()).isEqualTo(1);
      assertThat(dbBefore.get()).as("database-level beforeDelete").isEqualTo(1);
      assertThat(dbAfter.get()).as("database-level afterDelete").isEqualTo(1);
      assertThat(typeBefore.get()).as("type-level beforeDelete").isEqualTo(1);
      assertThat(typeAfter.get()).as("type-level afterDelete").isEqualTo(1);
      assertThat(database.existsRecord(rid)).isFalse();
    }

    @Test
    void aVetoedAsyncDeleteKeepsTheRecordAndSkipsTheSuccessCallback() {
      final AtomicInteger before = new AtomicInteger();
      final AtomicInteger after = new AtomicInteger();
      database.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
        before.incrementAndGet();
        return false;
      }).registerListener((AfterRecordDeleteListener) record -> after.incrementAndGet());

      final RID rid = createRecord();

      final AtomicInteger okCalls = new AtomicInteger();
      final AtomicReference<Throwable> error = new AtomicReference<>();
      database.async().deleteRecord(rid.getRecord(), record -> okCalls.incrementAndGet(), error::set);
      database.async().waitCompletion();

      assertThat(error.get()).isNull();
      assertThat(before.get()).as("the vetoing listener is asked once").isEqualTo(1);
      assertThat(after.get()).as("nothing was deleted, so no afterDelete").isZero();
      assertThat(okCalls.get()).as("a vetoed delete is not a success").isZero();
      assertThat(database.existsRecord(rid)).isTrue();
    }

    private RID createRecord() {
      final AtomicReference<RID> rid = new AtomicReference<>();
      database.transaction(() -> rid.set(database.newDocument(TYPE).set("id", 1).save().getIdentity()));
      return rid.get();
    }
  }

  /**
   * Regression tests for #6467: {@link DatabaseAsyncExecutorImpl#scanType} must not return successfully when one of
   * its per-bucket tasks fails with a bucket-level (as opposed to per-record) exception - that used to be swallowed,
   * routed only to the (typically unset) executor-wide error callback while the per-bucket latch still counted down,
   * so the caller saw a silently partial scan.
   */
  @Nested
  class ScanTypeBucketFailure extends TestHelper {

    private static final String TYPE = "Issue6467Item";

    /**
     * Direct unit test of the fix's mechanism: a bucket whose {@code scan()} throws must have that exception captured
     * into the shared holder AND rethrown (so the existing per-task rollback/logging in the worker still runs
     * unchanged), and {@code completed()} must still fire so a caller blocked in {@code scanType()} is never left
     * hanging on a failed bucket.
     */
    @Test
    void failingBucketScanIsCapturedRethrownAndStillCompletes() {
      final RuntimeException injected = new RuntimeException("simulated bucket I/O failure");

      final AtomicReference<Throwable> firstError = new AtomicReference<>();
      final CountDownLatch             latch      = new CountDownLatch(1);
      final DatabaseAsyncScanBucket task =
          new DatabaseAsyncScanBucket(latch, record -> true, null, brokenBucket("brokenBucket", injected), firstError);

      assertThatThrownBy(() -> task.execute(null, null)).isSameAs(injected);
      assertThat(firstError.get()).isSameAs(injected);

      task.completed();
      assertThat(latch.getCount()).isZero();
    }

    /**
     * A second bucket failing after {@code firstError} is already claimed must not be silently dropped: it loses the
     * {@code compareAndSet} race to be the one {@code scanType()} rethrows, but is still logged, and its own task
     * still rethrows so the worker's own rollback/logging/{@code onError} handling runs for it too. No real
     * concurrency is needed to exercise this deterministically: {@code compareAndSet}'s outcome depends only on
     * {@code firstError}'s current value, so calling the two tasks sequentially against one shared holder reproduces
     * the "already claimed" branch exactly as a genuine race would.
     */
    @Test
    void aSecondConcurrentlyFailingBucketLosesTheRaceButIsStillRethrown() {
      final RuntimeException firstFailure  = new RuntimeException("first bucket failure");
      final RuntimeException secondFailure = new RuntimeException("second bucket failure");

      final AtomicReference<Throwable> firstError = new AtomicReference<>();
      final DatabaseAsyncScanBucket taskA =
          new DatabaseAsyncScanBucket(new CountDownLatch(1), record -> true, null, brokenBucket("bucketA", firstFailure), firstError);
      final DatabaseAsyncScanBucket taskB =
          new DatabaseAsyncScanBucket(new CountDownLatch(1), record -> true, null, brokenBucket("bucketB", secondFailure), firstError);

      assertThatThrownBy(() -> taskA.execute(null, null)).isSameAs(firstFailure);
      assertThat(firstError.get()).isSameAs(firstFailure);

      // taskB still rethrows its OWN exception even though it lost the race to populate firstError.
      assertThatThrownBy(() -> taskB.execute(null, null)).isSameAs(secondFailure);
      assertThat(firstError.get()).as("the first bucket's failure is still the one scanType() will report").isSameAs(firstFailure);
    }

    private static Bucket brokenBucket(final String name, final RuntimeException toThrow) {
      return new Bucket() {
        @Override
        public RID createRecord(final Record record, final boolean discardRecordAfter) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void updateRecord(final Record record, final boolean discardRecordAfter) {
          throw new UnsupportedOperationException();
        }

        @Override
        public com.arcadedb.database.Binary getRecord(final RID rid) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean existsRecord(final RID rid) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRecord(final RID rid) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRecord(final RID rid, final boolean force) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void scan(final RawRecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
          throw toThrow;
        }

        @Override
        public Iterator<Record> iterator() {
          throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Record> inverseIterator() {
          throw new UnsupportedOperationException();
        }

        @Override
        public long count() {
          throw new UnsupportedOperationException();
        }

        @Override
        public int getFileId() {
          return 0;
        }

        @Override
        public String getName() {
          return name;
        }
      };
    }

    /**
     * End-to-end: {@code LocalBucket.scan()} rethrows a {@link DatabaseIsReadOnlyException} raised by the user's
     * per-record callback rather than routing it through {@code errorRecordCallback} - a deliberate abort-the-whole-
     * scan signal, and the real, reachable analogue of "a bucket's scan throws (I/O / corruption)" the issue
     * describes, as opposed to an ordinary per-record error (which already correctly routes through
     * {@code errorRecordCallback} and is not what this test is about). This test uses no {@code errorRecordCallback}
     * (the 3-arg {@code scanType()} overload), which is exactly the case that always rethrows; a non-null one that
     * returns {@code true} for this exception would too, but one that returns {@code false} would instead stop the
     * scan without rethrowing. Two buckets are used so the fix's per-bucket capture is proven to still let the OTHER
     * bucket be scanned to completion.
     */
    @Test
    void scanTypePropagatesABucketLevelFailureInsteadOfReturningAPartialScan() {
      final List<RID> allIds = new ArrayList<>();
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 2);
        for (int i = 0; i < 20; i++)
          allIds.add(database.newDocument(TYPE).set("id", i).save().getIdentity());
      });

      final Set<RID>      scannedIds      = ConcurrentHashMap.newKeySet();
      final AtomicInteger scanned         = new AtomicInteger();
      final AtomicInteger failingBucketId = new AtomicInteger(-1);

      assertThatThrownBy(() -> database.async().scanType(TYPE, true, record -> {
        scannedIds.add(record.getIdentity());
        if (scanned.incrementAndGet() == 5) {
          failingBucketId.set(record.getIdentity().getBucketId());
          throw new DatabaseIsReadOnlyException("simulated bucket-level scan failure");
        }
        return true;
      })).isInstanceOf(DatabaseOperationException.class);

      // Every record that lives in a DIFFERENT bucket than the one that failed must have been scanned: that bucket's
      // task is unaffected by the other one's exception and must still drain to completion, rather than the whole
      // scan silently stopping partway through (which is the exact bug #6467 is about).
      final List<RID> otherBucketIds = allIds.stream().filter(rid -> rid.getBucketId() != failingBucketId.get()).toList();
      assertThat(otherBucketIds).isNotEmpty();
      assertThat(scannedIds).containsAll(otherBucketIds);
    }
  }

  /**
   * Regression test for issue #6324, item 5: the routing that sends dispatched DDL off the async workers stopped being
   * SQL's private knowledge.
   * <p>
   * #6303 gave back {@code CREATE INDEX} / {@code REBUILD INDEX} sent with {@code awaitResponse=false} by routing
   * statements that parse to DDL onto {@code AsyncCommandPool}, whose threads are not workers of any executor and can
   * therefore satisfy the barrier a scan-based index build needs. The classification lived in the dispatcher and knew
   * about {@code sql} and {@code sqlscript} only, so the same statement in Cypher still ran on a worker and was still
   * refused there by #6281's guard - a refusal rather than the old hang, but an asymmetry a user meets without warning.
   * <p>
   * The question is now asked of the LANGUAGE, through {@link QueryEngine#classifyDDL(String)}. Cypher can answer it
   * for the same reason SQL can - the parse is a statement-cache lookup the execution repeats - and an engine that
   * cannot answer cheaply says {@code UNKNOWN} and keeps the behaviour it has always had.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncDispatchedDDLPerLanguage extends TestHelper {

    /** The reported shape in Cypher: {@code CREATE INDEX} sent without waiting for the response. */
    @Test
    @Timeout(180)
    void cypherCreateIndexDispatchedAsynchronouslyBuildsTheIndex() throws Exception {
      database.transaction(() -> database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER));
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newVertex("V").set("id", i).save();
      });

      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<String> ranOn = new AtomicReference<>();
      final AtomicReference<Exception> failure = new AtomicReference<>();
      database.async().command("cypher", "CREATE INDEX FOR (n:V) ON (n.id)", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
          ranOn.set(Thread.currentThread().getName());
          done.countDown();
        }

        @Override
        public void onError(final Exception exception) {
          failure.compareAndSet(null, exception);
          done.countDown();
        }
      });

      assertThat(done.await(120, TimeUnit.SECONDS)).as("the command must run to an answer, not park").isTrue();
      assertThat(failure.get()).as("the barrier is satisfiable off the workers, so there is nothing to refuse").isNull();
      assertThat(ranOn.get()).as("Cypher DDL goes to the same pool SQL DDL does").startsWith("ArcadeDB-AsyncCommand-");

      database.async().waitCompletion();

      assertThat(database.getSchema().getIndexByName("V[id]")).isNotNull();
      assertThat(database.getSchema().getIndexByName("V[id]").countEntries())
          .as("and it indexed the records that were already there").isEqualTo(50);
    }

    /**
     * The other half, and the half widening the fix would break: an ordinary Cypher WRITE keeps running on a worker,
     * where its batch transaction and its pinned bucket are. {@code CREATE (n:V ...)} carries the word the cheap filter
     * looks for, so this is also the case that proves the decision is the parse and not the keyword.
     */
    @Test
    @Timeout(180)
    void anOrdinaryCypherCreateStillRunsOnAWorker() throws Exception {
      database.transaction(() -> database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER));

      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<String> ranOn = new AtomicReference<>();
      database.async().command("cypher", "CREATE (n:V {id: 1})", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
          ranOn.set(Thread.currentThread().getName());
          done.countDown();
        }

        @Override
        public void onError(final Exception exception) {
          ranOn.set("error: " + exception);
          done.countDown();
        }
      });

      assertThat(done.await(120, TimeUnit.SECONDS)).isTrue();
      assertThat(ranOn.get()).as("a Cypher write is not DDL and must keep its worker").doesNotStartWith("ArcadeDB-AsyncCommand-");

      database.async().waitCompletion();
      assertThat(database.countType("V", false)).isEqualTo(1);
    }

    /** The hook itself, asked directly, for the three languages that answer it and one that does not. */
    @Test
    @Timeout(60)
    void theLanguagesThatCanClassifyDoAndTheOthersSaySo() {
      database.transaction(() -> database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER));

      final QueryEngineManager engines = QueryEngineManager.getInstance();

      assertThat(engines.getEngine("sql", (DatabaseInternal) database)
          .classifyDDL("CREATE INDEX ON V (id) UNIQUE")).isEqualTo(QueryEngine.DDLClassification.DDL);
      assertThat(engines.getEngine("sql", (DatabaseInternal) database)
          .classifyDDL("INSERT INTO V SET id = 1")).isEqualTo(QueryEngine.DDLClassification.NOT_DDL);

      assertThat(engines.getEngine("sqlscript", (DatabaseInternal) database)
          .classifyDDL("INSERT INTO V SET id = 1; CREATE INDEX ON V (id) UNIQUE;"))
          .as("a script routes as a whole, on whether ANY statement in it is DDL")
          .isEqualTo(QueryEngine.DDLClassification.DDL);

      assertThat(engines.getEngine("cypher", (DatabaseInternal) database)
          .classifyDDL("CREATE INDEX FOR (n:V) ON (n.id)")).isEqualTo(QueryEngine.DDLClassification.DDL);
      assertThat(engines.getEngine("cypher", (DatabaseInternal) database)
          .classifyDDL("CREATE (n:V {id: 1})"))
          .as("the word CREATE is not the decision: the parse is").isEqualTo(QueryEngine.DDLClassification.NOT_DDL);

      // An engine that cannot classify without paying for a parse execution will not reuse says UNKNOWN rather than
      // guessing. Asked of a language that ships with the engine module so the assertion does not depend on which
      // optional modules are on the test classpath.
      assertThat(engines.getEngine("java", (DatabaseInternal) database).classifyDDL("anything"))
          .isEqualTo(QueryEngine.DDLClassification.UNKNOWN);
    }
  }

  /**
   * Regression test for issue #9941: {@code async().query(language, query, callback, args...)} bound the executor's own
   * {@code ContextConfiguration} as the first query parameter, for positional parameters and for a named-parameter map
   * alike, because {@link DatabaseAsyncCommand#execute} called {@code database.query(language, command, configuration,
   * parameters)} and {@code Database} has no {@code query} overload taking a configuration, so the call resolved to the
   * varargs {@code query(String, String, Object...)}. The synchronous query is the control.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncQueryParameters extends TestHelper {

    private static final String TYPE = "Issue9941V";

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType(TYPE);
      database.transaction(() -> {
        database.newVertex(TYPE).set("name", "a").set("v", 1).save();
        database.newVertex(TYPE).set("name", "b").set("v", 2).save();
        database.newVertex(TYPE).set("name", "c").set("v", 3).save();
      });
    }

    @Test
    void asyncQueryBindsPositionalParameters() {
      try (final ResultSet rs = database.query("sql", "SELECT ? AS a, ? AS b", 1, "x")) {
        final Result row = rs.next();
        assertThat((Integer) row.getProperty("a")).isEqualTo(1);
        assertThat((String) row.getProperty("b")).isEqualTo("x");
      }

      final List<Result> rows = asyncQuery("sql", "SELECT ? AS a, ? AS b", 1, "x");
      assertThat(rows).hasSize(1);
      assertThat((Object) rows.getFirst().getProperty("a")).isEqualTo(1);
      assertThat((Object) rows.getFirst().getProperty("b")).isEqualTo("x");
    }

    @Test
    void asyncQueryBindsNamedParameters() {
      final Map<String, Object> named = Map.of("k", 5);
      try (final ResultSet rs = database.query("sql", "SELECT :k AS a", named)) {
        assertThat((Integer) rs.next().getProperty("a")).isEqualTo(5);
      }

      final List<Result> rows = asyncQuery("sql", "SELECT :k AS a", named);
      assertThat(rows).hasSize(1);
      assertThat((Object) rows.getFirst().getProperty("a")).isEqualTo(5);
    }

    @Test
    void asyncQueryFiltersWithParametersAsTheSynchronousQueryDoes() {
      final List<Result> positional = asyncQuery("sql", "SELECT FROM " + TYPE + " WHERE v >= ? ORDER BY v", 2);
      assertThat(positional).extracting(r -> (String) r.getProperty("name")).containsExactly("b", "c");

      final List<Result> named = asyncQuery("sql", "SELECT FROM " + TYPE + " WHERE name = :name", Map.of("name", "a"));
      assertThat(named).extracting(r -> (Integer) r.getProperty("v")).containsExactly(1);
    }

    @Test
    void asyncOpenCypherQueryBindsNamedParameters() {
      final String cypher = "MATCH (n:" + TYPE + ") WHERE n.v = $v RETURN n.name AS name";
      try (final ResultSet rs = database.query("opencypher", cypher, Map.of("v", 3))) {
        assertThat((String) rs.next().getProperty("name")).isEqualTo("c");
      }

      final List<Result> rows = asyncQuery("opencypher", cypher, Map.of("v", 3));
      assertThat(rows).extracting(r -> (String) r.getProperty("name")).containsExactly("c");
    }

    @Test
    void asyncQueryWithoutParametersStillAnswers() {
      final List<Result> rows = asyncQuery("sql", "SELECT count(*) AS total FROM " + TYPE);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.getFirst().getProperty("total")).longValue()).isEqualTo(3L);
    }

    private List<Result> asyncQuery(final String language, final String query, final Object... args) {
      final Collector collector = new Collector();
      database.async().query(language, query, collector, args);
      return collector.await();
    }

    private List<Result> asyncQuery(final String language, final String query, final Map<String, Object> args) {
      final Collector collector = new Collector();
      database.async().query(language, query, collector, args);
      return collector.await();
    }

    private class Collector implements AsyncResultsetCallback {
      private final List<Result>               rows  = new CopyOnWriteArrayList<>();
      private final AtomicReference<Exception> error = new AtomicReference<>();

      @Override
      public void onComplete(final ResultSet resultset) {
        while (resultset.hasNext())
          rows.add(resultset.next());
      }

      @Override
      public void onError(final Exception exception) {
        error.set(exception);
      }

      private List<Result> await() {
        database.async().waitCompletion();
        assertThat(error.get()).as("the asynchronous query must not fail").isNull();
        return rows;
      }
    }
  }

  /**
   * Issue #9987: an update through {@code database.async().updateRecord(...)} did not advance the {@code updateRecord}
   * statistic the metrics endpoint publishes, because the asynchronous task re-implemented the update around the bare
   * page write and missed the counter. The guarantee is the full matrix: create, update and delete, each through the
   * synchronous and the asynchronous API, each advances its own statistic by exactly one - and an update made inside a
   * transaction is counted once, not a second time when the commit flushes it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RecordStatistics extends TestHelper {

    @Override
    public void beginTest() {
      database.getSchema().createDocumentType("Doc");
    }

    @Test
    void synchronousCreateUpdateAndDeleteEachCountOnce() {
      final long creates = stat("createRecord");
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());
      assertThat(stat("createRecord")).isEqualTo(creates + 1);

      final long updates = stat("updateRecord");
      database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("v", 1).save());
      assertThat(stat("updateRecord")).as("counted when made, not a second time by the commit flush")
          .isEqualTo(updates + 1);

      final long deletes = stat("deleteRecord");
      database.transaction(() -> database.deleteRecord(database.lookupByRID(rid[0], true)));
      assertThat(stat("deleteRecord")).isEqualTo(deletes + 1);
    }

    @Test
    void synchronousUpdateOutsideATransactionCountsOnce() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());

      // THE PATH THAT WRITES AT ONCE IN AN IMPLICIT TRANSACTION, THROUGH updateRecordNoLock(), WHICH MUST NOT COUNT AGAIN
      final boolean autoTransaction = database.isAutoTransaction();
      database.setAutoTransaction(true);
      try {
        final long updates = stat("updateRecord");
        database.lookupByRID(rid[0], true).asDocument().modify().set("v", 1).save();
        assertThat(stat("updateRecord")).isEqualTo(updates + 1);
      } finally {
        database.setAutoTransaction(autoTransaction);
      }
      assertThat(database.lookupByRID(rid[0], true).asDocument().getInteger("v")).isEqualTo(1);
    }

    @Test
    void asynchronousCreateUpdateAndDeleteEachCountOnce() {
      final long creates = stat("createRecord");
      final AtomicInteger created = new AtomicInteger();
      database.async().createRecord(database.newDocument("Doc").set("v", 0), r -> created.incrementAndGet());
      database.async().waitCompletion();
      assertThat(created.get()).isEqualTo(1);
      assertThat(stat("createRecord")).isEqualTo(creates + 1);

      final RID rid = database.query("sql", "select from Doc").next().getIdentity().get();

      final long updates = stat("updateRecord");
      final AtomicInteger updated = new AtomicInteger();
      database.async().updateRecord(database.lookupByRID(rid, true).asDocument().modify().set("v", 1),
          r -> updated.incrementAndGet());
      database.async().waitCompletion();
      assertThat(updated.get()).isEqualTo(1);
      assertThat(stat("updateRecord")).as("the asynchronous update is counted like the synchronous one")
          .isEqualTo(updates + 1);
      assertThat(database.lookupByRID(rid, true).asDocument().getInteger("v")).isEqualTo(1);

      final long deletes = stat("deleteRecord");
      final AtomicInteger deleted = new AtomicInteger();
      database.async().deleteRecord(database.lookupByRID(rid, true), r -> deleted.incrementAndGet());
      database.async().waitCompletion();
      assertThat(deleted.get()).isEqualTo(1);
      assertThat(stat("deleteRecord")).isEqualTo(deletes + 1);
    }

    @Test
    void asynchronousUpdateWithoutACallbackIsCountedToo() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());

      final long updates = stat("updateRecord");
      database.async().updateRecord(database.lookupByRID(rid[0], true).asDocument().modify().set("v", 2), null);
      database.async().waitCompletion();

      assertThat(stat("updateRecord")).isEqualTo(updates + 1);
      assertThat(database.lookupByRID(rid[0], true).asDocument().getInteger("v")).isEqualTo(2);
    }

    @Test
    void anAsynchronousUpdateVetoedByAListenerIsNeitherCountedNorReportedAsDone() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());

      final AtomicInteger afterUpdate = new AtomicInteger();
      final BeforeRecordUpdateListener veto = record -> false;
      final AfterRecordUpdateListener after = record -> afterUpdate.incrementAndGet();
      database.getSchema().getType("Doc").getEvents().registerListener(veto).registerListener(after);
      try {
        final long updates = stat("updateRecord");
        final AtomicInteger updated = new AtomicInteger();
        final MutableDocument doc = database.lookupByRID(rid[0], true).asDocument().modify().set("v", 3);
        database.async().updateRecord(doc, r -> updated.incrementAndGet());
        database.async().waitCompletion();

        assertThat(updated.get()).as("a vetoed update does not report success").isZero();
        assertThat(afterUpdate.get()).as("nor fire the after-update listeners").isZero();
        assertThat(stat("updateRecord")).as("nor count as an update").isEqualTo(updates);
      } finally {
        database.getSchema().getType("Doc").getEvents().unregisterListener(veto).unregisterListener(after);
      }
      assertThat(database.lookupByRID(rid[0], true).asDocument().getInteger("v")).isZero();
    }

    @Test
    void anAsynchronousUpdateFiresTheAfterUpdateListenersOnce() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());

      final AtomicInteger beforeUpdate = new AtomicInteger();
      final AtomicInteger afterUpdate = new AtomicInteger();
      final BeforeRecordUpdateListener before = record -> beforeUpdate.incrementAndGet() > 0;
      final AfterRecordUpdateListener after = record -> afterUpdate.incrementAndGet();
      database.getSchema().getType("Doc").getEvents().registerListener(before).registerListener(after);
      try {
        database.async().updateRecord(database.lookupByRID(rid[0], true).asDocument().modify().set("v", 4), null);
        database.async().waitCompletion();
      } finally {
        database.getSchema().getType("Doc").getEvents().unregisterListener(before).unregisterListener(after);
      }

      assertThat(beforeUpdate.get()).isEqualTo(1);
      assertThat(afterUpdate.get()).isEqualTo(1);
    }

    private long stat(final String name) {
      return ((Number) database.getStats().get(name)).longValue();
    }
  }
}
