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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema statements racing each other and the execution plan cache.
 * <p>
 * A container for the regression suites of the feature; each nested class keeps its own fixture.
 */
class ConcurrentSchemaStatementTest {
  /**
   * Regression for #10067: CREATE ... TYPE ... IF NOT EXISTS checked that the type was missing and then created it, two steps, so two...
   * <p>
   * Issue #10067: {@code CREATE ... TYPE ... IF NOT EXISTS} checked that the type was missing and then created it, two steps, so two
   * statements racing on the same name had the loser fail with {@code Cannot create type ... because already exists} - which is what
   * two replicas asking the leader for the same new label at the same time produced. IF NOT EXISTS is the form that is safe to repeat
   * and to run concurrently: every statement answers, exactly one says it created the type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreateTypeIfNotExistsConcurrentTest extends TestHelper {

    private static final int THREADS = 8;
    private static final int ROUNDS  = 25;

    @Test
    void statementsRacingOnTheSameNameAllAnswerAndExactlyOneCreatesIt() throws Exception {
      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      try {
        for (final String kind : new String[] { "VERTEX", "EDGE", "DOCUMENT" })
          for (int round = 0; round < ROUNDS; round++) {
            final String name = "Racing" + kind + round;
            final CyclicBarrier together = new CyclicBarrier(THREADS);
            final AtomicInteger created = new AtomicInteger();
            final List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++)
              futures.add(pool.submit(() -> {
                together.await(30, TimeUnit.SECONDS);
                try (final ResultSet rs = database.command("sql", "CREATE " + kind + " TYPE " + name + " IF NOT EXISTS")) {
                  final Result row = rs.next();
                  assertThat(row.<String>getProperty("typeName")).isEqualTo(name);
                  if (Boolean.TRUE.equals(row.getProperty("created")))
                    created.incrementAndGet();
                }
                return null;
              }));
            for (final Future<?> future : futures)
              future.get(60, TimeUnit.SECONDS);

            assertThat(created.get()).as("%s: exactly one statement creates the type", name).isEqualTo(1);
            assertThat(database.getSchema().existsType(name)).isTrue();
          }
      } finally {
        pool.shutdownNow();
      }
    }

    /**
     * The same race for a statement that links super types (the composite type of a node with two labels), which since issue #10147
     * takes the async quiescence and then the write lock for the create and the links together: the losers still all answer, exactly
     * one creates the type, and it ends up under both of its super types.
     */
    @Test
    void statementsRacingOnTheSameTypeWithSuperTypesAllAnswerAndExactlyOneCreatesIt() throws Exception {
      database.command("sql", "CREATE VERTEX TYPE RacingBaseA").close();
      database.command("sql", "CREATE VERTEX TYPE RacingBaseB").close();
      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      try {
        for (int round = 0; round < 10; round++) {
          final String name = "RacingChild" + round;
          final CyclicBarrier together = new CyclicBarrier(THREADS);
          final AtomicInteger created = new AtomicInteger();
          final List<Future<?>> futures = new ArrayList<>();
          for (int t = 0; t < THREADS; t++)
            futures.add(pool.submit(() -> {
              together.await(30, TimeUnit.SECONDS);
              try (final ResultSet rs = database.command("sql", "CREATE VERTEX TYPE " + name + " IF NOT EXISTS EXTENDS RacingBaseA, RacingBaseB")) {
                if (Boolean.TRUE.equals(rs.next().getProperty("created")))
                  created.incrementAndGet();
              }
              return null;
            }));
          for (final Future<?> future : futures)
            future.get(60, TimeUnit.SECONDS);

          assertThat(created.get()).as("%s: exactly one statement creates the type", name).isEqualTo(1);
          assertThat(database.getSchema().getType(name).getSuperTypes()).extracting(type -> type.getName())
              .containsExactlyInAnyOrder("RacingBaseA", "RacingBaseB");
        }
      } finally {
        pool.shutdownNow();
      }
    }

    /** Without IF NOT EXISTS the loser of the race is still told the type is there: the guarantee is not widened. */
    @Test
    void withoutIfNotExistsTheTypeBeingThereIsStillAnError() {
      database.command("sql", "CREATE VERTEX TYPE Plain");

      assertThatThrownBy(() -> database.command("sql", "CREATE VERTEX TYPE Plain")).isInstanceOf(AlreadyExistsException.class)
          .hasMessageContaining("already exists");
      assertThatThrownBy(() -> database.getSchema().createVertexType("Plain")).isInstanceOf(TypeAlreadyExistsException.class)
          .hasMessageContaining("Cannot create type 'Plain' because already exists");
    }
  }

  /**
   * Regression for #9741: concurrent executions of the same CREATE INDEX IF NOT EXISTS text failed with a NullPointerException on...
   * <p>
   * Regression tests for issue #9741: concurrent executions of the same {@code CREATE INDEX IF NOT EXISTS} text failed with a
   * NullPointerException on {@code this.name}.
   * <p>
   * A parsed statement is cached and SHARED by every execution of the same text. {@code CreateIndexStatement} wrote the
   * auto-derived index name (and a {@code $variable}'s resolved type) into its own fields while it ran and put them back in a
   * {@code finally}, so the first execution to finish nulled the name under one still running. The same shape lived in
   * {@code CreatePropertyStatement} and {@code CreateTypeAbstractStatement} for a {@code $variable} type name, where an
   * overlapping execution could act on the type another execution resolved. Execution must not write to the statement at all.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConcurrentCreateIndexTest extends TestHelper {

    private static final String DDL = "CREATE INDEX IF NOT EXISTS ON V (a, b, c) UNIQUE";

    /**
     * Deterministic form of the race: while one execution builds the index, the shared statement must still read as written. Before
     * the fix {@code name} held the derived {@code V[a,b,c]} for the whole build, which is the window the reporter's threads hit.
     */
    @Test
    void sharedStatementIsNotWrittenWhileItRuns() throws Exception {
      createAndFillType(20_000);

      final CreateIndexStatement stmt = (CreateIndexStatement) new SQLAntlrParser(database).parse(DDL);
      final Identifier declaredType = stmt.typeName;

      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread runner = new Thread(() -> {
        try {
          stmt.execute(database, (Map<String, Object>) null, null, false).close();
        } catch (final Throwable e) {
          failure.set(e);
        }
      });

      final AtomicBoolean mutated = new AtomicBoolean();
      runner.start();
      while (runner.isAlive()) {
        VarHandle.acquireFence();
        if (stmt.name != null || stmt.typeName != declaredType)
          mutated.set(true);
        Thread.onSpinWait();
      }
      runner.join();

      assertThat(failure.get()).isNull();
      assertThat(mutated.get()).isFalse();
      assertThat(database.getSchema().existsIndex("V[a,b,c]")).isTrue();
    }

    /** The reporter's shape: many threads sending the same text at once, through the statement cache. */
    @Test
    void concurrentIdenticalCreateIndexIfNotExistsAllSucceed() throws Exception {
      final int threads = 8;
      for (int round = 0; round < 3; round++) {
        if (database.getSchema().existsType("V"))
          database.getSchema().dropType("V");
        createAndFillType(5_000);

        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Boolean> created = new ConcurrentLinkedQueue<>();
        final Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
          workers[i] = new Thread(() -> {
            try {
              barrier.await(30, TimeUnit.SECONDS);
              try (final ResultSet rs = database.command("sql", DDL)) {
                final Result row = rs.next();
                assertThat(row.<String>getProperty("name")).isEqualTo("V[a,b,c]");
                created.add(row.getProperty("created"));
              }
            } catch (final Throwable e) {
              failures.add(e);
            }
          });
          workers[i].start();
        }
        for (final Thread worker : workers)
          worker.join(TimeUnit.MINUTES.toMillis(2));

        assertThat(failures).isEmpty();
        assertThat(created).hasSize(threads);
        // Exactly one index, one sub-index per bucket: the concurrent creators must not have built theirs side by side
        final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("V[a,b,c]");
        assertThat(database.getSchema().getType("V").getAllIndexes(false)).hasSize(1);
        assertThat(index.getIndexesOnBuckets()).hasSize(database.getSchema().getType("V").getBuckets(false).size());
        assertThat(database.getSchema().getIndexes()).hasSize(1 + index.getIndexesOnBuckets().length);
        assertThat(created.stream().filter(Boolean::booleanValue).count()).isEqualTo(1L);
        assertThat(index.isUnique()).isTrue();
        assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO V SET a = 't', b = 's7', c = 'm'")))
            .isInstanceOf(DuplicatedKeyException.class);
      }
    }

    /**
     * Concurrent replacers of an incompatible index - Cypher's CREATE CONSTRAINT upgrading a plain index - all found the old
     * index before the first one dropped it, and dropping twice is a no-op, so every one of them went on to build its own.
     */
    @Test
    void concurrentReplacersBuildOneIndex() throws Exception {
      final int threads = 8;
      for (int round = 0; round < 10; round++) {
        if (database.getSchema().existsType("V"))
          database.getSchema().dropType("V");
        createAndFillType(5_000);
        database.getSchema().buildTypeIndex("V", new String[] { "b" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Boolean> built = new ConcurrentLinkedQueue<>();
        final Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
          workers[i] = new Thread(() -> {
            try {
              final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("V", new String[] { "b" });
              builder.withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withIgnoreIfExists(true).withReplaceIfIncompatible(true);
              barrier.await(30, TimeUnit.SECONDS);
              assertThat(builder.create().isUnique()).isTrue();
              built.add(builder.isBuilt());
            } catch (final Throwable e) {
              failures.add(e);
            }
          });
          workers[i].start();
        }
        for (final Thread worker : workers)
          worker.join(TimeUnit.MINUTES.toMillis(2));

        assertThat(failures).isEmpty();
        final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("V[b]");
        assertThat(index.isUnique()).isTrue();
        assertThat(database.getSchema().getType("V").getAllIndexes(false)).hasSize(1);
        assertThat(index.getIndexesOnBuckets()).hasSize(database.getSchema().getType("V").getBuckets(false).size());
        assertThat(built.stream().filter(Boolean::booleanValue).count()).isEqualTo(1L);
      }
    }

    /** A $variable type name resolves per execution and is never written back: a refused execution used to leave it resolved. */
    @Test
    void variableTypeNamesAreNotWrittenBackToTheStatement() {
      database.getSchema().createDocumentType("Existing");

      final CreateDocumentTypeStatement createType = (CreateDocumentTypeStatement) new SQLAntlrParser(database).parse(
          "CREATE DOCUMENT TYPE $t");
      assertThatThrownBy(() -> createType.execute(database, (Map<String, Object>) null, contextWith("t", "Existing"), false))
          .hasMessageContaining("already exists");
      assertThat(createType.name.getStringValue()).isEqualTo("$t");

      createType.execute(database, (Map<String, Object>) null, contextWith("t", "Fresh"), false).close();
      assertThat(createType.name.getStringValue()).isEqualTo("$t");
      assertThat(database.getSchema().existsType("Fresh")).isTrue();

      final CreatePropertyStatement createProperty = (CreatePropertyStatement) new SQLAntlrParser(database).parse(
          "CREATE PROPERTY $t.p STRING");
      createProperty.execute(database, (Map<String, Object>) null, contextWith("t", "Fresh"), false).close();
      assertThat(createProperty.typeName.getStringValue()).isEqualTo("$t");
      assertThat(database.getSchema().getType("Fresh").existsProperty("p")).isTrue();

      final CreateIndexStatement createIndex = (CreateIndexStatement) new SQLAntlrParser(database).parse(
          "CREATE INDEX ON $t (p) NOTUNIQUE");
      try (final ResultSet rs = createIndex.execute(database, (Map<String, Object>) null, contextWith("t", "Fresh"), false)) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("Fresh[p]");
      }
      assertThat(createIndex.typeName.getStringValue()).isEqualTo("$t");
      assertThat(createIndex.name).isNull();
      assertThat(database.getSchema().existsIndex("Fresh[p]")).isTrue();

      // An undefined variable names no type: refused with a message that says so, for each statement shape
      assertThatThrownBy(() -> createIndex.execute(database, (Map<String, Object>) null, new BasicCommandContext(), false))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("'$t'").hasMessageContaining("not defined");
      assertThatThrownBy(() -> createProperty.execute(database, (Map<String, Object>) null, new BasicCommandContext(), false))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("not defined");
      assertThatThrownBy(() -> createType.execute(database, (Map<String, Object>) null, new BasicCommandContext(), false))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("not defined");
    }

    /** A manual index name is kept on the $variable path too: the old code discarded it whenever the type was a variable. */
    @Test
    void manualIndexNameIsKeptWithAVariableTypeName() {
      database.getSchema().createDocumentType("Named").createProperty("p", Type.STRING);

      final CreateIndexStatement createIndex = (CreateIndexStatement) new SQLAntlrParser(database).parse(
          "CREATE INDEX byP ON $t (p) NOTUNIQUE");
      try (final ResultSet rs = createIndex.execute(database, (Map<String, Object>) null, contextWith("t", "Named"), false)) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo("byP");
        assertThat(row.<Boolean>getProperty("created")).isTrue();
      }
      assertThat(database.getSchema().getIndexByName("byP").getTypeName()).isEqualTo("Named");
      assertThat(createIndex.name.getStringValue()).isEqualTo("byP");
    }

    /** created comes from the builder that ran (TypeIndexBuilder.isBuilt()), for the specialised builders too. */
    @Test
    void createdIsReportedBySpecialisedBuilders() {
      database.getSchema().createDocumentType("Doc").createProperty("text", Type.STRING);

      for (final boolean expected : new boolean[] { true, false })
        try (final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (text) FULL_TEXT")) {
          final Result row = rs.next();
          assertThat(row.<Boolean>getProperty("created")).isEqualTo(expected);
          assertThat(row.<String>getProperty("name")).isEqualTo("Doc[text]");
        }
    }

    private static BasicCommandContext contextWith(final String variable, final Object value) {
      final BasicCommandContext context = new BasicCommandContext();
      context.setVariable(variable, value);
      return context;
    }

    private void createAndFillType(final int records) {
      database.getSchema().buildVertexType().withName("V").create();
      for (final String property : List.of("a", "b", "c"))
        database.getSchema().getType("V").createProperty(property, Type.STRING);

      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          database.newVertex("V").set("a", "t", "b", "s" + i, "c", "m").save();
      });
    }
  }

  /**
   * Regression for #10147: a CREATE ... TYPE statement runs in one schema frame, and whatever waits for the async workers to drain has...
   * <p>
   * Issue #10147: a {@code CREATE ... TYPE} statement runs in one schema frame, and whatever waits for the async workers to drain has to be
   * asked BEFORE the frame is opened, or it waits for workers that need the write lock the frame holds, until its timeout. Linking a super
   * type is known to the base class; building an index is known only to the statement that does it, which says so through
   * {@code createsAnIndex()}. Nothing forces a statement to say so, and a missing answer shows only under load, so what every kind of
   * statement answers is pinned here: a new kind of statement, or a new way for an existing one to build an index, is a decision to take
   * in this test.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreatesAnIndexHookTest extends TestHelper {

    @Test
    void onlyAUniqueEdgeTypeBuildsAnIndexWhenItIsCreated() {
      assertThat(statement("CREATE EDGE TYPE R UNIQUE").createsAnIndex()).as("a UNIQUE edge type builds its constraint's index").isTrue();
      assertThat(statement("CREATE EDGE TYPE R EXTENDS B UNIQUE").createsAnIndex()).isTrue();

      assertThat(statement("CREATE EDGE TYPE R").createsAnIndex()).isFalse();
      assertThat(statement("CREATE EDGE TYPE R LIGHTWEIGHT").createsAnIndex()).isFalse();
      assertThat(statement("CREATE EDGE TYPE R UNIDIRECTIONAL").createsAnIndex()).isFalse();
      assertThat(statement("CREATE VERTEX TYPE V").createsAnIndex()).isFalse();
      assertThat(statement("CREATE VERTEX TYPE V EXTENDS A, B").createsAnIndex()).as("linking is asked for through the super types").isFalse();
      assertThat(statement("CREATE DOCUMENT TYPE D").createsAnIndex()).isFalse();
    }

    private CreateTypeAbstractStatement statement(final String sql) {
      return (CreateTypeAbstractStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
    }
  }

  /**
   * Regression for #6671: the cache's last invalidation used to be checked against the planning start in a separate lock scope...
   * <p>
   * Issue #6671: the cache's last invalidation used to be checked against the planning start in a separate
   * lock scope ({@code synchronized(this)}) from the {@code put()} that followed it ({@code synchronized(map)}),
   * leaving a window in which a concurrent {@code invalidate()} (triggered by a DDL such as {@code DROP INDEX}) could
   * run between the check and the insert: the check would see "not yet invalidated", the DDL would invalidate and
   * clear the map, and then the stale plan would be inserted into the freshly-cleared map anyway.
   * <p>
   * This test reproduces the race deterministically (no real threads, hence no flakiness) by driving the two
   * operations - "plan a query" and "invalidate the cache" - in the exact interleaving that used to defeat the
   * check: capture {@code planningEpoch}, invalidate the cache (standing in for a concurrent DDL), then attempt to
   * cache a plan built at that {@code planningEpoch}. The single-lock, single-call {@code put(statement, plan,
   * planningEpoch)} introduced by the fix must refuse to cache it.
   */
  @Nested
  class ExecutionPlanCacheRaceTest {
    private DatabaseInternal database;

    @BeforeEach
    void setUp() {
      database = (DatabaseInternal) new DatabaseFactory("./target/databases/issue6671-execplan-race").create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void planBuiltBeforeAConcurrentInvalidationIsNeverCached() throws Exception {
      database.getSchema().createDocumentType("Race6671");
      final String stm = "SELECT FROM Race6671";

      final ExecutionPlanCache cache = database.getExecutionPlanCache();
      final CommandContext context = new BasicCommandContext().setDatabase(database);

      // No pause after createDocumentType's invalidation: put()'s guard counts invalidations rather than timestamping
      // them, so the warm-up query below is cached even when planned in the same millisecond (#8635).
      // Warm the cache once to obtain a real, valid ExecutionPlan instance to fight over.
      database.query("sql", stm).close();
      assertThat(cache.contains(stm)).isTrue();
      final ExecutionPlan plan = cache.get(stm, context);
      assertThat(plan).isNotNull();

      // Planning "began" here, before the concurrent DDL below invalidates the cache - exactly the interleaving
      // that used to slip through the two separately-locked calls.
      final long planningEpoch = cache.getInvalidationEpoch();

      // Stand in for a concurrent DDL (e.g. DROP INDEX) landing on another thread while planning was in flight.
      cache.invalidate();
      assertThat(cache.contains(stm)).isFalse();

      // The plan was built at planningEpoch, which the invalidation has moved past: it must be rejected.
      cache.put(stm, plan, planningEpoch);
      assertThat(cache.contains(stm)).as("a plan built before a concurrent invalidation must never be cached").isFalse();

      // A plan whose planning genuinely started after the invalidation must still be cached normally.
      final long freshPlanningEpoch = cache.getInvalidationEpoch();
      cache.put(stm, plan, freshPlanningEpoch);
      assertThat(cache.contains(stm)).as("a plan planned after the invalidation must be cached").isTrue();
    }
  }
}
