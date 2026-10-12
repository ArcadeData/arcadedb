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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue9741ConcurrentCreateIndexTest extends TestHelper {

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
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'$t'").hasMessageContaining("not defined");
    assertThatThrownBy(() -> createProperty.execute(database, (Map<String, Object>) null, new BasicCommandContext(), false))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not defined");
    assertThatThrownBy(() -> createType.execute(database, (Map<String, Object>) null, new BasicCommandContext(), false))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not defined");
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
