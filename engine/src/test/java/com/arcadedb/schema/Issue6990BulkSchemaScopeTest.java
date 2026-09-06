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
import com.arcadedb.query.sql.parser.AlterTypeStatement;
import com.arcadedb.query.sql.parser.CompactIndexStatement;
import com.arcadedb.query.sql.parser.CreateIndexStatement;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.CreatePropertyStatement;
import com.arcadedb.query.sql.parser.CreateVertexTypeStatement;
import com.arcadedb.query.sql.parser.RebuildIndexStatement;
import com.arcadedb.query.sql.parser.RefreshMaterializedViewStatement;
import com.arcadedb.query.sql.parser.TruncateTypeStatement;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6990: a DDL script has to replicate as ONE Raft entry, not one per statement.
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
 * follower-side count lives in {@code Issue6990BulkSchemaScriptIT}.
 */
class Issue6990BulkSchemaScopeTest extends TestHelper {

  /**
   * Counts outermost schema recording sessions, which is the number of Raft entries the same code would publish under
   * HA. Nested calls are the ones that ride an already-open frame, so they are deliberately not counted.
   */
  private static final class SessionCountingDatabase {
    private final DatabaseInternal delegate;
    private final DatabaseInternal proxy;
    private       int              depth      = 0;
    private       int              outermost  = 0;

    private SessionCountingDatabase(final DatabaseInternal delegate) {
      this.delegate = delegate;
      this.proxy = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (p, method, args) -> {
            final boolean session = "recordFileChanges".equals(method.getName());
            if (session) {
              if (depth == 0)
                ++outermost;
              ++depth;
            }
            try {
              return method.invoke(delegate, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            } finally {
              if (session)
                --depth;
            }
          });
    }
  }

  /**
   * Sessions opened by the last {@link #countSchemaSessions(Runnable)} call, readable even when the work threw. That is
   * the only way to measure a script that fails part way, which is the case with the most to say about the scope.
   */
  private int lastSessionCount = -1;

  /**
   * Runs {@code work} with the counting database installed, and returns how many outermost sessions it opened. A
   * failure propagates; the count is still available in {@link #lastSessionCount}.
   */
  private int countSchemaSessions(final Runnable work) {
    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    final SessionCountingDatabase counter = new SessionCountingDatabase(local);
    local.setWrappedDatabaseInstance(counter.proxy);
    lastSessionCount = -1;
    try {
      work.run();
    } finally {
      local.setWrappedDatabaseInstance(previous);
      lastSessionCount = counter.outermost;
    }
    return counter.outermost;
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
   * The statements that must never be folded into a batch, pinned by the flag itself so a new subclass cannot inherit
   * the wrong answer silently. COMPACT INDEX is the loudest of them: compaction refuses to start while a recording
   * session is active (the #4063 guard), so inside a scope it would compact nothing and report success.
   */
  @Test
  void recordLevelDdlIsNotBatchable() {
    final DatabaseInternal db = (DatabaseInternal) database;

    assertThat(new CompactIndexStatement().isBulkSchemaScopeSafe(db)).isFalse();
    assertThat(new RebuildIndexStatement().isBulkSchemaScopeSafe(db)).isFalse();
    assertThat(new TruncateTypeStatement().isBulkSchemaScopeSafe(db)).isFalse();
    assertThat(new RefreshMaterializedViewStatement().isBulkSchemaScopeSafe(db)).isFalse();

    assertThat(new CreateVertexTypeStatement().isBulkSchemaScopeSafe(db)).isTrue();
    assertThat(new CreatePropertyStatement().isBulkSchemaScopeSafe(db)).isTrue();
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
