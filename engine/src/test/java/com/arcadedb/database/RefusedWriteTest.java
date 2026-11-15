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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableLightEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.DeleteStep;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.LimitExecutionStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SaveElementStep;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the writes the engine refuses (a unique index, a permission, a write veto, an HA refusal): a refused write leaves nothing behind and is reported as refused. Each nested class keeps the regression of one reported issue (see the Javadoc of each).
 */
public class RefusedWriteTest {
  /**
   * Issue #7467: a {@code save()} that a unique index refuses must leave the transaction exactly as it found it.
   * <p>
   * {@code LocalDatabase.createRecordNoLock} writes the record into the bucket, assigns its identity, increments
   * the bucket's record delta and puts it in the transaction's record cache, and only THEN runs
   * {@code DocumentIndexer.createDocument}, which is where the unique check lives. So a
   * {@link DuplicatedKeyException} raised there used to leave the record body in the transaction with nothing to
   * find it by, and any caller that tallies the refusal and carries on - the {@code /ws} insert session, the gRPC
   * insert stream, an HTTP batch, a SQL script with its own error handling - committed a record that:
   * <ul>
   * <li>exists in the bucket and is returned by a full scan,</li>
   * <li>is counted by {@code count(*)},</li>
   * <li>is NOT in the unique index that was supposed to forbid it, so no lookup by key finds it,</li>
   * <li>and was acknowledged to the client as not written.</li>
   * </ul>
   * Three views of the system that disagree, and no single one of them reveals the problem - which is why the
   * retraction belongs in the engine rather than in each caller.
   * <p>
   * The inline refusal is the same-transaction twin: {@code TransactionIndexContext.addIndexKeyLock} compares
   * against the keys this transaction has already queued. A duplicate of a COMMITTED record is decided at commit
   * time instead, which fails the whole transaction and was never the defect - the last test here keeps that
   * behaviour pinned.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FailedCreateLeavesNothingInTheTransaction extends TestHelper {
    private static final String TYPE = "Keyed7467";

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "name");
    }

    /** The defect itself, on the shape the report describes: the refused row must not reach the commit. */
    @Test
    void aRefusedRowIsNotCommittedWithTheRestOfTheTransaction() {
      database.transaction(() -> {
        database.newDocument(TYPE).set("name", "dup").set("row", 1).save();

        assertThatThrownBy(() -> database.newDocument(TYPE).set("name", "dup").set("row", 2).save())
            .isInstanceOf(DuplicatedKeyException.class);

        // The caller carries on with the rest of its batch, exactly as the /ws session does.
        database.newDocument(TYPE).set("name", "other").set("row", 3).save();
      });

      assertThat(database.countType(TYPE, false)).as("the refused row must not be in the bucket").isEqualTo(2);
      assertThat(scanCount()).as("a full scan must agree with count(*)").isEqualTo(2);
      assertThat(rowOfKey("dup")).as("the key must belong to the row that took it").isEqualTo(1);
      assertThat(rowOfKey("other")).isEqualTo(3);
    }

    /**
     * The index is the authority afterwards: the key is taken by exactly one record, and a later transaction is
     * still refused. Before the fix the index held one entry and the bucket held two records, so which of them a
     * later insert collided with depended on what the index happened to hold.
     */
    @Test
    void theUniqueIndexStillHoldsExactlyOneEntryForTheKey() {
      database.transaction(() -> {
        database.newDocument(TYPE).set("name", "dup").set("row", 1).save();
        assertThat(catchThrowable(() -> database.newDocument(TYPE).set("name", "dup").set("row", 2).save()))
            .isInstanceOf(DuplicatedKeyException.class);
      });

      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE name = 'dup'")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }

      assertThatThrownBy(() -> database.transaction(
          () -> database.newDocument(TYPE).set("name", "dup").set("row", 4).save()))
          .isInstanceOf(DuplicatedKeyException.class);

      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }

    /**
     * The document the engine handed back is re-insertable. Its identity was assigned by the bucket write that has
     * just been taken back, so leaving it set would make the next {@code save()} an UPDATE of a record that is not
     * there - which is the same reasoning {@code TransactionContext.rollback} applies to every new record.
     */
    @Test
    void theRefusedDocumentIsCleanlyReInsertableUnderAFreeKey() {
      final MutableDocument[] refused = new MutableDocument[1];

      database.transaction(() -> {
        database.newDocument(TYPE).set("name", "dup").save();

        refused[0] = database.newDocument(TYPE).set("name", "dup");
        assertThat(catchThrowable(refused[0]::save)).isInstanceOf(DuplicatedKeyException.class);
        assertThat(refused[0].getIdentity()).as("the identity of a create that was taken back").isNull();

        refused[0].set("name", "free");
        refused[0].save();
      });

      assertThat(database.countType(TYPE, false)).isEqualTo(2);
      assertThat(refused[0].getIdentity()).isNotNull();
    }

    /**
     * More than one index on the type: the undo has to take back the entries the indexes BEFORE the refusing one
     * already hold, or the record is gone and they still point at it.
     */
    @Test
    void theEntriesOfTheIndexesBeforeTheRefusingOneAreTakenBackToo() {
      // The NON-unique index is created first so the indexer reaches it first: by the time the unique one refuses
      // the row, this one already holds an entry for a record that is about to be taken back.
      final DocumentType type = database.getSchema().createDocumentType("Two7467");
      type.createProperty("tag", Type.STRING);
      type.createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Two7467", "tag");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Two7467", "name");

      database.transaction(() -> {
        database.newDocument("Two7467").set("name", "dup").set("tag", "shared").save();
        assertThat(catchThrowable(() -> database.newDocument("Two7467").set("name", "dup").set("tag", "shared").save()))
            .isInstanceOf(DuplicatedKeyException.class);
      });

      try (final ResultSet rs = database.query("sql", "SELECT FROM Two7467 WHERE tag = 'shared'")) {
        assertThat(rs.stream().count()).as("the non-unique index must not hold an entry for the refused row").isEqualTo(1);
      }
      assertThat(database.countType("Two7467", false)).isEqualTo(1);
    }

    /** A vertex goes down the same path, so the graph types get the same guarantee. */
    @Test
    void aRefusedVertexIsNotCommittedEither() {
      database.getSchema().createVertexType("V7467").createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V7467", "name");

      database.transaction(() -> {
        final MutableVertex first = database.newVertex("V7467").set("name", "dup");
        first.save();
        assertThat(catchThrowable(() -> database.newVertex("V7467").set("name", "dup").save()))
            .isInstanceOf(DuplicatedKeyException.class);
      });

      assertThat(database.countType("V7467", false)).isEqualTo(1);
    }

    /**
     * {@code RESTORE} writes the record the same way and hands it to the same indexer afterwards, so it left the
     * same residue behind and comes back the same way (claude-review on PR #7936).
     * <p>
     * The inline check is reached by two RESTOREs of one key in ONE transaction: the comment that used to sit on
     * that call said a duplicate is decided at commit, which is true only of a duplicate of COMMITTED state - a
     * key this transaction has already queued is decided at the {@code put}, exactly as it is on the CREATE path.
     */
    @Test
    void aRefusedRestoreIsNotCommittedEither() {
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        rids[0] = database.newDocument(TYPE).set("name", "first").save().getIdentity();
        rids[1] = database.newDocument(TYPE).set("name", "second").save().getIdentity();
      });
      database.transaction(() -> {
        database.lookupByRID(rids[0], true).asDocument().delete();
        database.lookupByRID(rids[1], true).asDocument().delete();
      });
      assertThat(database.countType(TYPE, false)).isZero();

      database.transaction(() -> {
        database.command("sql", "RESTORE DOCUMENT " + TYPE + " RID " + rids[0] + " SET name = 'dup'").close();

        assertThat(catchThrowable(() -> database.command("sql",
            "RESTORE DOCUMENT " + TYPE + " RID " + rids[1] + " SET name = 'dup'").close()))
            .as("the second restore of the same key must be refused")
            .isInstanceOf(DuplicatedKeyException.class);
      });

      assertThat(database.countType(TYPE, false)).as("the refused restore must not be in the bucket").isEqualTo(1);
      assertThat(scanCount()).isEqualTo(1);

      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE name = 'dup'")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    }

    /**
     * What happens when the retraction itself cannot run (CodeRabbit on PR #7936). If the physical free of the
     * refused record fails, the body stays in the bucket while its index entries have just been taken away -
     * exactly the state the undo exists to prevent - so the transaction must not be committable afterwards. A
     * warning alone would have left the caller free to publish it.
     * <p>
     * The refusal is asserted on the mechanism rather than on a forced disk failure: the alternative is a mock
     * bucket, which would pin the shape of {@code undoRecordWrite}'s call rather than the property that matters,
     * and the property is that a transaction told it cannot be published refuses its own commit and says why.
     */
    @Test
    void aTransactionWhoseRetractionFailedCannotBeCommitted() {
      database.begin();
      try {
        database.newDocument(TYPE).set("name", "in-flight").save();

        final TransactionContext transaction = ((DatabaseInternal) database).getTransaction();
        transaction.setRollbackOnly("record #1:0 could not be taken back after its indexing refused it");
        // The first reason wins, so the message names what went wrong rather than what noticed last.
        transaction.setRollbackOnly("something noticed later");

        assertThatThrownBy(database::commit)
            .isInstanceOf(TransactionException.class)
            .hasMessageContaining("could not be taken back")
            .hasMessageContaining("Roll it back");

        // Still usable for the rollback the caller's error handling is about to reach, and not before.
        assertThat(database.isTransactionActive()).isTrue();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(database.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();

      // The refusal belonged to that transaction, not to the context the next begin() reuses.
      database.transaction(() -> database.newDocument(TYPE).set("name", "after").save());
      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }

    /**
     * The counter-case that keeps the undo honest: a duplicate of a record that is already COMMITTED is not
     * decided at {@code save()} at all - the check runs inside the commit lock - so the whole transaction fails
     * and nothing of it lands. Unchanged by this fix, and the reason the /ws session documents {@code per_stream}
     * as reporting such a conflict on the commit frame rather than on the row.
     */
    @Test
    void aDuplicateOfACommittedRecordStillFailsTheWholeTransaction() {
      database.transaction(() -> database.newDocument(TYPE).set("name", "committed").save());

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument(TYPE).set("name", "fresh").save();
        database.newDocument(TYPE).set("name", "committed").save();
      })).isInstanceOf(DuplicatedKeyException.class);

      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }

    private long scanCount() {
      final long[] total = new long[1];
      database.transaction(() -> database.scanType(TYPE, false, record -> {
        total[0]++;
        return true;
      }));
      return total[0];
    }

    private int rowOfKey(final String name) {
      try (final ResultSet rs = database.query("sql", "SELECT row FROM " + TYPE + " WHERE name = ?", name)) {
        return rs.next().<Integer>getProperty("row");
      }
    }
  }

  /**
   * Issue #8053: the rollback-only marker of issue #7467 was read at ONE of the two commit entry points.
   * <p>
   * {@link TransactionContext#setRollbackOnly} exists so a transaction whose half-written record could not be
   * freed cannot be published - "committing it would publish a record no index entry points at". Its only read
   * was at the top of {@link TransactionContext#commit()}, and {@code commit()} is not the method an HA node
   * calls: {@code RaftReplicatedDatabase.commit()} takes the two phases apart itself so it can put the WAL bytes
   * on the wire between them, and drives {@link TransactionContext#commit1stPhase(boolean)} directly. So on the
   * deployment where the consequence is worst - the state is not merely committed locally but shipped as a
   * {@code TX_ENTRY} and applied on every follower - the marker did nothing.
   * <p>
   * The refusal now lives in {@code commit1stPhase}, the method every commit path converges on and the one that
   * already decides whether there is anything to publish. {@code commit()} keeps it by calling that method, and
   * no second copy of the guard can drift away from the first - a guard with two copies is the shape that
   * produced this gap.
   * <p>
   * The {@code commit1stPhase} rows here are the engine-level pin; the replicated wrapper's own two call sites
   * are pinned in {@code Issue8053RollbackOnlyRefusedOnTheReplicatedCommitPathTest} in the {@code ha-raft}
   * module.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class RollbackOnlyRefusedByCommit1stPhase extends TestHelper {
    private static final String TYPE   = "Keyed8053";
    private static final String REASON = "record #1:0 could not be taken back after its indexing refused it";

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "name");
    }

    /**
     * The gap itself, on the leader's own argument value: the HA wrapper asks for phase 1 and never for
     * {@code commit()}, so phase 1 is what has to refuse. Before the fix this returned the WAL bytes.
     */
    @Test
    void phase1RefusesARollbackOnlyTransactionOnTheLeader() {
      assertPhase1Refuses(true);
    }

    /**
     * The same on a replica ORIGINATING its own write: {@code RaftReplicatedDatabase.commit()} passes its own
     * {@code isLeader()} through to phase 1, so {@code false} is a real production argument value and not a
     * variant invented for the test.
     */
    @Test
    void phase1RefusesARollbackOnlyTransactionOnAReplica() {
      assertPhase1Refuses(false);
    }

    /**
     * The standalone path keeps the behaviour issue #7467 gave it, now by inheritance rather than by its own
     * copy of the test: {@code LocalDatabase.commit()} reaches {@code commit()}, which reaches phase 1.
     */
    @Test
    void theStandaloneCommitStillRefusesIt() {
      database.begin();
      try {
        database.newDocument(TYPE).set("name", "in-flight").save();
        ((DatabaseInternal) database).getTransaction().setRollbackOnly(REASON);

        assertThatThrownBy(database::commit)
            .isInstanceOf(TransactionException.class)
            .hasMessageContaining("could not be taken back")
            .hasMessageContaining("Roll it back");
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(database.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();
    }

    /** The guard must not cost a healthy transaction its commit: phase 1 still publishes when nothing marked it. */
    @Test
    void anUnmarkedTransactionStillCommitsThroughPhase1() {
      database.begin();
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      database.newDocument(TYPE).set("name", "healthy").save();

      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      assertThat(phase1).as("an unmarked transaction still produces the WAL bytes to publish").isNotNull();
      tx.commit2ndPhase(phase1);

      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }

    /**
     * Drives phase 1 the way {@code RaftReplicatedDatabase.commit()} drives it and asserts the three things the
     * caller needs: it is refused, it is refused BEFORE the transaction leaves {@code BEGUN} so the caller's own
     * error handling can still roll it back, and nothing of it is durable afterwards.
     */
    private void assertPhase1Refuses(final boolean isLeader) {
      database.begin();
      try {
        database.newDocument(TYPE).set("name", "in-flight").save();

        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        tx.setRollbackOnly(REASON);

        assertThatThrownBy(() -> tx.commit1stPhase(isLeader))
            .as("the WAL bytes of a transaction that cannot be published must never be produced")
            .isInstanceOf(TransactionException.class)
            .hasMessageContaining("could not be taken back")
            .hasMessageContaining("Roll it back");

        assertThat(tx.getStatus())
            .as("refused before the phase starts, so the caller can still roll it back")
            .isEqualTo(TransactionContext.STATUS.BEGUN);
        assertThat(tx.getRollbackOnlyReason()).isEqualTo(REASON);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(database.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();

      // The refusal belonged to that transaction, not to the context the next begin() reuses.
      database.transaction(() -> database.newDocument(TYPE).set("name", "after-" + isLeader).save());
      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }
  }

  /**
   * Issue #8075 (parent of #8051 and #8066), follow-ups to #7467.
   * <ul>
   * <li>#8051: the engine's retraction of a refused create frees the body through the permission-checked delete, so a
   * role with {@code createRecord} but not {@code deletedRecord} turned one refused row into a rollback-only
   * transaction.</li>
   * <li>#8066: a refused in-transaction UPDATE left its new key queued in the transaction's index changes, so the
   * chunk's commit died naming the row already reported refused.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RefusedRowLeavesNoTrace {
    private static final String PATH = "target/databases/Issue8075RefusedRowLeavesNoTraceTest";
    private static final String TYPE = "Keyed8075";

    private DatabaseFactory factory;
    private Database        database;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "name");
    }

    @AfterEach
    void tearDown() {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
      database.drop();
      factory.close();
    }

    /** #8051: an ingestion role (create + read, no delete) must still get the one-row refusal, not a doomed batch. */
    @Test
    void aRefusedCreateByARoleWithoutDeleteDoesNotDoomTheTransaction() {
      bindIngestionUser();

      database.transaction(() -> {
        database.newDocument(TYPE).set("name", "dup").set("row", 1).save();
        assertThat(catchThrowable(() -> database.newDocument(TYPE).set("name", "dup").set("row", 2).save()))
            .isInstanceOf(DuplicatedKeyException.class);
        database.newDocument(TYPE).set("name", "other").set("row", 3).save();
      });

      assertThat(database.countType(TYPE, false)).isEqualTo(2);
    }

    /** #8051 on the restore path. */
    @Test
    void aRefusedRestoreByARoleWithoutDeleteDoesNotDoomTheTransaction() {
      final RID[] freed = new RID[2];
      database.transaction(() -> {
        freed[0] = database.newDocument(TYPE).set("name", "s0").save().getIdentity();
        freed[1] = database.newDocument(TYPE).set("name", "s1").save().getIdentity();
      });
      database.transaction(() -> {
        database.deleteRecord(freed[0].asDocument());
        database.deleteRecord(freed[1].asDocument());
      });

      bindIngestionUser();

      database.transaction(() -> {
        // The key is queued by THIS transaction, so the duplicate is decided inline (against committed state it is
        // decided at commit). The new record takes one of the two freed slots; the other is the restore target.
        final RID taken = database.newDocument(TYPE).set("name", "x").save().getIdentity();
        final RID target = taken.equals(freed[0]) ? freed[1] : freed[0];
        final MutableDocument shell = database.newDocument(TYPE).set("name", "x");
        final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(target.getBucketId());
        assertThat(catchThrowable(() -> ((DatabaseInternal) database).getEmbedded().restoreRecord(shell, bucket, target.getPosition())))
            .isInstanceOf(DuplicatedKeyException.class);
      });

      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }

    /** #8066: a refused update must not leave its key queued: the rest of the chunk commits. */
    @Test
    void aRefusedUpdateLeavesNoKeyQueuedAndTheRestOfTheChunkCommits() {
      database.transaction(() -> {
        database.newDocument(TYPE).set("name", "a").set("row", 1).save();
        final MutableDocument mb = database.newDocument(TYPE).set("name", "b").set("row", 2);
        mb.save();
        mb.set("name", "a");
        assertThat(catchThrowable(mb::save)).isInstanceOf(DuplicatedKeyException.class);

        database.newDocument(TYPE).set("name", "c").set("row", 3).save();
      });

      assertThat(database.countType(TYPE, false)).isEqualTo(3);
      assertThat(rowOf("a")).isEqualTo(1);
      assertThat(rowOf("b")).as("the refused update must not have been written").isEqualTo(2);
      assertThat(rowOf("c")).isEqualTo(3);
    }

    /** A record already queued by an earlier update keeps its queue entry when a later update of it is refused. */
    @Test
    void aRefusedUpdateOfAnAlreadyQueuedRecordKeepsItsEarlierUpdate() {
      // Same transaction throughout: a duplicate of a COMMITTED key is decided at commit, not inline
      database.transaction(() -> {
        database.newDocument(TYPE).set("name", "a").set("row", 1).save();
        final MutableDocument mb = database.newDocument(TYPE).set("name", "b").set("row", 2);
        mb.save();
        mb.set("row", 20).save();
        mb.set("name", "a");
        assertThat(catchThrowable(mb::save)).isInstanceOf(DuplicatedKeyException.class);
        mb.set("name", "b");
        mb.save();
      });

      assertThat(database.countType(TYPE, false)).isEqualTo(2);
      assertThat(rowOf("a")).isEqualTo(1);
      assertThat(rowOf("b")).isEqualTo(20);
    }

    private int rowOf(final String key) {
      return database.query("sql", "SELECT FROM " + TYPE + " WHERE name = '" + key + "'").next().<Integer>getProperty("row");
    }

    private void bindIngestionUser() {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "ingest";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return access != ACCESS.DELETE_RECORD;
        }

        @Override
        public boolean requestAccessOnType(final String typeName, final ACCESS access) {
          return access != ACCESS.DELETE_RECORD;
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }
  }

  /**
   * Regression test for issue #8922. A row refused by a unique index after its body was written is taken back by
   * {@code undoRecordWrite}, which freed the primary record only: the externalised values of an EXTERNAL property stayed
   * committed in the paired {@code *_ext} bucket, unreferenced, and its persisted record count stayed one too high.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RefusedRowExternalValue extends TestHelper {

    private static final String BLOB = "x".repeat(200);

    @Test
    void refusedRowInCallerOwnedTransactionLeavesNoExternalRecord() {
      final DocumentType type = database.getSchema().createDocumentType("T");
      type.createProperty("id", Type.STRING);
      type.createProperty("blob", Type.STRING).setExternal(true);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "T", "id");

      database.transaction(() -> {
        database.newDocument("T").set("id", "a").set("blob", "A" + BLOB).save();
        database.newDocument("T").set("id", "b").set("blob", "B" + BLOB).save();
        assertThat(catchThrowable(() -> database.newDocument("T").set("id", "a").set("blob", "C" + BLOB).save()))
            .isInstanceOf(DuplicatedKeyException.class);
      });

      assertThat(database.countType("T", false)).isEqualTo(2);
      assertThat(externalCount(type)).isEqualTo(2);

      database.close();
      database = factory.open();

      final DocumentType reopened = database.getSchema().getType("T");
      assertThat(externalCount(reopened)).isEqualTo(2);

      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        final Object orphans = rs.next().getProperty("orphanedExternalRecords");
        assertThat(orphans == null ? 0L : ((Number) orphans).longValue()).isZero();
      }
    }

    private long externalCount(final DocumentType type) {
      long total = 0;
      for (final Bucket bucket : type.getBuckets(false)) {
        final Integer externalId = ((LocalDocumentType) type).getExternalBucketIdFor(bucket.getFileId());
        assertThat(externalId).isNotNull();
        total += ((LocalBucket) database.getSchema().getBucketById(externalId)).count();
      }
      return total;
    }
  }

  /**
   * Issue #9305: {@code DELETE_RECORD} was checked only by {@code LocalBucket.deleteRecord}, the LAST step of a delete, after the
   * index entries, the EXTERNAL values and the vertex/edge links had already been removed. A caller owning its transaction that
   * carried on after the refusal committed a half-applied delete. The user below resolves permissions as the server does: the
   * grant map is keyed on the type's INVOLVED buckets, every other file id is default-allow. Since #9637 the involved buckets
   * include the paired {@code _ext} bucket too, so the refusal is now raised on the primary bucket before any cleanup starts AND
   * would be raised again on the paired bucket.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RefusedDeleteLeavesNoTrace {
    private static final String PATH = "target/databases/Issue9305RefusedDeleteLeavesNoTraceTest";
    private static final String BLOB = "x".repeat(200);

    private DatabaseFactory factory;
    private Database        database;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
      database.drop();
      factory.close();
    }

    @Test
    void refusedDocumentDeleteKeepsIndexAndExternalValues() {
      final DocumentType type = database.getSchema().createDocumentType("T");
      type.createProperty("id", Type.STRING);
      type.createProperty("blob", Type.STRING).setExternal(true);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "T", "id");

      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("T").set("id", "a").set("blob", BLOB).save().getIdentity());

      bindUserRefusingDeleteOn(type);

      database.begin();
      final Throwable refused = catchThrowable(() -> database.deleteRecord(rid[0].asDocument()));
      assertThat(refused).isInstanceOf(SecurityException.class);
      database.commit();

      assertThat(database.countType("T", false)).isEqualTo(1);
      assertThat(externalCount(type)).isEqualTo(1);
      final IndexCursor cursor = database.getSchema().getIndexByName("T[id]").get(new Object[] { "a" });
      assertThat(cursor.hasNext()).isTrue();
      assertThat((String) rid[0].asDocument().getString("blob")).isEqualTo(BLOB);
    }

    @Test
    void refusedVertexDeleteKeepsEdgesAndIndex() {
      final DocumentType v = database.getSchema().createVertexType("V");
      v.createProperty("id", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E");

      final RID[] ids = new RID[2];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V").set("id", "a").save();
        final MutableVertex b = database.newVertex("V").set("id", "b").save();
        a.newEdge("E", b);
        ids[0] = a.getIdentity();
        ids[1] = b.getIdentity();
      });

      bindUserRefusingDeleteOn(v);

      database.begin();
      assertThat(catchThrowable(() -> database.deleteRecord(ids[0].asVertex()))).isInstanceOf(SecurityException.class);
      database.commit();

      assertThat(database.countType("E", false)).isEqualTo(1);
      assertThat(ids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
      assertThat(ids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
      assertThat(database.getSchema().getIndexByName("V[id]").get(new Object[] { "a" }).hasNext()).isTrue();
    }

    @Test
    void refusedEdgeDeleteKeepsBothEndpointsLinked() {
      database.getSchema().createVertexType("V");
      final DocumentType e = database.getSchema().createEdgeType("E");

      final RID[] ids = new RID[3];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V").save();
        final MutableVertex b = database.newVertex("V").save();
        ids[0] = a.getIdentity();
        ids[1] = b.getIdentity();
        ids[2] = a.newEdge("E", b).getIdentity();
      });

      bindUserRefusingDeleteOn(e);

      database.begin();
      assertThat(catchThrowable(() -> database.deleteRecord(ids[2].asEdge()))).isInstanceOf(SecurityException.class);
      database.commit();

      assertThat(database.countType("E", false)).isEqualTo(1);
      assertThat(ids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
      assertThat(ids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
    }

    /** The refusal comes from a bucket the vertex delete reaches only AFTER it started (its edge): the commit must not publish that. */
    @Test
    void refusalRaisedMidDeleteDoomsTheTransaction() {
      final DocumentType v = database.getSchema().createVertexType("V");
      final DocumentType e = database.getSchema().createEdgeType("E");

      final RID[] ids = new RID[2];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V").save();
        final MutableVertex b = database.newVertex("V").save();
        a.newEdge("E", b);
        ids[0] = a.getIdentity();
        ids[1] = b.getIdentity();
      });

      bindUserRefusingDeleteOn(e);

      database.begin();
      assertThat(catchThrowable(() -> database.deleteRecord(ids[0].asVertex()))).isInstanceOf(SecurityException.class);
      assertThat(catchThrowable(database::commit)).isNotNull();
      if (database.isTransactionActive())
        database.rollback();

      assertThat(database.countType("V", false)).isEqualTo(2);
      assertThat(database.countType("E", false)).isEqualTo(1);
      assertThat(ids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
      assertThat(ids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
    }

    @Test
    void grantedDeleteStillWorks() {
      final DocumentType type = database.getSchema().createDocumentType("T");
      type.createProperty("blob", Type.STRING).setExternal(true);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("T").set("blob", BLOB).save().getIdentity());

      database.transaction(() -> database.deleteRecord(rid[0].asDocument()));

      assertThat(database.countType("T", false)).isZero();
      assertThat(externalCount(type)).isZero();
    }

    private long externalCount(final DocumentType type) {
      long total = 0;
      for (final Bucket bucket : type.getBuckets(false)) {
        final Integer externalId = ((LocalDocumentType) type).getExternalBucketIdFor(bucket.getFileId());
        assertThat(externalId).isNotNull();
        total += ((LocalBucket) database.getSchema().getBucketById(externalId)).count();
      }
      return total;
    }

    /** Same resolution as ServerSecurityDatabaseUser: only the type's involved bucket ids are listed, the rest is default-allow. */
    private void bindUserRefusingDeleteOn(final DocumentType type) {
      final Set<Integer> gated = new HashSet<>();
      for (final Bucket bucket : type.getInvolvedBuckets())
        gated.add(bucket.getFileId());

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "nodelete";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return access != ACCESS.DELETE_RECORD || !gated.contains(fileId);
        }

        @Override
        public boolean requestAccessOnType(final String typeName, final ACCESS access) {
          return true;
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }
  }

  /**
   * Issue #8270: a database an HA server opened before it could replicate it refuses writes, and only writes, until
   * the refusal is lifted.
   * <p>
   * Each entry point a write can take into the engine is driven on its own:
   * <ul>
   * <li>a SQL statement in a managed transaction;</li>
   * <li>an explicit transaction creating a record, and one updating a record;</li>
   * <li>a transaction begun under the refusal and committed after it was lifted;</li>
   * <li>a schema change creating files, one that only rewrites the schema configuration, and a database setting - the
   * kinds that do not end in a commit.</li>
   * </ul>
   */
  @Nested
  class WriteRefusal extends TestHelper {

    private static final String TYPE   = "Issue8270Doc";
    private static final String REASON = "the test says so";

    private RID existing;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE);
        existing = database.newDocument(TYPE).set("name", "before").save().getIdentity();
      });
    }

    @Override
    protected void endTest() {
      local().acceptWrites(REASON);
    }

    private LocalDatabase local() {
      return (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    }

    @Test
    void aStatementInAManagedTransactionIsRefusedAndLeavesNothingBehind() {
      local().refuseWrites(REASON);

      // The shape an HTTP command takes: the handler runs the statement inside database.transaction(), which retries a
      // NeedRetryException and rethrows it once the retries are spent.
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET name = 'refused'")))
          .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);

      assertThat(database.isTransactionActive()).isFalse();
      assertThat(database.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE name = 'refused'").next()
          .<Long>getProperty("c")).isEqualTo(0L);
    }

    @Test
    void anExplicitTransactionCreatingARecordIsRefused() {
      local().refuseWrites(REASON);

      database.begin();
      database.newDocument(TYPE).set("name", "refused").save();
      assertThatThrownBy(() -> database.commit()).isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
      // Refused before the commit started, like a rollback-only transaction: the caller rolls it back.
      assertThat(database.isTransactionActive()).isTrue();
      database.rollback();

      assertThat(database.countType(TYPE, true)).isEqualTo(1L);
    }

    @Test
    void anExplicitTransactionUpdatingARecordIsRefused() {
      local().refuseWrites(REASON);

      database.begin();
      database.lookupByRID(existing, true).asDocument().modify().set("name", "refused").save();
      assertThatThrownBy(() -> database.commit()).isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
      database.rollback();

      assertThat(database.lookupByRID(existing, true).asDocument().getString("name")).isEqualTo("before");
    }

    @Test
    void aTransactionBegunWhileWritesWereRefusedIsRefusedAfterTheyAreAccepted() {
      // A handle resolved while the database waited to be wrapped may be the plain database even after the wrap: the
      // transaction it began then must not commit locally just because the refusal has been lifted meanwhile.
      local().refuseWrites(REASON);
      database.begin();
      database.newDocument(TYPE).set("name", "begun-refused").save();
      local().acceptWrites(REASON);

      assertThatThrownBy(() -> database.commit()).isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
      database.rollback();
      assertThat(database.countType(TYPE, true)).isEqualTo(1L);

      // The next transaction begins with writes accepted and commits.
      database.transaction(() -> database.newDocument(TYPE).set("name", "after").save());
      assertThat(database.countType(TYPE, true)).isEqualTo(2L);
    }

    @Test
    void aSchemaChangeIsRefused() {
      local().refuseWrites(REASON);

      assertThatThrownBy(() -> database.getSchema().createDocumentType("Issue8270Refused"))
          .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
      assertThat(database.getSchema().existsType("Issue8270Refused")).isFalse();
    }

    @Test
    void aConfigurationOnlySchemaChangeIsRefused() {
      // ALTER PROPERTY rewrites the schema configuration and nothing else: no commit, no file created.
      database.getSchema().getType(TYPE).createProperty("name", String.class);
      local().refuseWrites(REASON);

      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".name MANDATORY true"))
          .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
      assertThat(database.getSchema().getType(TYPE).getProperty("name").isMandatory()).isFalse();
    }

    @Test
    void aDatabaseSettingChangeIsRefused() {
      local().refuseWrites(REASON);

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy'"))
          .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    }

    @Test
    void aPrivilegeCheckThatIsNotAWriteIsNotRefused() {
      local().refuseWrites(REASON);

      // UPDATE_SECURITY guards read-only operations too (BACKUP DATABASE, LOAD CSV): the refusal must not block them.
      local().checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SECURITY);

      assertThatThrownBy(() -> local().checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA))
          .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    }

    @Test
    void readsAndReadOnlyTransactionsAreNotRefused() {
      local().refuseWrites(REASON);

      assertThat(database.query("sql", "SELECT FROM " + TYPE).hasNext()).isTrue();
      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(1L));
    }

    @Test
    void writesAreAcceptedOnceTheRefusalIsLifted() {
      local().refuseWrites(REASON);
      assertThat(local().getWriteRefusal()).isEqualTo(REASON);

      local().acceptWrites(REASON);
      assertThat(local().getWriteRefusal()).isNull();

      database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET name = 'accepted'"));
      database.getSchema().createDocumentType("Issue8270Accepted");
      assertThat(database.countType(TYPE, true)).isEqualTo(2L);
      assertThat(database.getSchema().existsType("Issue8270Accepted")).isTrue();
    }
  }

  /**
   * Issue #9837: {@code Record.delete()} and {@code Database.deleteRecord()} are {@code void}, so they throw away the one answer a
   * caller that COUNTS deletions needs: the engine's {@code false} for a delete a {@code BeforeRecordDeleteListener} vetoed.
   * {@link DatabaseInternal#deleteRecordReportingVeto(Record)} keeps it, under the same read lock {@code deleteRecord} takes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeleteRecordReportingVeto extends TestHelper {

    private RID create(final String name) {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("T").set("name", name).save().getIdentity());
      return rid[0];
    }

    private long count() {
      return database.countType("T", false);
    }

    @Test
    void answersTrueWhenTheRecordIsDeleted() {
      database.getSchema().createDocumentType("T");
      final RID rid = create("a");

      final boolean[] deleted = new boolean[1];
      database.transaction(() -> deleted[0] = ((DatabaseInternal) database).deleteRecordReportingVeto(database.lookupByRID(rid, true)));

      assertThat(deleted[0]).isTrue();
      assertThat(count()).isZero();
    }

    @Test
    void answersFalseAndKeepsTheRecordWhenAListenerVetoesTheDelete() {
      database.getSchema().createDocumentType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> false);
      final RID rid = create("a");

      final boolean[] deleted = {true};
      database.transaction(() -> deleted[0] = ((DatabaseInternal) database).deleteRecordReportingVeto(database.lookupByRID(rid, true)));

      assertThat(deleted[0]).isFalse();
      assertThat(count()).isEqualTo(1L);
    }

    @Test
    void aVetoOfOneRecordDoesNotStopTheNext() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordDeleteListener) record -> !"keep".equals(record.asDocument().getString("name")));
      final RID keep = create("keep");
      final RID drop = create("drop");

      final int[] deleted = new int[1];
      database.transaction(() -> {
        for (final RID rid : new RID[] { keep, drop })
          if (((DatabaseInternal) database).deleteRecordReportingVeto(database.lookupByRID(rid, true)))
            deleted[0]++;
      });

      assertThat(deleted[0]).isEqualTo(1);
      assertThat(count()).isEqualTo(1L);
    }

    @Test
    void anyOtherRefusalIsThrownAsDeleteRecordDoes() {
      database.getSchema().createDocumentType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
        throw new IllegalStateException("refused");
      });
      final RID rid = create("a");

      assertThatThrownBy(() -> database.transaction(
          () -> ((DatabaseInternal) database).deleteRecordReportingVeto(database.lookupByRID(rid, true)))).isInstanceOf(
          IllegalStateException.class).hasMessageContaining("refused");
      assertThat(count()).isEqualTo(1L);
    }
  }

  /**
   * Issue #9974: a write a before-event listener vetoed must not be counted as applied. The engine decides the answer, and the void
   * {@code Database.createRecord()} / {@code updateRecord()} and {@code MutableDocument.save()} threw it away, so every caller that
   * counts the records it wrote counted the refused ones too. {@link DatabaseInternal#createRecordReportingVeto},
   * {@link DatabaseInternal#updateRecordReportingVeto} and {@link MutableDocument#saveReportingVeto()} keep it, the SQL
   * {@code DELETE}/{@code UPDATE}/{@code INSERT} steps pass on only the rows the engine wrote, and the asynchronous create does not
   * report a success for a vetoed record.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WriteVetoReporting extends TestHelper {

    // --- the accessors ---

    @Test
    void createReportsTheVeto() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("name")));

      final boolean[] answers = new boolean[2];
      final MutableDocument refused = database.newDocument("T").set("name", "refused");
      database.transaction(() -> {
        answers[0] = ((DatabaseInternal) database).createRecordReportingVeto(database.newDocument("T").set("name", "kept"), null);
        answers[1] = refused.saveReportingVeto();
      });

      assertThat(answers[0]).isTrue();
      assertThat(answers[1]).isFalse();
      assertThat(refused.getIdentity()).isNull();
      assertThat(count("T")).isEqualTo(1L);
    }

    @Test
    void updateReportsTheVeto() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordUpdateListener) record -> !"refused".equals(record.asDocument().getString("name")));
      final RID rid = createDocuments("T", "a")[0];

      final boolean[] answers = new boolean[2];
      database.transaction(() -> answers[0] = rid.asDocument().modify().set("name", "b").saveReportingVeto());
      database.transaction(
          () -> answers[1] = ((DatabaseInternal) database).updateRecordReportingVeto(rid.asDocument().modify().set("name", "refused")));

      assertThat(answers[0]).isTrue();
      assertThat(answers[1]).isFalse();
      assertThat(rid.asDocument().getString("name")).isEqualTo("b");
    }

    @Test
    void anUpdateOfARecordThisTransactionDeletedIsNotApplied() {
      database.getSchema().createDocumentType("T");
      final RID rid = createDocuments("T", "a")[0];

      final boolean[] applied = { true };
      database.transaction(() -> {
        final MutableDocument doc = rid.asDocument().modify();
        database.deleteRecord(doc);
        applied[0] = doc.set("name", "b").saveReportingVeto();
      });

      assertThat(applied[0]).isFalse();
      assertThat(count("T")).isZero();
    }

    /** A lightweight edge has no record to write, so there is nothing a listener could refuse: its save is always applied. */
    @Test
    void aLightweightEdgeSaveIsAlwaysApplied() {
      database.getSchema().createVertexType("V");
      final EdgeType light = database.getSchema().createEdgeType("Light");
      light.getEvents().registerListener((BeforeRecordUpdateListener) record -> false);

      final boolean[] applied = new boolean[1];
      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("V").save();
        final MutableVertex v2 = database.newVertex("V").save();
        applied[0] = new MutableLightEdge(database, light, v1.getIdentity(), v2.getIdentity()).saveReportingVeto();
      });

      assertThat(applied[0]).isTrue();
    }

    @Test
    void anAsynchronousCreateVetoedDoesNotReportASuccess() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("name")));

      final AtomicInteger created = new AtomicInteger();
      database.async().createRecord(database.newDocument("T").set("name", "kept"), record -> created.incrementAndGet());
      database.async().createRecord(database.newDocument("T").set("name", "refused"), record -> created.incrementAndGet());
      database.async().waitCompletion();

      assertThat(created.get()).isEqualTo(1);
      assertThat(count("T")).isEqualTo(1L);
    }

    // --- SQL ---

    @Test
    void sqlDeleteCountsOnlyTheRecordsItDeleted() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordDeleteListener) record -> !"keep".equals(record.asDocument().getString("name")));
      createDocuments("T", "a", "keep", "b");

      assertThat(commandCount("DELETE FROM T")).isEqualTo(2L);
      assertThat(count("T")).isEqualTo(1L);
    }

    @Test
    void sqlDeleteReturnBeforeReturnsOnlyTheRecordsItDeleted() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordDeleteListener) record -> !"keep".equals(record.asDocument().getString("name")));
      createDocuments("T", "a", "keep", "b");

      final List<String> returned = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "DELETE FROM T RETURN BEFORE")) {
          while (rs.hasNext())
            returned.add(rs.next().getProperty("name"));
        }
      });

      assertThat(returned).containsExactlyInAnyOrder("a", "b");
    }

    /**
     * A whole fetch batch of vetoed rows must not end the statement: the steps after the delete stop at the first empty pull, so a
     * step that answered one would leave every later record undeleted.
     */
    @Test
    void sqlDeleteGoesOnPastAWholeBatchOfVetoedRows() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordDeleteListener) record -> record.asDocument().getInteger("i") >= 250);
      database.transaction(() -> {
        for (int i = 0; i < 500; i++)
          database.newDocument("T").set("i", i).save();
      });

      assertThat(commandCount("DELETE FROM T")).isEqualTo(250L);
      assertThat(count("T")).isEqualTo(250L);
    }

    /**
     * The delete step acts one row ahead of what it returns: a LIMIT must still bound the records deleted, and every record deleted
     * must be one the statement returns.
     */
    @Test
    void sqlDeleteWithALimitDeletesExactlyTheRowsItReturns() {
      database.getSchema().createDocumentType("T");
      createDocuments("T", "a", "b", "c", "d");

      final List<String> returned = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "DELETE FROM T RETURN BEFORE LIMIT 2")) {
          while (rs.hasNext())
            returned.add(rs.next().getProperty("name"));
        }
      });

      assertThat(returned).hasSize(2);
      assertThat(count("T")).isEqualTo(2L);
      assertThat(commandCount("DELETE FROM T LIMIT 1")).isEqualTo(1L);
      assertThat(count("T")).isEqualTo(1L);
    }

    /**
     * The write steps act one row ahead of what they return ({@code SkippingResultSet}), which is only safe because no step after
     * them can stop pulling early. Pinned on the plans themselves: in every DML plan a LIMIT comes before the write step.
     */
    @Test
    void noDmlPlanLimitsAfterItsWriteStep() {
      database.getSchema().createDocumentType("T");
      createDocuments("T", "a", "b", "c");

      for (final String sql : new String[] { "DELETE FROM T LIMIT 1", "DELETE FROM T RETURN BEFORE LIMIT 1",
          "UPDATE T SET x = 1 LIMIT 1", "UPDATE T SET x = 2 RETURN AFTER LIMIT 1", "UPDATE T SET x = 3 RETURN BEFORE LIMIT 1",
          "INSERT INTO T SET name = 'n'", "INSERT INTO T FROM SELECT name FROM T LIMIT 1" })
        database.transaction(() -> {
          try (final ResultSet rs = database.command("sql", sql)) {
            final List<ExecutionStep> steps = rs.getExecutionPlan().orElseThrow().getSteps();
            int write = -1;
            for (int i = 0; i < steps.size(); i++)
              if (steps.get(i) instanceof DeleteStep || steps.get(i) instanceof SaveElementStep)
                write = i;
            assertThat(write).as("write step of: %s", sql).isGreaterThanOrEqualTo(0);
            for (int i = write + 1; i < steps.size(); i++)
              assertThat(steps.get(i)).as("step after the write of: %s", sql).isNotInstanceOf(LimitExecutionStep.class);
          }
        });
    }

    @Test
    void sqlDeleteOfOnlyVetoedRowsCountsZero() {
      database.getSchema().createDocumentType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> false);
      createDocuments("T", "a", "b", "c");

      assertThat(commandCount("DELETE FROM T")).isZero();
      assertThat(count("T")).isEqualTo(3L);
    }

    @Test
    void sqlUpdateCountsOnlyTheRecordsItChanged() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordUpdateListener) record -> !"keep".equals(record.asDocument().getString("name")));
      createDocuments("T", "a", "keep", "b");

      assertThat(commandCount("UPDATE T SET touched = true")).isEqualTo(2L);
      assertThat(commandCount("SELECT count() AS count FROM T WHERE touched = true")).isEqualTo(2L);
    }

    @Test
    void sqlUpdateGoesOnPastAWholeBatchOfVetoedRows() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordUpdateListener) record -> record.asDocument().getInteger("i") >= 250);
      database.transaction(() -> {
        for (int i = 0; i < 500; i++)
          database.newDocument("T").set("i", i).save();
      });

      assertThat(commandCount("UPDATE T SET touched = true")).isEqualTo(250L);
      assertThat(commandCount("SELECT count() AS count FROM T WHERE touched = true")).isEqualTo(250L);
    }

    @Test
    void sqlInsertDoesNotReturnARecordItDidNotCreate() {
      database.getSchema().createDocumentType("T").getEvents()
          .registerListener((BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("name")));

      final List<Result> returned = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "INSERT INTO T CONTENT [{\"name\":\"a\"},{\"name\":\"refused\"},{\"name\":\"b\"}]")) {
          while (rs.hasNext())
            returned.add(rs.next());
        }
      });

      assertThat(returned).extracting(r -> r.<String>getProperty("name")).containsExactlyInAnyOrder("a", "b");
      assertThat(returned).allMatch(r -> r.getIdentity().isPresent());
      assertThat(count("T")).isEqualTo(2L);
    }

    // --- helpers ---

    private RID[] createDocuments(final String type, final String... names) {
      final RID[] rids = new RID[names.length];
      database.transaction(() -> {
        for (int i = 0; i < names.length; i++)
          rids[i] = database.newDocument(type).set("name", names[i]).save().getIdentity();
      });
      return rids;
    }

    private long commandCount(final String sql) {
      final long[] count = new long[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", sql)) {
          count[0] = ((Number) rs.next().getProperty("count")).longValue();
        }
      });
      return count[0];
    }

    private long count(final String type) {
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }
}
