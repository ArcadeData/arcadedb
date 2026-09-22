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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #8053: the rollback-only marker of issue #7467 must stop the REPLICATED commit too.
 * <p>
 * {@link RaftReplicatedDatabase#commit()} never calls {@code TransactionContext.commit()} - it drives
 * {@code commit1stPhase}/{@code commit2ndPhase} itself so it can put the WAL bytes on the wire between the two
 * phases - and the marker's only read used to be at the top of {@code commit()}. A transaction that could not
 * take back a half-written record was therefore refused on a standalone server and published on an HA leader,
 * where it does not merely land locally but ships as a {@code TX_ENTRY} and is applied on every follower.
 * <p>
 * Both of the wrapper's phase-1 call sites are driven here against a REAL {@link LocalDatabase} and a real
 * {@link TransactionContext}, because the guard lives in the transaction and a mocked one cannot refuse
 * anything. The Raft server is the only mock: what matters is that the broker is never asked to replicate.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8053RollbackOnlyRefusedOnTheReplicatedCommitPathTest {
  private static final String TYPE   = "Doc8053";
  private static final String REASON = "record #1:0 could not be taken back after its indexing refused it";

  @TempDir
  Path tempDir;

  private LocalDatabase          proxied;
  private RaftTransactionBroker  broker;
  private RaftReplicatedDatabase database;

  @BeforeEach
  void setUp() {
    // The type is created BEFORE the wrapper is installed: a DDL statement issued afterwards would route
    // through the Raft schema path, which needs a real cluster and is not what is under test here.
    proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue8053").toString()).create();
    proxied.getSchema().createDocumentType(TYPE, 1).createProperty("name", Type.STRING);

    // Same reason, one level down: the FIRST serialization of a property name the dictionary has not seen
    // opens its own nested transaction (Dictionary.getIdByName) and commits it, which after the wrapper is
    // installed would be a Raft commit of the dictionary page rather than of the record under test. Warm the
    // name in, then take the record back out so the counts below start from zero.
    final MutableDocument[] warmUp = new MutableDocument[1];
    proxied.transaction(() -> warmUp[0] = proxied.newDocument(TYPE).set("name", "warm-up").save());
    proxied.transaction(() -> warmUp[0].delete());

    broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raftServer = mock(RaftHAServer.class, RETURNS_DEEP_STUBS);
    when(raftServer.isLeader()).thenReturn(true);
    when(raftServer.getTransactionBroker()).thenReturn(broker);

    // The constructor points the database's wrapped instance at the replicated wrapper, which is what makes
    // this the commit path a write on a replicated database really takes.
    database = new RaftReplicatedDatabase(null, proxied, raftServer);
  }

  @AfterEach
  void tearDown() {
    if (proxied != null && proxied.isOpen()) {
      if (proxied.isTransactionActive())
        proxied.rollback();
      proxied.drop();
    }
  }

  /**
   * The ordinary phase-1 arm - every user write on a replicated database. The commit must be refused and the
   * broker must never be asked to replicate anything, because the WAL bytes are produced by the very call that
   * now refuses.
   */
  @Test
  void theReplicatedCommitRefusesARollbackOnlyTransaction() {
    database.begin();
    proxied.newDocument(TYPE).set("name", "in-flight").save();
    proxied.getTransaction().setRollbackOnly(REASON);

    assertThatThrownBy(database::commit)
        .isInstanceOf(TransactionException.class)
        .hasMessageContaining("could not be taken back")
        .hasMessageContaining("Roll it back");

    verify(broker, never()).replicateTransaction(anyString(), any(), any());
    assertThat(proxied.isTransactionActive())
        .as("the wrapper's own phase-1 error handling rolled it back")
        .isFalse();
    assertThat(proxied.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();
  }

  /**
   * The schema-commit arm, the second of the wrapper's two direct phase-1 calls: it buffers the WAL bytes for
   * the {@code SCHEMA_ENTRY} instead of shipping a {@code TX_ENTRY}, and used to buffer them for a transaction
   * that could not be published either.
   * <p>
   * Reached by setting the thread-local the arm keys on, which is what {@code recordFileChanges} sets around a
   * DDL callback. Reflection rather than a real DDL statement: the alternative needs a live Raft group, and
   * what is under test is which branch of {@code commit()} runs, not how the branch is selected.
   */
  @Test
  void theSchemaCommitArmRefusesItToo() throws Exception {
    final ThreadLocal<Boolean> schemaCommitThread = threadLocal("isSchemaCommitThread");
    final ThreadLocal<List<byte[]>> schemaWalBuffer = threadLocal("schemaWalBuffer");

    database.begin();
    proxied.newDocument(TYPE).set("name", "in-flight").save();
    proxied.getTransaction().setRollbackOnly(REASON);

    schemaCommitThread.set(Boolean.TRUE);
    try {
      assertThatThrownBy(database::commit)
          .isInstanceOf(TransactionException.class)
          .hasMessageContaining("could not be taken back");

      assertThat(schemaWalBuffer.get())
          .as("no WAL bytes of an unpublishable transaction may be buffered for the SCHEMA_ENTRY")
          .isEmpty();

      // Since #8149 this arm rolls back what phase 1 refused, the way the ordinary arm above always has.
      assertThat(proxied.isTransactionActive())
          .as("the schema-commit arm rolls back what phase 1 refused (#8149)")
          .isFalse();
    } finally {
      schemaCommitThread.remove();
      schemaWalBuffer.remove();
      if (proxied.isTransactionActive())
        proxied.rollback();
    }

    assertThat(proxied.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();
  }

  @SuppressWarnings("unchecked")
  private static <T> ThreadLocal<T> threadLocal(final String name) throws Exception {
    final Field field = RaftReplicatedDatabase.class.getDeclaredField(name);
    field.setAccessible(true);
    return (ThreadLocal<T>) field.get(null);
  }
}
