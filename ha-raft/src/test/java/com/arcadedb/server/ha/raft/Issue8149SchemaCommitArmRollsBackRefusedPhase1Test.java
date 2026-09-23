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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

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
 * Issue #8149: the schema-commit arm of {@link RaftReplicatedDatabase#commit()} - the one a DDL callback inside
 * {@code recordFileChanges} takes on the leader - must leave nothing open behind when phase 1 refuses the
 * transaction, the way the ordinary arm always has. It used to only pop the context in a {@code finally}, which
 * pops nothing for a single-level transaction, so a refused DDL commit left its transaction ACTIVE on the thread,
 * with the database write lock released on the way out.
 * <p>
 * Driven against a REAL {@link LocalDatabase} and a real {@link TransactionContext}, because what is under test is
 * what the transaction stack looks like afterwards. The arm is selected by setting the thread-local
 * {@code recordFileChanges} sets around a DDL callback (the same technique as the #8053 test); the Raft server is
 * the only mock.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8149SchemaCommitArmRollsBackRefusedPhase1Test {
  private static final String TYPE        = "Doc8149";
  private static final String UNIQUE_TYPE = "Uniq8149";
  private static final String REASON      = "record #1:0 could not be taken back after its indexing refused it";

  @TempDir
  Path tempDir;

  private LocalDatabase                        proxied;
  private RaftTransactionBroker                broker;
  private RaftReplicatedDatabase               database;
  private ThreadLocal<Boolean>                 schemaCommitThread;
  private ThreadLocal<List<byte[]>>            schemaWalBuffer;
  private ThreadLocal<List<Map<Integer, Integer>>> schemaBucketDeltaBuffer;

  @BeforeEach
  void setUp() throws Exception {
    // Every piece of schema and every property name is put in place BEFORE the wrapper is installed: afterwards a
    // DDL statement, or the dictionary's own nested commit of a name it has not seen, would take the Raft path.
    proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue8149").toString()).create();
    proxied.getSchema().createDocumentType(TYPE, 1).createProperty("name", Type.STRING);
    proxied.getSchema().createDocumentType(UNIQUE_TYPE, 1).createProperty("key", Type.STRING);
    proxied.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, UNIQUE_TYPE, "key");

    proxied.transaction(() -> proxied.newDocument(TYPE).set("name", "warm-up").save());
    proxied.transaction(() -> proxied.newDocument(UNIQUE_TYPE).set("key", "taken").save());
    proxied.transaction(() -> proxied.command("sql", "DELETE FROM " + TYPE));

    broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raftServer = mock(RaftHAServer.class, RETURNS_DEEP_STUBS);
    when(raftServer.isLeader()).thenReturn(true);
    when(raftServer.getTransactionBroker()).thenReturn(broker);

    database = new RaftReplicatedDatabase(null, proxied, raftServer);

    schemaCommitThread = threadLocal("isSchemaCommitThread");
    schemaWalBuffer = threadLocal("schemaWalBuffer");
    schemaBucketDeltaBuffer = threadLocal("schemaBucketDeltaBuffer");
  }

  @AfterEach
  void tearDown() {
    schemaCommitThread.remove();
    schemaWalBuffer.remove();
    schemaBucketDeltaBuffer.remove();
    if (proxied != null && proxied.isOpen()) {
      proxied.rollbackAllNested();
      proxied.drop();
    }
  }

  /**
   * The refusal the issue was observed with: the rollback-only marker, checked before phase 1 does any work, so
   * phase 1 itself has rolled nothing back. The arm must.
   */
  @Test
  void aRefusedSchemaCommitIsRolledBack() {
    database.begin();
    proxied.newDocument(TYPE).set("name", "in-flight").save();
    proxied.getTransaction().setRollbackOnly(REASON);

    schemaCommitThread.set(Boolean.TRUE);
    assertThatThrownBy(database::commit)
        .isInstanceOf(TransactionException.class)
        .hasMessageContaining("could not be taken back");

    assertThat(proxied.isTransactionActive())
        .as("the schema-commit arm must roll back what phase 1 refused, like the ordinary arm (#8149)")
        .isFalse();
    assertThat(schemaWalBuffer.get()).as("nothing refused may be buffered for the SCHEMA_ENTRY").isEmpty();
    assertThat(schemaBucketDeltaBuffer.get()).isEmpty();
    verify(broker, never()).replicateTransaction(anyString(), any(), any());

    schemaCommitThread.remove();
    assertThat(proxied.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();
  }

  /**
   * A refusal raised from INSIDE phase 1's work (a unique-key violation found while replaying the index queue):
   * the exception type is preserved, and the thread is left with no transaction, open or half-concluded.
   */
  @Test
  void aSchemaCommitRefusedInsidePhase1LeavesNoTransactionBehind() {
    database.begin();
    proxied.newDocument(UNIQUE_TYPE).set("key", "taken").save();

    schemaCommitThread.set(Boolean.TRUE);
    assertThatThrownBy(database::commit).isInstanceOf(DuplicatedKeyException.class);

    assertThat(proxied.isTransactionActive()).isFalse();
    assertThat(schemaWalBuffer.get()).isEmpty();

    schemaCommitThread.remove();
    assertThat(proxied.countType(UNIQUE_TYPE, false)).as("only the record committed before the test").isEqualTo(1);
  }

  /**
   * A refused NESTED transaction: exactly the inner one goes. The rollback must not pop the context itself on top
   * of the arm's own {@code popIfNotLastTransaction()}, or the enclosing transaction would be discarded with it.
   */
  @Test
  void aRefusedNestedSchemaCommitRollsBackOnlyTheInnerTransaction() {
    database.begin();
    proxied.newDocument(TYPE).set("name", "outer").save();
    final TransactionContext outer = proxied.getTransaction();

    database.begin();
    assertThat(proxied.getNestedTransactions()).isEqualTo(2);
    proxied.newDocument(TYPE).set("name", "inner").save();
    proxied.getTransaction().setRollbackOnly(REASON);

    schemaCommitThread.set(Boolean.TRUE);
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class);

    assertThat(proxied.getNestedTransactions()).as("only the refused inner transaction is removed").isEqualTo(1);
    assertThat(proxied.getTransaction()).isSameAs(outer);
    assertThat(outer.isActive()).as("the enclosing transaction is untouched").isTrue();

    schemaCommitThread.remove();
    proxied.rollback();
    assertThat(proxied.countType(TYPE, false)).isZero();
  }

  /**
   * The ordinary arm, same nested shape but refused from inside phase 1: phase 1 has already rolled the inner
   * transaction back, and the arm's rollback still has to take it off the stack, or the enclosing transaction is
   * hidden behind an inactive one.
   */
  @Test
  void aNestedCommitRefusedInsidePhase1OnTheOrdinaryArmRemovesOnlyTheInnerTransaction() {
    database.begin();
    proxied.newDocument(TYPE).set("name", "outer").save();
    final TransactionContext outer = proxied.getTransaction();

    database.begin();
    proxied.newDocument(UNIQUE_TYPE).set("key", "taken").save();

    assertThatThrownBy(database::commit).isInstanceOf(DuplicatedKeyException.class);
    verify(broker, never()).replicateTransaction(anyString(), any(), any());

    assertThat(proxied.getNestedTransactions()).as("only the refused inner transaction is removed").isEqualTo(1);
    assertThat(proxied.getTransaction()).isSameAs(outer);
    assertThat(outer.isActive()).isTrue();

    proxied.rollback();
  }

  @SuppressWarnings("unchecked")
  private static <T> ThreadLocal<T> threadLocal(final String name) throws Exception {
    final Field field = RaftReplicatedDatabase.class.getDeclaredField(name);
    field.setAccessible(true);
    return (ThreadLocal<T>) field.get(null);
  }
}
