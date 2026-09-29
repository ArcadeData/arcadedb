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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8610: under READ_COMMITTED, reading a record, letting a concurrent transaction commit a change to it,
 * then writing a value derived from the read through {@code modify()} lost the concurrent change silently, because
 * {@code modify()} reloads a vertex whose page moved on and the stale read disappeared with the reload. A document in
 * the same situation was already refused by the #6950 record-image check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue8610StaleReadModifyTest {
  private Database database;
  private RID      rid;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-8610-stale-read");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sqlscript", "CREATE VERTEX TYPE V; CREATE EDGE TYPE E; CREATE DOCUMENT TYPE D");
    final AtomicReference<RID> created = new AtomicReference<>();
    database.transaction(() -> created.set(database.newVertex("V").set("n", 0).save().getIdentity()));
    rid = created.get();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen()) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  /**
   * The issue: a read-modify-write on a vertex read in this transaction, over a concurrent commit, must be refused as a
   * retryable conflict, never commit a value computed from the stale read.
   */
  @Test
  void readModifyWriteOverAConcurrentCommitIsRefused() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    final int n = read.getInteger("n");
    commitConcurrently("n", 5);

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(readN()).as("the concurrent write survives").isEqualTo(5);
  }

  /**
   * The same for a vertex read through a scan.
   */
  @Test
  void readModifyWriteOfAScannedVertexOverAConcurrentCommitIsRefused() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Iterator<Record> it = database.iterateType("V", false);
    final Vertex read = it.next().asVertex();
    final int n = read.getInteger("n");
    commitConcurrently("n", 5);

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(readN()).isEqualTo(5);
  }

  /**
   * Retrying the transaction, as the retry helper does, reads the vertex again and succeeds: the refusal is retryable.
   */
  @Test
  void theRetryReadsAgainAndCommits() {
    final AtomicReference<Boolean> first = new AtomicReference<>(true);
    database.transaction(() -> {
      final Vertex read = rid.asVertex();
      final int n = read.getInteger("n");
      if (first.getAndSet(false))
        commitConcurrently("n", 5);
      read.modify().set("n", n + 1).save();
    }, false, 3);

    assertThat(readN()).as("the retry built on the concurrent 5").isEqualTo(6);
  }

  /**
   * A document read through a scan: its content is reloaded on modify() when its page moved on (issue #8312), which
   * hid the stale read from the #6950 record-image check the same way.
   */
  @Test
  void readModifyWriteOfAScannedDocumentOverAConcurrentCommitIsRefused() {
    final AtomicReference<RID> doc = new AtomicReference<>();
    database.transaction(() -> doc.set(database.newDocument("D").set("n", 0).save().getIdentity()));

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Document read = database.iterateType("D", false).next().asDocument();
    final int n = read.getInteger("n");
    commitConcurrently(() -> doc.get().asDocument().modify().set("n", 5).save());

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(doc.get().asDocument().getInteger("n")).isEqualTo(5);
  }

  /**
   * The same for an edge read through a scan.
   */
  @Test
  void readModifyWriteOfAScannedEdgeOverAConcurrentCommitIsRefused() {
    final AtomicReference<RID> edge = new AtomicReference<>();
    database.transaction(() -> {
      final MutableVertex other = database.newVertex("V").save();
      edge.set(rid.asVertex().newEdge("E", other, "n", 0).getIdentity());
    });

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Edge read = database.iterateType("E", false).next().asEdge();
    final int n = read.getInteger("n");
    commitConcurrently(() -> edge.get().asEdge().modify().set("n", 5).save());

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(edge.get().asEdge().getInteger("n")).isEqualTo(5);
  }

  /**
   * Creating an edge changes only the vertex edge lists, which is what the reload exists for: a concurrent change to the
   * vertex properties must not turn it into a conflict, nor be lost.
   */
  @Test
  void edgeCreationOverAConcurrentPropertyChangeCommits() {
    final AtomicReference<RID> other = new AtomicReference<>();
    database.transaction(() -> other.set(database.newVertex("V").set("n", 100).save().getIdentity()));

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    commitConcurrently("n", 5);
    read.newEdge("E", other.get().asVertex());
    database.commit();

    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
  }

  /**
   * A vertex read outside this transaction (held across transactions) keeps the refresh it always had: its content is
   * reloaded by modify() and the write commits. Only a read made inside the transaction is checked.
   */
  @Test
  void aVertexReadBeforeTheTransactionIsStillRefreshed() {
    final Vertex held = rid.asVertex();
    database.transaction(() -> held.modify().set("a", 1).save());
    database.transaction(() -> held.modify().set("b", 2).save());

    final Vertex reloaded = rid.asVertex();
    assertThat(reloaded.getInteger("a")).isEqualTo(1);
    assertThat(reloaded.getInteger("b")).isEqualTo(2);
  }

  /**
   * Nothing changed concurrently: no conflict, however the vertex was read.
   */
  @Test
  void aCurrentReadCommits() {
    database.transaction(() -> {
      final Vertex read = rid.asVertex();
      read.modify().set("n", read.getInteger("n") + 1).save();
    });
    assertThat(readN()).isEqualTo(1);
  }

  /**
   * The switch restores the previous behavior (the reload wins silently), for an application that relies on it.
   */
  @Test
  void theCheckCanBeDisabled() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_STALE_READ_CHECK, false);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    final int n = read.getInteger("n");
    commitConcurrently("n", 5);
    read.modify().set("n", n + 1).save();
    database.commit();

    assertThat(readN()).as("pre-#8610 behavior: the value computed from the stale read overwrites the concurrent 5")
        .isEqualTo(1);
  }

  private void commitConcurrently(final String property, final int value) {
    commitConcurrently(() -> rid.asVertex().modify().set(property, value).save());
  }

  private void commitConcurrently(final Runnable write) {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread concurrent = new Thread(() -> {
      try {
        database.transaction(write::run);
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    concurrent.start();
    try {
      concurrent.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(failure.get()).isNull();
  }

  private int readN() {
    return rid.asVertex().getInteger("n");
  }
}
