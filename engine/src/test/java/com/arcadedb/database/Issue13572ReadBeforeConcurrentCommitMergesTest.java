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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13572: under READ_COMMITTED, a transaction that inserts a record and then reads the page it wrote (a scan of the type in the
 * same statement, {@code CREATE (n:Sku) WITH n MATCH (x:Sku) RETURN count(x)}) was refused at commit whenever another transaction had
 * committed an insert into the same page afterwards. The read was served from the transaction's own image of the page, and #11590
 * held every such read against the slot merge. But nobody had committed to the page when the read happened: the image was the
 * committed page plus the transaction's own write, so the read was a valid READ_COMMITTED read and a commit landing after it is an
 * ordinary phantom. Only a read of an image that was already stale when it was made must refuse the merge; Issue11590ReadOwnStalePageTest
 * keeps that side.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13572ReadBeforeConcurrentCommitMergesTest extends TestHelper {
  private static final int THREADS = 4;
  private static final int ROUNDS  = 10;

  @Test
  void scanOfTheWrittenPageBeforeAConcurrentInsertStillMerges() throws Exception {
    createSkuType();

    database.begin();
    final RID own = database.newVertex("Sku").set("id", 1).save().getIdentity();
    assertThat(scanCountOfSku()).isEqualTo(2L);
    final RID other = commitFromAnotherThread("CREATE (n:Sku {id: 2}) RETURN n");
    assertThat(pageOf(other)).as("the concurrent insert lands on the page the transaction wrote and scanned").isEqualTo(pageOf(own));
    database.commit();

    assertThat(idsOfSku()).containsExactly(0, 1, 2);
  }

  @Test
  void readOfARecordBeforeAConcurrentUpdateOfItStillMerges() throws Exception {
    final RID seed = createSkuType();

    database.begin();
    final RID own = database.newVertex("Sku").set("id", 1).save().getIdentity();
    assertThat(pageOf(own)).isEqualTo(pageOf(seed));
    assertThat(seed.asVertex(true).getInteger("id")).isZero();
    commitFromAnotherThread("MATCH (n:Sku {id: 0}) SET n.id = 10 RETURN n");
    database.commit();

    assertThat(idsOfSku()).containsExactly(1, 10);
  }

  /** The order of #11590 still refuses: a scan made AFTER the concurrent insert committed misses it, so the merge must not run. */
  @Test
  void scanOfTheWrittenPageAfterAConcurrentInsertIsStillRefused() throws Exception {
    createSkuType();

    database.begin();
    final RID own = database.newVertex("Sku").set("id", 1).save().getIdentity();
    final RID other = commitFromAnotherThread("CREATE (n:Sku {id: 2}) RETURN n");
    assertThat(pageOf(other)).isEqualTo(pageOf(own));
    assertThat(scanCountOfSku()).as("the scan of the transaction's own image misses the concurrent insert").isEqualTo(2L);

    Throwable failure = null;
    try {
      database.commit();
    } catch (final ConcurrentModificationException e) {
      failure = e;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
    assertThat(failure).as("a commit must not rest on a scan that missed a record committed before it")
        .isInstanceOf(ConcurrentModificationException.class);
    assertThat(idsOfSku()).containsExactly(0, 2);
  }

  /** Same as above for a record read by RID after a concurrent commit changed it: the stale read still refuses the merge. */
  @Test
  void readOfARecordAfterAConcurrentUpdateOfItIsStillRefused() throws Exception {
    final RID seed = createSkuType();

    database.begin();
    final RID own = database.newVertex("Sku").set("id", 1).save().getIdentity();
    assertThat(pageOf(own)).isEqualTo(pageOf(seed));
    commitFromAnotherThread("MATCH (n:Sku {id: 0}) SET n.id = 10 RETURN n");
    final int read = seed.asVertex(true).getInteger("id");

    Throwable failure = null;
    try {
      database.commit();
    } catch (final ConcurrentModificationException e) {
      failure = e;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
    if (failure == null)
      assertThat(read).as("a transaction that commits must have read the value committed before its read").isEqualTo(10);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
  }

  /** The reproduction of the issue: every transaction writes and reads before any of them commits, and all of them commit. */
  @Test
  void concurrentCreatesThatReadTheirLabelInTheSameStatementAllCommit() throws Exception {
    runConcurrentRounds("CREATE (n:Sku {id: $id}) WITH n MATCH (x:Sku) RETURN count(x) AS c");
  }

  /** The Spring Data shape the issue reduces from. */
  @Test
  void concurrentSpringDataSavesAllCommit() throws Exception {
    runConcurrentRounds("OPTIONAL MATCH (hlp:Sku) WHERE id(hlp) = $rid WITH hlp WHERE hlp IS NULL "
        + "CREATE (sku:Sku) SET sku.id = $id RETURN sku "
        + "UNION MATCH (sku:Sku) WHERE id(sku) = $rid SET sku.id = $id RETURN sku");
  }

  private void runConcurrentRounds(final String statement) throws Exception {
    createSkuType();

    final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
    for (int round = 0; round < ROUNDS; round++) {
      final CyclicBarrier written = new CyclicBarrier(THREADS);
      final Thread[] threads = new Thread[THREADS];
      final int base = round * THREADS;
      for (int t = 0; t < THREADS; t++) {
        final int id = base + t + 1;
        threads[t] = new Thread(() -> {
          try {
            database.begin();
            try {
              database.command("opencypher", statement, Map.of("id", id, "rid", "#999:999")).close();
              written.await(30, TimeUnit.SECONDS);
              database.commit();
            } finally {
              if (database.isTransactionActive())
                database.rollback();
            }
          } catch (final Throwable e) {
            failures.add(e);
          }
        });
        threads[t].start();
      }
      for (final Thread thread : threads)
        thread.join();
    }

    assertThat(failures).as("concurrent creates of unrelated nodes must all commit").isEmpty();
    final List<Object> ids = idsOfSku();
    assertThat(ids).hasSize(THREADS * ROUNDS + 1);
    assertThat(new HashSet<>(ids)).hasSize(THREADS * ROUNDS + 1);
  }

  /** Type Sku in one bucket, with one committed record (id 0) so its page 0 exists and every transaction can insert into it. */
  private RID createSkuType() {
    final RID[] seed = new RID[1];
    database.transaction(() -> {
      database.getSchema().createVertexType("Sku", 1).createProperty("id", Type.INTEGER);
      seed[0] = database.newVertex("Sku").set("id", 0).save().getIdentity();
    });
    return seed[0];
  }

  private long scanCountOfSku() {
    long counted = 0;
    for (final Iterator<Record> it = database.iterateType("Sku", false); it.hasNext(); it.next())
      counted++;
    return counted;
  }

  private long pageOf(final RID rid) {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    return rid.getPosition() / bucket.getMaxRecordsInPage();
  }

  private List<Object> idsOfSku() {
    final List<Object> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM Sku ORDER BY id")) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  /**
   * Runs and commits the openCypher {@code command} in a transaction of another thread, so it is committed before the caller goes on;
   * returns the RID of the node it returns.
   */
  private RID commitFromAnotherThread(final String command) throws InterruptedException {
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final AtomicReference<RID> returned = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        database.transaction(() -> {
          try (final ResultSet rs = database.command("opencypher", command)) {
            returned.set(rs.next().getIdentity().orElseThrow());
          }
        });
      } catch (final Throwable e) {
        error.set(e);
      }
    });
    thread.start();
    thread.join();
    assertThat(error.get()).isNull();
    return returned.get();
  }
}
