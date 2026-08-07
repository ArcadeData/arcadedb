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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.schema.EdgeType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduction attempt for issue #661 / email #660:
 * "java.lang.IllegalArgumentException: Target vertex is not persistent. Call save() on vertex first"
 * thrown at GraphEngine.newEdge <- CreateStep.createEdge while executing the client's TRANSFER query:
 *
 * <pre>
 *   MATCH (src:Account {accountNumber: $debitAcct})
 *   MATCH (dst:Account {accountNumber: $creditAcct})
 *   WHERE NOT EXISTS { MATCH (src)-[t:TRANSFER {transactionId: $tranId}]->(dst) }
 *   CREATE (src)-[:TRANSFER {...}]->(dst)
 * </pre>
 *
 * Faithful to the reported environment: single node, 32 buckets on Account and TRANSFER, txRetries=3,
 * concurrent ingestion, each command wrapped in the client's own explicit transaction, and a
 * (possibly non-race-proof) get-or-create of Account by accountNumber running under contention.
 */
class CypherTargetVertexNotPersistentIssue661Test {
  private Database database;

  private static final String TRANSFER_QUERY = """
      MATCH (src:Account {accountNumber: $debitAcct})
      MATCH (dst:Account {accountNumber: $creditAcct})
      WHERE NOT EXISTS {
          MATCH (src)-[t:TRANSFER {transactionId: $tranId}]->(dst)
      }
      CREATE (src)-[:TRANSFER {
          transactionId : $tranId,
          amountPaid    : $amount,
          isLaundering  : false
      }]->(dst)
      """;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-target-not-persistent-661");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      final Schema schema = database.getSchema();
      // 32 buckets each, as confirmed by the client (issue #665/#667). No UNIQUE index on accountNumber -
      // the client has not confirmed one, matching Luca's "duplicate account" hypothesis (issue #674).
      final VertexType account = schema.createVertexType("Account", 32);
      account.createProperty("accountNumber", String.class);
      final EdgeType transfer = schema.createEdgeType("TRANSFER", 32);
      transfer.createProperty("transactionId", String.class);
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * Models the client pipeline under concurrency: each worker, inside its own explicit transaction,
   * ensures both accounts exist (non-atomic get-or-create, like getOrGenAccount) and then runs the
   * exact TRANSFER command.
   * <p>
   * Key to reproducing "Target vertex is not persistent": the dst account is created FRESH inside the
   * same outer transaction as the TRANSFER (so it is an unsaved-on-rollback MutableVertex), while the
   * src is a shared hot hub account whose OUT edge-list collides under concurrency, raising a
   * ConcurrentModificationException. CreateStep runs the CREATE in a nested {@code transaction(block, true)}
   * that RETRIES on ConcurrentModificationException; because it joined the caller's transaction, that
   * retry rolls back the WHOLE outer transaction - resetting the freshly-created dst's RID to null - and
   * then re-runs the CREATE with the now-stale dst binding.
   */
  @Test
  void concurrentTransfersDoNotThrowTargetNotPersistent() throws InterruptedException {
    final int threads = 8;
    final int iterations = 80;
    final int hubCount = 2; // few shared src hubs => heavy edge-list contention on src OUT head

    // Pre-create and COMMIT the hub (src) accounts so they are persistent immutable vertices.
    database.transaction(() -> {
      for (int h = 0; h < hubCount; h++)
        database.command("cypher", "CREATE (a:Account {accountNumber: $n})", Map.of("n", "HUB-" + h));
    });

    final AtomicLong success = new AtomicLong();
    final AtomicLong retriesExhausted = new AtomicLong();
    final ConcurrentLinkedQueue<Throwable> notPersistent = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Throwable> otherErrors = new ConcurrentLinkedQueue<>();

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        for (int i = 0; i < iterations; i++) {
          final String debit = "HUB-" + (i % hubCount);           // shared hot src => edge-list collisions
          final String credit = "DST-" + threadId + "-" + i;      // brand-new dst, unique per transfer
          final String tranId = "TX-" + threadId + "-" + i;
          try {
            database.transaction(() -> {
              ensureAccount(debit);
              ensureAccount(credit); // creates dst FRESH inside this same outer transaction
              database.command("cypher", TRANSFER_QUERY,
                  Map.of("debitAcct", debit, "creditAcct", credit, "tranId", tranId, "amount", 100.0));
            });
            success.incrementAndGet();
          } catch (final Throwable e) {
            if (messageChainContains(e, "Target vertex is not persistent"))
              notPersistent.add(e);
            else if (messageChainContains(e, "concurrent") || e.getClass().getSimpleName().contains("NeedRetry")
                || e.getClass().getSimpleName().contains("ConcurrentModification"))
              retriesExhausted.incrementAndGet();
            else
              otherErrors.add(e);
          }
        }
        done.countDown();
      }, "worker-" + t).start();
    }

    start.countDown();
    done.await();

    // The engine must never hand an unsaved vertex to edge creation. Transient MVCC conflicts
    // (retriesExhausted / otherErrors) are an acceptable concurrency outcome the caller replays;
    // "Target vertex is not persistent" is data corruption and must never occur (issue #661).
    assertThat(notPersistent)
        .as("Target vertex is not persistent must never be thrown; "
            + "success=%d retriesExhausted(CME)=%d otherErrors=%d rootCause=%s",
            success.get(), retriesExhausted.get(), otherErrors.size(), rootCauseMessage(notPersistent.peek()))
        .isEmpty();
    // Sanity: at least some transfers must have committed, otherwise the scenario exercised nothing.
    assertThat(success.get()).isPositive();
  }

  private static String rootCauseMessage(Throwable e) {
    if (e == null)
      return "n/a";
    Throwable root = e;
    while (root.getCause() != null)
      root = root.getCause();
    return root.toString();
  }

  /** Non-atomic get-or-create of an Account by accountNumber, mirroring a non-race-proof getOrGenAccount. */
  private void ensureAccount(final String accountNumber) {
    try (final var rs = database.query("cypher", "MATCH (a:Account {accountNumber: $n}) RETURN a",
        Map.of("n", accountNumber))) {
      if (rs.hasNext())
        return;
    }
    database.command("cypher", "CREATE (a:Account {accountNumber: $n})", Map.of("n", accountNumber));
  }

  private static boolean messageChainContains(Throwable e, final String needle) {
    for (Throwable c = e; c != null; c = c.getCause()) {
      final String m = c.getMessage();
      if (m != null && m.contains(needle))
        return true;
    }
    return false;
  }
}
