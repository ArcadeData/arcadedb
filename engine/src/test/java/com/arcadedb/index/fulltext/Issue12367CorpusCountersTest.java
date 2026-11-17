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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12367: the BM25 corpus counters of the leader of a cluster counted 40% more documents than the type held, while the
 * replicas counted exactly. The replicas load the index from the schema and check its counters against the live document count at
 * their first search, repairing a drift; the node that CREATED the index had that check consumed by the creation itself, so a drift
 * of its counters was never repaired. The creation now leaves the check armed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12367CorpusCountersTest extends TestHelper {
  private static final String INDEX = "Doc[content]";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.content STRING");
    database.command("sql", "CREATE PROPERTY Doc.k INTEGER");
    database.command("sql", "CREATE INDEX ON Doc (k) UNIQUE_HASH NULL_STRATEGY SKIP");
    database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(INDEX));
  }

  private long live() {
    return database.countType("Doc", true);
  }

  private void search() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('" + INDEX + "', 'alpha') = true")) {
      rs.stream().count();
    }
  }

  @Test
  void theNodeThatCreatedTheIndexChecksItsCountersAtItsFirstSearch() {
    assertThat(metadata().isStaleCheckDone()).as("the creation leaves the check armed").isFalse();

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newVertex("Doc").set("k", i).set("content", "alpha beta " + i).save();
    });
    assertThat(metadata().getTotalDocs()).isEqualTo(10L);

    // a drift of the counters, by whatever cause: before the fix the creating node kept it for the rest of its session
    metadata().applyCorpusDelta(7L, 70L);
    assertThat(metadata().getTotalDocs()).isEqualTo(17L);

    search();
    assertThat(metadata().getTotalDocs()).as("the first search repairs the drift").isEqualTo(10L);
    assertThat(metadata().isStaleCheckDone()).isTrue();
  }

  /** Every write shape moves the counters by exactly the documents it adds or removes. */
  @Test
  void everyWriteShapeKeepsTheCountersOnTheLiveCount() throws Exception {
    final List<String> drifts = new ArrayList<>();

    database.transaction(() -> {
      final MutableVertex d = database.newVertex("Doc").set("content", "alpha beta gamma").save();
      d.set("other", 1).save();
    });
    check("insert, then update of another property in the same transaction", drifts);

    database.transaction(() -> {
      final MutableVertex d = database.newVertex("Doc").set("content", "alpha beta gamma").save();
      d.set("content", "delta epsilon").save();
    });
    check("insert, then update of the text in the same transaction", drifts);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET k = 3, content = 'one two'");
      database.command("sql", "UPDATE Doc SET emb = [1,2,3] WHERE k = 3");
    });
    check("SQL insert and update in the same transaction", drifts);

    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET k = 4, content = 'three four'"));
    database.transaction(() -> database.command("sql", "UPDATE Doc SET emb = [1,2,3] WHERE k = 4"));
    database.transaction(() -> database.command("sql", "UPDATE Doc SET content = 'five six' WHERE k = 4"));
    check("insert and updates in separate transactions", drifts);

    database.transaction(() -> database.command("sql", "UPDATE Doc SET content = 'up sert', k = 5 UPSERT WHERE k = 5"));
    database.transaction(() -> database.command("sql", "UPDATE Doc SET content = 'up sert again' UPSERT WHERE k = 5"));
    check("UPSERT twice", drifts);

    final String merge = "MERGE (n:Doc {k: 6}) ON CREATE SET n.content = 'merge one' ON MATCH SET n.content = 'merge two'";
    database.transaction(() -> database.command("opencypher", merge));
    database.transaction(() -> database.command("opencypher", merge));
    check("openCypher MERGE twice", drifts);

    database.transaction(() -> {
      final MutableVertex d = database.newVertex("Doc").set("k", 7).save();
      d.set("content", "late text").save();
    });
    check("insert without the text, then set the text in the same transaction", drifts);

    database.transaction(() -> database.command("opencypher", "CREATE (n:Doc {k: 8, content: 'cypher create'}) SET n.emb = [1,2]"));
    check("openCypher CREATE then SET", drifts);

    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE k = 8"));
    check("delete", drifts);

    // concurrent writers whose conflicts are retried
    final AtomicInteger failures = new AtomicInteger();
    final Thread[] threads = new Thread[6];
    for (int t = 0; t < threads.length; t++) {
      final int thread = t;
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 200; i++) {
          final int n = i;
          try {
            database.transaction(() -> {
              final MutableVertex d = database.newVertex("Doc").set("content", "thread " + thread + " doc " + n + " text").save();
              d.set("emb", n).save();
            }, true, 10);
          } catch (final Exception e) {
            failures.incrementAndGet();
          }
        }
      });
      threads[t].start();
    }
    for (final Thread t : threads)
      t.join();
    check("concurrent insert and update with retried conflicts (" + failures.get() + " gave up)", drifts);

    assertThat(drifts).isEmpty();
  }

  private void check(final String shape, final List<String> drifts) {
    final long totalDocs = metadata().getTotalDocs();
    final long live = live();
    if (totalDocs != live)
      drifts.add(shape + ": totalDocs=" + totalDocs + " live=" + live);
  }
}
