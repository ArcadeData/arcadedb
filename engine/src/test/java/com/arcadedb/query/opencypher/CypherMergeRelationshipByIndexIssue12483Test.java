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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12483: a MERGE of a relationship between two bound nodes answers its existence check from an exact-key index of the
 * relationship's type, instead of walking an edge list. The walk recorded a read of the head of the list, and any relationship a
 * concurrent transaction gave either node moved it and refused the MERGE at commit: a bulk load of relationships with unique keys
 * between shared vertices was refused about twice per committed batch. The same relationship created concurrently, by any
 * statement and with the key it is committed with, still refuses it through the registry of #11482.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergeRelationshipByIndexIssue12483Test extends TestHelper {
  private static final String MERGE = "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:E {uuid: 'x'}]->(b) SET e.f = 1";
  private static final String BATCH = "UNWIND $rows AS r MATCH (a:V {id: r[0]}), (b:V {id: r[1]}) MERGE (a)-[e:E {uuid: r[2]}]->(b)";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E", 1).createProperty("uuid", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "E", "uuid");
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("id", i).save();
    });
  }

  /**
   * The shape of the load in the issue: a batch gives a vertex its first relationship in one row and MERGEs another one of it in a
   * later row, whose existence check walked the list the batch had just started; a concurrent batch giving the same vertex its first
   * relationship moved that head, and the walk's head read refused the batch at commit.
   */
  @Test
  void aRelationshipAConcurrentTransactionGaveAnEndpointDoesNotRefuseTheBatch() throws Exception {
    database.begin();
    database.command("opencypher", BATCH, Map.of("rows", List.of(List.of(1, 2, "x1"), List.of(1, 3, "x2"), List.of(4, 2, "x3")))).close();
    // the first relationship of node 1 to another node, and of another node to node 2: both heads the batch walked move
    commitFromAnotherThread("MATCH (a:V {id: 1}), (c:V {id: 5}) CREATE (a)-[:E {uuid: 'y'}]->(c)");
    commitFromAnotherThread("MATCH (c:V {id: 5}), (b:V {id: 2}) CREATE (c)-[:E {uuid: 'z'}]->(b)");
    database.commit();

    assertThat(count("MATCH (a:V {id: 1})-[e:E]->() RETURN count(e) AS c")).isEqualTo(3L);
    assertThat(count("MATCH ()-[e:E]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(3L);
    assertThat(count("MATCH ()-[e:E]->() RETURN count(e) AS c")).isEqualTo(5L);
  }

  @Test
  void anotherRelationshipCreatedConcurrentlyBetweenTheSameNodesDoesNotRefuseTheBatch() throws Exception {
    database.begin();
    database.command("opencypher", BATCH, Map.of("rows", List.of(List.of(1, 2, "x1"), List.of(1, 2, "x2")))).close();
    // the same two nodes and type, another key: a relationship the MERGE was not looking for
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:E {uuid: 'y'}]->(b)");
    database.commit();
    assertThat(count("MATCH (a:V {id: 1})-[e:E]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(3L);
  }

  @Test
  void theSameRelationshipCreatedConcurrentlyRefusesTheMerge() throws Exception {
    database.begin();
    database.command("opencypher", MERGE).close();
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:E {uuid: 'x'}]->(b)");
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);

    // the retry finds it and does not create it again
    database.transaction(() -> database.command("opencypher", MERGE).close());
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN e.f AS c")).isEqualTo(1L);
  }

  /** The key is the one the relationship is committed with: a SET after the CREATE gives it, and it is the one marked. */
  @Test
  void theSameRelationshipGivenItsKeyByASetAfterItsCreationRefusesTheMerge() throws Exception {
    database.begin();
    database.command("opencypher", MERGE).close();
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[e:E]->(b) SET e.uuid = 'x'");
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
  }

  /** The same relationship created the other way round: an undirected MERGE would have found it, the key is the unordered pair. */
  @Test
  void theSameRelationshipCreatedConcurrentlyTheOtherWayRoundRefusesAnUndirectedMerge() throws Exception {
    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:E {uuid: 'x'}]-(b)").close();
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (b)-[:E {uuid: 'x'}]->(a)");
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  void theSameRelationshipMergedConcurrentlyRefusesTheMerge() throws Exception {
    database.begin();
    database.command("opencypher", MERGE).close();
    commitFromAnotherThread(MERGE);
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  /** A relationship of a subtype is found by a MERGE of its super type, and created concurrently it refuses one. */
  @Test
  void aRelationshipOfASubtypeCreatedConcurrentlyRefusesTheMergeOfItsSuperType() throws Exception {
    database.transaction(() -> database.getSchema().createEdgeType("SubE", 1).addSuperType("E"));

    database.begin();
    database.command("opencypher", MERGE).close();
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:SubE {uuid: 'x'}]->(b)");
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);

    database.transaction(() -> database.command("opencypher", MERGE).close());
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  /**
   * The values are compared as stored: an INTEGER key the MERGE looks up by a LONG parameter and the concurrent statement writes as
   * a whole DOUBLE is the same key.
   */
  @Test
  void theSameKeyWrittenInAnotherNumericFormRefusesTheMerge() throws Exception {
    database.transaction(() -> {
      database.getSchema().createEdgeType("N", 1).createProperty("n", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "N", "n");
    });

    final String merge = "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:N {n: $n}]->(b)";
    database.begin();
    database.command("opencypher", merge, Map.of("n", 7L)).close();
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:N {n: 7.0}]->(b)");
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);

    database.transaction(() -> database.command("opencypher", merge, Map.of("n", 7L)).close());
    assertThat(count("MATCH (a:V {id: 1})-[e:N]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  /** A DATE key looked up by a {@code date()} and written by the concurrent statement as text is the same key once stored. */
  @Test
  void theSameDateKeyWrittenAsTextRefusesTheMerge() throws Exception {
    database.transaction(() -> {
      database.getSchema().createEdgeType("D", 1).createProperty("d", Type.DATE);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "D", "d");
    });

    final String merge = "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:D {d: date('2024-03-01')}]->(b)";
    database.begin();
    database.command("opencypher", merge).close();
    commitFromAnotherThread("MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:D {d: '2024-03-01'}]->(b)");
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);

    database.transaction(() -> database.command("opencypher", merge).close());
    assertThat(count("MATCH (a:V {id: 1})-[e:D]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  /**
   * An index registered on the type after a transaction created a relationship of it: the reconciliation indexes the relationship at
   * its commit, so a MERGE answered by the new index meanwhile has to be refused by its key as well.
   */
  @Test
  void aRelationshipCreatedBeforeItsTypeWasIndexedRefusesAMergeAnsweredByTheNewIndex() throws Exception {
    database.transaction(() -> database.getSchema().createEdgeType("F", 1).createProperty("k", Type.STRING));

    final CountDownLatch created = new CountDownLatch(1);
    final CountDownLatch commit = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread creator = new Thread(() -> {
      try {
        database.transaction(() -> {
          database.command("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:F {k: 'x'}]->(b)").close();
          created.countDown();
          try {
            commit.await(30, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      } catch (final Throwable e) {
        error.set(e);
      }
    });
    creator.start();
    assertThat(created.await(30, TimeUnit.SECONDS)).isTrue();

    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "F", "k");

    final String merge = "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:F {k: 'x'}]->(b)";
    database.begin();
    database.command("opencypher", merge).close();
    commit.countDown();
    creator.join();
    assertThat(error.get()).isNull();
    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);

    database.transaction(() -> database.command("opencypher", merge).close());
    assertThat(count("MATCH (a:V {id: 1})-[e:F]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  @Test
  void anExistingRelationshipIsMatchedThroughTheIndexAndUpdated() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:E {uuid: 'x', f: 0}]->(b)").close());
    database.transaction(() -> database.command("opencypher", MERGE).close());
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN e.f AS c")).isEqualTo(1L);

    // the same key between other nodes, or the other way round, is another relationship
    database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 2}), (b:V {id: 1}) MERGE (a)-[e:E {uuid: 'x'}]->(b)").close());
    database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 3}) MERGE (a)-[e:E {uuid: 'x'}]->(b)").close());
    assertThat(count("MATCH ()-[e:E {uuid: 'x'}]->() RETURN count(e) AS c")).isEqualTo(3L);

    // an undirected MERGE matches it whichever way it points
    database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 3}), (b:V {id: 1}) MERGE (a)-[e:E {uuid: 'x'}]-(b)").close());
    assertThat(count("MATCH ()-[e:E {uuid: 'x'}]->() RETURN count(e) AS c")).isEqualTo(3L);

    // a property of the pattern the index does not cover is still compared
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:E {uuid: 'x', f: 2}]->(b)").close());
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(2L);
  }

  /** A relationship a previous row of the same statement created is found through the index, as the walk found it. */
  @Test
  void aRelationshipCreatedEarlierInTheTransactionIsFound() {
    database.transaction(() -> {
      database.command("opencypher", "UNWIND [1, 2, 3] AS i MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[e:E {uuid: 'x'}]->(b)")
          .close();
      database.command("opencypher", MERGE).close();
    });
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  /** A relationship deleted in the transaction is not matched, so it is created again, as the walk did. */
  @Test
  void aRelationshipDeletedInTheTransactionIsNotMatched() {
    database.transaction(() -> database.command("opencypher", MERGE).close());
    database.transaction(() -> {
      database.command("opencypher", "MATCH ()-[e:E {uuid: 'x'}]->() DELETE e").close();
      database.command("opencypher", MERGE).close();
    });
    assertThat(count("MATCH (a:V {id: 1})-[e:E {uuid: 'x'}]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  /** An index whose key the pattern covers only in part keeps the walk, which answers the same. */
  @Test
  void aPrefixOfACompositeIndexKeepsTheWalk() {
    database.transaction(() -> {
      database.getSchema().createEdgeType("C", 1).createProperty("k1", Type.STRING);
      database.getSchema().getType("C").createProperty("k2", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "C", "k1", "k2");
      database.command("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 2}) CREATE (a)-[:C {k1: 'p', k2: 'q'}]->(b)").close();
    });
    database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 2}) MERGE (a)-[:C {k1: 'p'}]->(b)").close());
    assertThat(count("MATCH (a:V {id: 1})-[e:C]->(b:V {id: 2}) RETURN count(e) AS c")).isEqualTo(1L);
  }

  /**
   * The load of the issue, smaller: writers MERGE-ing relationships with unique keys between shared vertices, each batch in one
   * transaction and retried when refused. Every relationship is stored once, reachable from both ends.
   */
  @Test
  void concurrentMergeBatchesStoreEveryRelationshipOnce() throws Exception {
    final int writers = 4;
    final int batches = 12;
    final int batchSize = 40;
    final ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int b = 0; b < batches; b++) {
        final List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < batchSize; i++)
          rows.add(List.of(i % 6, (i * 7 + b) % 6, "b" + b + "-" + i));
        futures.add(pool.submit(() -> {
          for (int attempt = 0; ; attempt++) {
            try {
              database.transaction(() -> database.command("opencypher",
                  "UNWIND $rows AS r MATCH (a:V {id: r[0]}), (b:V {id: r[1]}) MERGE (a)-[e:E {uuid: r[2]}]->(b)", Map.of("rows", rows))
                  .close(), false, 0);
              return;
            } catch (final NeedRetryException e) {
              if (attempt > 500)
                throw e;
            }
          }
        }));
      }
      for (final Future<?> future : futures)
        future.get(120, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    final long expected = (long) batches * batchSize;
    assertThat(count("MATCH ()-[e:E]->() RETURN count(e) AS c")).isEqualTo(expected);
    assertThat(count("MATCH ()-[e:E]->() RETURN count(DISTINCT e.uuid) AS c")).isEqualTo(expected);
    assertThat(count("MATCH (a:V)-[e:E]->() RETURN count(e) AS c")).isEqualTo(expected);
    assertThat(count("MATCH ()-[e:E]->(b:V) RETURN count(e) AS c")).isEqualTo(expected);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void commitFromAnotherThread(final String command) throws InterruptedException {
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        database.transaction(() -> database.command("opencypher", command).close());
      } catch (final Throwable e) {
        error.set(e);
      }
    });
    thread.start();
    thread.join();
    assertThat(error.get()).isNull();
  }
}
