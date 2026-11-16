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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.WALFile;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12250: a transaction applied as pages - a follower applying the replicated log, the leader applying a commit a follower
 * made in its own transaction - fires no record event, so the views hear of it from
 * {@link GraphAnalyticalViewRegistry#onTransactionApplied}. The applying thread only counts it: the view reports STALE and is not
 * served, an incremental view rebuilds when it is asked for, an OFF view waits for an explicit rebuild, and a view behind such a
 * commit does not persist its CSR. The HA end of it is {@code Issue12250GavFollowsReplicatedCommitsIT} in ha-raft.
 */
class GraphAnalyticalViewReplicatedCommitTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.getSchema().createDocumentType("Doc");
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("V").save();
      for (int i = 0; i < 10; i++)
        hub.newEdge("E", database.newVertex("V").save());
      database.newDocument("Doc").save();
    });
  }

  @Test
  void anIncrementalViewIsStaleAfterAReplicatedCommitAndRebuildsWhenAskedFor() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g UPDATE MODE SYNCHRONOUS");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    final long builtAt = view.getBuildTimestamp();

    // A page of a document type's bucket changes nothing the view serves
    GraphAnalyticalViewRegistry.onTransactionApplied(database, entryWriting(bucketOf("Doc")));
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

    GraphAnalyticalViewRegistry.onTransactionApplied(database, entryWriting(bucketOf("E")));
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
    assertThat(view.getBuildTimestamp()).as("nothing rebuilt on the applying thread").isEqualTo(builtAt);

    // A query that finds it behind takes the record path and asks for the rebuild, which runs off its thread: no wait asks for it
    assertThat(edges()).isEqualTo(10);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
        .until(() -> view.getStatus() == GraphAnalyticalView.Status.READY);
    assertThat(view.getBuildTimestamp()).isGreaterThanOrEqualTo(builtAt);
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.getEdgeCount()).isEqualTo(10);
  }

  @Test
  void anOffViewStaysStaleUntilRebuilt() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

    GraphAnalyticalViewRegistry.onTransactionApplied(database, entryWriting(bucketOf("V")));
    assertThat(view.isReady()).isFalse();
    assertThat(view.awaitReady(1, TimeUnit.SECONDS)).as("nothing rebuilds an OFF view by itself").isFalse();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);

    database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW g");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
  }

  @Test
  void aViewBehindAReplicatedCommitDoesNotPersistItsCsr() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g UPDATE MODE SYNCHRONOUS");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    GraphAnalyticalViewRegistry.onTransactionApplied(database, entryWriting(bucketOf("V")));

    // The transaction counter its certificate checks does not move for such a commit: a CSR written now would be reopened as current
    reopenDatabase();
    assertThat(GraphAnalyticalViewCSRPersistence.fileFor(database, "g").isFile()).isFalse();
    final GraphAnalyticalView reopened = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(reopened.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(reopened.isRestoredFromPersistedCsr()).isFalse();
    assertThat(reopened.getEdgeCount()).isEqualTo(10);
  }

  @Test
  void onlyBucketsAViewCanReadCount() {
    final Schema schema = database.getSchema();
    assertThat(GraphAnalyticalViewRegistry.writesGraphBuckets(schema, entryWriting(bucketOf("V")))).isTrue();
    assertThat(GraphAnalyticalViewRegistry.writesGraphBuckets(schema, entryWriting(bucketOf("E")))).isTrue();
    // The edge list of a vertex bucket belongs to no type, the vertex type involves it
    final int edgeList = schema.getBuckets().stream().filter(bucket -> bucket.getName().endsWith("_out_edges")).findFirst()
        .orElseThrow().getFileId();
    assertThat(GraphAnalyticalViewRegistry.writesGraphBuckets(schema, entryWriting(edgeList))).isTrue();
    assertThat(GraphAnalyticalViewRegistry.writesGraphBuckets(schema, entryWriting(bucketOf("Doc")))).isFalse();
    assertThat(GraphAnalyticalViewRegistry.writesGraphBuckets(schema, entryWriting(schema.getDictionary().getFileId()))).isFalse();
  }

  private long edges() {
    try (final ResultSet rs = database.query("sql", "SELECT sum(out('E').size()) AS c FROM V")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private int bucketOf(final String typeName) {
    return database.getSchema().getType(typeName).getBuckets(false).getFirst().getFileId();
  }

  private static WALFile.WALTransaction entryWriting(final int... fileIds) {
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.pages = new WALFile.WALPage[fileIds.length];
    for (int i = 0; i < fileIds.length; i++) {
      tx.pages[i] = new WALFile.WALPage();
      tx.pages[i].fileId = fileIds[i];
    }
    return tx;
  }
}
