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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A walk of a vertex's edge list reads each chunk as it stood when it reached it, and follows that image's pointer to the chunk
 * behind it. A transaction that commits in between can empty the chunk behind and delete it, relinking the committed image of the
 * one in front around it: the walk's image still names the deleted chunk, and following it answered a
 * {@link RecordNotFoundException}, which is not retryable (issue #11713, the intermittent failure of
 * {@code EdgeAppendMergeRaceTest}). The walk now resumes from the committed image of the chunk it is on, and when that chunk is
 * gone as well it answers a retryable {@link ConcurrentModificationException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgeWalkConcurrentChunkDeleteTest extends TestHelper {
  private static final int SOURCES = 300;

  private int     savedThreshold;
  // THE ONE TEST THAT CORRUPTS THE CHAIN ON PURPOSE TURNS THE AFTER-TEST INTEGRITY CHECK OFF
  private boolean checkIntegrity = true;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return checkIntegrity;
  }

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    // THE CLASSIC LAYOUT: ONE CHAIN OF CHUNKS, NEWEST FIRST
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
  }

  @Test
  void walkResumesPastAChunkAConcurrentCommitDeleted() throws Exception {
    final RID hub = buildHub();
    final List<List<RID>> chunks = sourcesPerChunk(hub);
    assertThat(chunks.size()).as("the hub's list must span at least three chunks").isGreaterThanOrEqualTo(3);

    final List<RID> deleted = chunks.get(1);
    final Set<RID> expected = new HashSet<>();
    for (final List<RID> chunk : chunks)
      expected.addAll(chunk);
    deleted.forEach(expected::remove);

    database.begin();
    try {
      final Iterator<Vertex> walk = hub.asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS").iterator();
      final Set<RID> seen = new HashSet<>();
      // THE WALK STOPS ON THE LAST ENTRY OF THE HEAD CHUNK, WHOSE IMAGE IT HOLDS
      for (int i = 0; i < chunks.getFirst().size(); i++)
        seen.add(walk.next().getIdentity());

      deleteInAnotherTransaction(deleted);

      while (walk.hasNext())
        seen.add(walk.next().getIdentity());

      assertThat(seen).as("every edge but the ones the concurrent commit deleted, each once").isEqualTo(expected);
    } finally {
      database.rollback();
    }
  }

  @Test
  void walkWhoseOwnChunkWasDeletedAnswersARetryableConflict() throws Exception {
    final RID hub = buildHub();
    final List<List<RID>> chunks = sourcesPerChunk(hub);
    assertThat(chunks.size()).as("the hub's list must span at least four chunks").isGreaterThanOrEqualTo(4);

    // THE WALK HOLDS THE SECOND CHUNK; THE CONCURRENT COMMIT EMPTIES IT AND THE THIRD, SO NEITHER IMAGE LEADS ANYWHERE
    final List<RID> deleted = new ArrayList<>(chunks.get(1));
    deleted.addAll(chunks.get(2));

    database.begin();
    try {
      final Iterator<Vertex> walk = hub.asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS").iterator();
      for (int i = 0; i < chunks.get(0).size() + chunks.get(1).size(); i++)
        walk.next();

      deleteInAnotherTransaction(deleted);

      assertThatThrownBy(() -> {
        while (walk.hasNext())
          walk.next();
      }).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aDanglingChainPointerIsStillReportedAsSuch() {
    final RID hub = buildHub();
    final List<List<RID>> chunks = sourcesPerChunk(hub);
    assertThat(chunks.size()).isGreaterThanOrEqualTo(2);

    // NOT A RACE: THE COMMITTED IMAGE OF THE HEAD CHUNK ITSELF NAMES A CHUNK THAT IS GONE. RESUMING FROM IT FINDS THE SAME
    // POINTER, SO THE CORRUPTION IS REPORTED AS IT ALWAYS WAS RATHER THAN DRESSED UP AS A CONFLICT TO RETRY FOREVER
    checkIntegrity = false;
    final RID[] second = new RID[1];
    database.transaction(() -> {
      final EdgeSegment head = (EdgeSegment) database.lookupByRID(((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk(), true);
      second[0] = head.getPreviousRID();
    });
    database.transaction(() -> database.getSchema().getBucketById(second[0].getBucketId()).deleteRecord(second[0]));

    database.transaction(() -> {
      final EdgeSegment head = (EdgeSegment) database.lookupByRID(((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk(), true);
      assertThatThrownBy(head::getPrevious).isInstanceOf(RecordNotFoundException.class);
    });
  }

  private RID buildHub() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Account").createProperty("number", Type.INTEGER);
      database.getSchema().createEdgeType("TRANSFERS");
    });

    final RID[] hub = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Account").set("number", 0).save();
      hub[0] = v.getIdentity();
    });

    // ONE EDGE PER TRANSACTION, SO THE LIST GROWS CHUNK BY CHUNK AS IT DOES UNDER LOAD
    for (int i = 1; i <= SOURCES; i++) {
      final int n = i;
      database.transaction(() -> database.newVertex("Account").set("number", n).save().newEdge("TRANSFERS", hub[0]));
    }
    return hub[0];
  }

  /** The source vertices of the hub's incoming list, one list per chunk, head chunk first. */
  private List<List<RID>> sourcesPerChunk(final RID hub) {
    final List<List<RID>> chunks = new ArrayList<>();
    database.transaction(() -> {
      EdgeSegment segment = (EdgeSegment) database.lookupByRID(((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk(), true);
      while (segment != null) {
        final List<RID> sources = new ArrayList<>();
        final AtomicInteger position = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);
        while (position.get() < segment.getUsed()) {
          segment.getRID(position);
          sources.add(segment.getRID(position));
        }
        chunks.add(sources);
        segment = segment.getPrevious();
      }
    });
    return chunks;
  }

  private void deleteInAnotherTransaction(final List<RID> sources) throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.transaction(() -> {
          for (final RID source : sources)
            source.asVertex(true).delete();
        });
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, "concurrent-deleter");
    writer.start();
    writer.join();
    if (failure.get() != null)
      throw new AssertionError("the concurrent delete failed", failure.get());
  }
}
