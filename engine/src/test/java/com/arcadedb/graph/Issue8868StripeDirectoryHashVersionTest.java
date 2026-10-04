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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DatabaseMetadataException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #8868 (prerequisite step): the {@link StripeDirectory} placement-hash version byte (header byte 1) was
 * written but never read. Entries of a promoted vertex are located by RE-HASHING the neighbour RID, so a directory
 * written by a future release with a different placement function would be read by this release with the version-0
 * hash: {@code isConnectedTo} / {@code containsVertex} / {@code removeVertex} would look in the wrong stripe and
 * silently answer "not connected" or miss a removal. The directory must FAIL CLOSED on an unknown version instead,
 * on every path that builds one from stored bytes.
 */
class Issue8868StripeDirectoryHashVersionTest extends TestHelper {
  private static final String VERTEX_TYPE = "Issue8868Node";
  private static final String EDGE_TYPE   = "Issue8868Link";
  private static final int    DEGREE      = 300;
  private static final byte   FUTURE_HASH = StripeDirectory.HASH_VERSION + 1;

  private boolean reopened;

  /** The tests deliberately write a directory this release cannot read, which is the state under test. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @AfterEach
  void restoreDefaults() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.reset();
  }

  @Test
  void currentVersionDirectoryStaysReadableAfterReopen() {
    final RID hub = reopen(createPromotedHub());

    database.transaction(() -> {
      final StripeDirectory directory = loadDirectory(hub);
      assertThat(directory.getGenerationCount()).isEqualTo(2);
      assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)).isEqualTo(DEGREE);
    });
  }

  @Test
  void constructorRejectsUnknownHashVersion() {
    final RID hub = createPromotedHub();

    database.transaction(() -> {
      final RID dirRID = inHead(hub);
      final Binary content = bucketOf(dirRID).getRecord(dirRID).copyOfContent();
      content.putByte(1, FUTURE_HASH);

      assertThatThrownBy(() -> new StripeDirectory(database, dirRID, content))
          .isInstanceOf(DatabaseMetadataException.class)
          .hasMessageContaining("hash version " + FUTURE_HASH)
          .hasMessageContaining(dirRID.toString());
    });
  }

  @Test
  void lazyPlaceholderRejectsUnknownHashVersionOnFirstAccess() {
    final RID created = createPromotedHub();
    final RID dirRID = reopen(writeFutureHashVersion(created));

    database.transaction(() -> {
      final StripeDirectory lazy = (StripeDirectory) ((DatabaseInternal) database).getRecordFactory()
          .newImmutableRecord(database, null, dirRID, StripeDirectory.RECORD_TYPE);
      assertThatThrownBy(lazy::getGenerationCount)
          .isInstanceOf(DatabaseMetadataException.class)
          .hasMessageContaining("hash version " + FUTURE_HASH);
      // A rejected load must not leave the content behind: the second access fails again instead of reading it.
      assertThatThrownBy(() -> lazy.getHead(1, 0))
          .isInstanceOf(DatabaseMetadataException.class)
          .hasMessageContaining("hash version " + FUTURE_HASH);
    });
  }

  @Test
  void readsOfAFutureVersionDirectoryFailLoudly() {
    final RID created = createPromotedHub();
    final RID neighbourBefore = anyNeighbour(created);
    final RID dirRID = reopen(writeFutureHashVersion(created));
    final RID hub = reopen(created);
    final RID neighbour = reopen(neighbourBefore);

    database.transaction(() -> {
      // RecordFactory content path (lookupByRID)
      assertFailsClosed(() -> database.lookupByRID(dirRID, true));

      final Vertex v = hub.asVertex();
      assertFailsClosed(() -> v.countEdges(Vertex.DIRECTION.IN, EDGE_TYPE));
      // The silent wrong answer the version byte exists to prevent: a re-hashed lookup in the wrong stripe.
      assertFailsClosed(() -> v.isConnectedTo(neighbour, Vertex.DIRECTION.IN));
    });
  }

  @Test
  void appendsToAFutureVersionDirectoryFailLoudly() {
    final RID created = createPromotedHub();
    final RID dirRID = reopen(writeFutureHashVersion(created));
    final RID hub = reopen(created);

    assertFailsClosed(() -> database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final MutableVertex src = database.newVertex(VERTEX_TYPE);
        src.save();
        src.newEdge(EDGE_TYPE, hub);
      }
    }));

    // Nothing was written over the future-format directory.
    database.transaction(() -> {
      final Binary raw = bucketOf(dirRID).getRecord(dirRID);
      assertThat(raw.getByte(1)).isEqualTo(FUTURE_HASH);
    });
  }

  @Test
  void checkDatabaseReportsTheDirectoryAndDoesNotReclaimItsSegments() {
    final RID created = createPromotedHub();
    writeFutureHashVersion(created);
    final RID hub = reopen(created);

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();
    assertThat(result.get("warnings").toString()).contains(hub.toString());

    // Without FIX nothing changes: the directory is still there, still in the future format.
    database.transaction(() -> {
      final RID dirRID = inHead(hub);
      assertThat(bucketOf(dirRID).getRecord(dirRID).getByte(1)).isEqualTo(FUTURE_HASH);
    });
  }

  private RID createPromotedHub() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex(VERTEX_TYPE);
      hub.save();
      holder[0] = hub.getIdentity();
    });
    for (int batch = 0; batch < DEGREE / 50; batch++)
      database.transaction(() -> {
        for (int i = 0; i < 50; i++) {
          final MutableVertex src = database.newVertex(VERTEX_TYPE);
          src.save();
          src.newEdge(EDGE_TYPE, holder[0]);
        }
      });

    database.transaction(() -> assertThat(database.lookupByRID(inHead(holder[0]), true))
        .as("hub must be promoted to the striped layout, otherwise these tests prove nothing")
        .isInstanceOf(StripeDirectory.class));
    return holder[0];
  }

  /** Rewrites the hub's directory in place with an unknown hash version, as a future release would. */
  private RID writeFutureHashVersion(final RID hub) {
    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final RID dirRID = inHead(hub);
      final StripeDirectory copy = new StripeDirectory(database, dirRID, bucketOf(dirRID).getRecord(dirRID).copyOfContent());
      copy.getContent().putByte(1, FUTURE_HASH);
      ((DatabaseInternal) database).updateRecord(copy);
      holder[0] = dirRID;
    });
    return holder[0];
  }

  /** The operation must throw a {@link DatabaseMetadataException}, either directly or wrapped by a caller. */
  private static void assertFailsClosed(final ThrowingCallable operation) {
    final Throwable thrown = catchThrowable(operation);
    assertThat(thrown).as("an unknown placement hash version must fail closed, not answer with the version-0 hash").isNotNull();
    for (Throwable t = thrown; t != null; t = t.getCause())
      if (t instanceof DatabaseMetadataException) {
        assertThat(t).hasMessageContaining("hash version " + FUTURE_HASH);
        return;
      }
    throw new AssertionError("expected a DatabaseMetadataException in the cause chain", thrown);
  }

  /**
   * Closes and reopens the database the first time it is called for a test (drops every cached record, so the next
   * read comes from the stored bytes), and rebinds {@code rid} to the open instance.
   */
  private RID reopen(final RID rid) {
    if (!reopened) {
      reopenDatabase();
      reopened = true;
    }
    return new RID(rid.getBucketId(), rid.getPosition());
  }

  private RID anyNeighbour(final RID hub) {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = hub.asVertex().getVertices(Vertex.DIRECTION.IN, EDGE_TYPE).iterator().next().getIdentity());
    return holder[0];
  }

  private StripeDirectory loadDirectory(final RID hub) {
    return (StripeDirectory) database.lookupByRID(inHead(hub), true);
  }

  private RID inHead(final RID hub) {
    return ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
  }

  private LocalBucket bucketOf(final RID rid) {
    return (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
  }
}
