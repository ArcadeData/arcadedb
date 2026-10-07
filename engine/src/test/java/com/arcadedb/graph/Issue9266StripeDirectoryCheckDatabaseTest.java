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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9266, follow-ups of #9211 (#8868 roadmap):
 * <ul>
 *   <li>{@link StripeDirectory} accepts a SET of supported placement hash versions instead of an equality on
 *   {@link StripeDirectory#HASH_VERSION}, so a second version is one array entry away;</li>
 *   <li>{@code CHECK DATABASE} reports a stripe directory it cannot read - unknown version, truncated header or body -
 *   as a per-record finding ({@code unreadableStripeDirectories}) with the reason, never as an exception and never as
 *   a corrupted record (fix mode raw-deletes those), and {@code FIX} rebuilds the owning vertex's edge list from the
 *   surviving edge records and reclaims the directory it supersedes.</li>
 * </ul>
 * The hub is its own vertex type, so a check restricted to the neighbours' type or to the edge type reaches the
 * directory only through a back-reference probe: each pass and each arm is driven separately.
 */
class Issue9266StripeDirectoryCheckDatabaseTest extends TestHelper {
  private static final String HUB_TYPE    = "Issue9266Hub";
  private static final String VERTEX_TYPE = "Issue9266Node";
  private static final String EDGE_TYPE   = "Issue9266Link";
  private static final int    DEGREE      = 300;
  private static final byte   FUTURE_HASH = StripeDirectory.HASH_VERSION + 1;
  /** Header (3) + generation 0 (count + 1 slot = 16) + generation 1's stripe count (4): every gen-1 slot is cut off. */
  private static final int    TRUNCATED   = 3 + 16 + 4;

  /** Which of the hub's two lists is promoted and damaged: IN (neighbours point at the hub) unless a test says OUT. */
  private Vertex.DIRECTION hubSide = Vertex.DIRECTION.IN;

  /** The tests deliberately leave a directory this release cannot read, which is the state under test. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @AfterEach
  void restoreDefaults() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.reset();
  }

  @Test
  void supportedHashVersionsAreASetLookupNotAnEquality() {
    assertThat(StripeDirectory.isSupportedHashVersion(StripeDirectory.HASH_VERSION)).isTrue();
    assertThat(StripeDirectory.isSupportedHashVersion(FUTURE_HASH)).isFalse();
    assertThat(StripeDirectory.isSupportedHashVersion((byte) -1)).isFalse();
  }

  @Test
  void aBodyTooShortForItsGenerationsIsRejectedOnLoad() {
    final RID hub = createPromotedHub();

    database.transaction(() -> {
      final RID dirRID = head(hub);
      final byte[] full = bucketOf(dirRID).getRecord(dirRID).copyOfContent().toByteArray();
      final Binary truncated = new Binary(Arrays.copyOf(full, TRUNCATED), TRUNCATED);

      assertThat(StripeDirectory.describeUnreadableContent(new Binary(full, full.length))).isNull();
      assertThat(StripeDirectory.describeUnreadableContent(truncated)).startsWith("is truncated");
      assertThatThrownBy(() -> new StripeDirectory(database, dirRID, truncated))
          .isInstanceOf(DatabaseMetadataException.class)
          .hasMessageContaining(dirRID.toString())
          .hasMessageContaining("is truncated");
    });
  }

  /** A header declaring no generation at all is what a zero-padded short record looks like: corrupt, not empty. */
  @Test
  void aHeaderWithNoGenerationIsRejected() {
    final Binary content = new Binary(new byte[] { StripeDirectory.RECORD_TYPE, StripeDirectory.HASH_VERSION, 0, 0, 0 }, 5);
    assertThat(StripeDirectory.describeUnreadableContent(content)).contains("0 generations");
  }

  @Test
  void checkReportsAFutureVersionDirectoryAsAPerRecordFindingAndChangesNothing() {
    final RID created = createPromotedHub();
    final RID written = rewriteDirectory(created, false);
    reopenDatabase();
    final RID dirRID = rebind(written);
    final RID hub = rebind(created);

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

    assertThat(unreadable(result)).containsExactly(dirRID);
    assertThat(warnings(result)).anyMatch(w -> w.contains(dirRID.toString()) && w.contains("hash version " + FUTURE_HASH));
    // Never a corrupted record: fix mode raw-deletes those, and neither the hub nor its directory is what to delete.
    assertThat(corrupted(result)).doesNotContain(hub, dirRID);

    database.transaction(() -> assertThat(bucketOf(dirRID).getRecord(dirRID).getByte(1)).isEqualTo(FUTURE_HASH));
  }

  @Test
  void checkReportsATruncatedDirectoryAsAPerRecordFinding() {
    final RID created = createPromotedHub();
    final RID written = rewriteDirectory(created, true);
    reopenDatabase();
    final RID dirRID = rebind(written);

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

    assertThat(unreadable(result)).containsExactly(dirRID);
    assertThat(warnings(result)).anyMatch(w -> w.contains(dirRID.toString()) && w.contains("is truncated"));
  }

  /**
   * The silent case the layout validation closes: a directory cut down to a bare header is zero-padded by the bucket
   * to {@code MINIMUM_RECORD_SIZE} and used to read as ZERO generations, i.e. an empty edge list - CHECK then blamed
   * every edge ("missing from that vertex's IN list") and never the directory. It must be reported as the directory.
   */
  @Test
  void checkReportsAZeroGenerationDirectoryAsTheDirectoryNotAsAnEmptyList() {
    final RID created = createPromotedHub();
    final RID written = rewriteDirectory(created, full -> new byte[] { StripeDirectory.RECORD_TYPE, StripeDirectory.HASH_VERSION, 0, 0, 0 });
    reopenDatabase();
    final RID dirRID = rebind(written);

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

    assertThat(unreadable(result)).containsExactly(dirRID);
    assertThat(warnings(result)).anyMatch(w -> w.contains(dirRID.toString()) && w.contains("0 generations"));
    assertThat(warnings(result)).noneMatch(w -> w.contains("missing from that vertex's"));
  }

  @Test
  void fixRebuildsTheHubOverAFutureVersionDirectoryAndReclaimsIt() {
    assertFixRepairs(false);
  }

  @Test
  void fixRebuildsTheHubOverATruncatedDirectoryAndReclaimsIt() {
    assertFixRepairs(true);
  }

  @Test
  void fixRebuildsAPromotedOutgoingListToo() {
    hubSide = Vertex.DIRECTION.OUT;
    assertFixRepairs(false);
  }

  /** Vertex pass, owner arm: only the hub's own type is checked. */
  @Test
  void theHubsOwnVertexCheckReportsTheDirectory() {
    assertScopedCheckReports(c -> c.setTypes(Set.of(HUB_TYPE)));
  }

  /** Vertex pass, far-endpoint probe arm: only the neighbours are checked, the hub is reached through their edges. */
  @Test
  void theNeighboursVertexCheckReportsTheDirectoryThroughItsProbe() {
    assertScopedCheckReports(c -> c.setTypes(Set.of(VERTEX_TYPE)));
  }

  /** Edge pass: the back-reference probe of each edge into the hub's IN list. */
  @Test
  void theEdgeCheckReportsTheDirectoryThroughItsProbe() {
    assertScopedCheckReports(c -> c.setTypes(Set.of(EDGE_TYPE)));
  }

  /** Vertex pass, owner arm, outgoing side. */
  @Test
  void theHubsOwnVertexCheckReportsAnOutgoingDirectory() {
    hubSide = Vertex.DIRECTION.OUT;
    assertScopedCheckReports(c -> c.setTypes(Set.of(HUB_TYPE)));
  }

  /** Vertex pass, far-endpoint probe arm into the hub's OUT list. */
  @Test
  void theNeighboursVertexCheckReportsAnOutgoingDirectoryThroughItsProbe() {
    hubSide = Vertex.DIRECTION.OUT;
    assertScopedCheckReports(c -> c.setTypes(Set.of(VERTEX_TYPE)));
  }

  /** Edge pass: the back-reference probe of each edge into the hub's OUT list. */
  @Test
  void theEdgeCheckReportsAnOutgoingDirectoryThroughItsProbe() {
    hubSide = Vertex.DIRECTION.OUT;
    assertScopedCheckReports(c -> c.setTypes(Set.of(EDGE_TYPE)));
  }

  /** {@code CHECK DATABASE RECORD <hub>}: the scoped vertex arm merges the finding too. */
  @Test
  void theRecordScopedCheckReportsTheDirectory() {
    final RID created = createPromotedHub();
    final RID written = rewriteDirectory(created, false);
    reopenDatabase();
    final RID dirRID = rebind(written);
    final RID hub = rebind(created);

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).setRecords(Set.of(hub)).check();
    assertThat(unreadable(result)).containsExactly(dirRID);
  }

  /** {@code CHECK DATABASE RECORD <edge>}: the scoped edge arm reaches the directory through its probe and merges it. */
  @Test
  void theRecordScopedCheckOfAnEdgeReportsTheDirectory() {
    final RID created = createPromotedHub();
    final RID[] edge = new RID[1];
    database.transaction(() -> edge[0] = created.asVertex().getEdges(hubSide, EDGE_TYPE).iterator().next().getIdentity());
    final RID written = rewriteDirectory(created, false);
    reopenDatabase();
    final RID dirRID = rebind(written);

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).setRecords(Set.of(rebind(edge[0])))
        .check();
    assertThat(unreadable(result)).containsExactly(dirRID);
  }

  private void assertScopedCheckReports(final Consumer<DatabaseChecker> scope) {
    final RID created = createPromotedHub();
    final RID written = rewriteDirectory(created, false);
    reopenDatabase();
    final RID dirRID = rebind(written);

    final DatabaseChecker checker = new DatabaseChecker(database).setVerboseLevel(0);
    scope.accept(checker);
    final Map<String, Object> result = checker.check();

    assertThat(unreadable(result)).containsExactly(dirRID);
    assertThat(warnings(result)).anyMatch(w -> w.contains(dirRID.toString()) && w.contains("cannot read"));
  }

  private void assertFixRepairs(final boolean truncate) {
    final RID created = createPromotedHub();
    final RID neighbourBefore = anyNeighbour(created);
    final RID written = rewriteDirectory(created, truncate);
    reopenDatabase();
    final RID dirRID = rebind(written);
    final RID hub = rebind(created);
    final RID neighbour = rebind(neighbourBefore);

    final Map<String, Object> fixed = new DatabaseChecker(database).setVerboseLevel(0).setFix(true).check();
    assertThat(unreadable(fixed)).containsExactly(dirRID);
    assertThat(fixed.get("reconnectedEdges")).isEqualTo((long) DEGREE);
    // The vertex the directory belonged to survives: only its edge list was rebuilt.
    assertThat((Collection<RID>) fixed.get("deletedRecordsAfterFix")).doesNotContain(hub);

    database.transaction(() -> {
      assertThat(head(hub)).isNotEqualTo(dirRID);
      assertThat(hub.asVertex().countEdges(hubSide, EDGE_TYPE)).isEqualTo(DEGREE);
      assertThat(hub.asVertex().isConnectedTo(neighbour, hubSide)).isTrue();
      // Superseded and unreachable: reclaimed by the orphan segment pass.
      assertThat(bucketOf(dirRID).existsRecord(dirRID)).isFalse();
    });

    final Map<String, Object> after = new DatabaseChecker(database).setVerboseLevel(0).check();
    assertThat(unreadable(after)).isEmpty();
    assertThat(after.get("totalWarnings")).isEqualTo(0L);
  }

  private RID createPromotedHub() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    database.transaction(() -> {
      database.getSchema().createVertexType(HUB_TYPE);
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex(HUB_TYPE);
      hub.save();
      holder[0] = hub.getIdentity();
    });
    for (int batch = 0; batch < DEGREE / 50; batch++)
      database.transaction(() -> {
        for (int i = 0; i < 50; i++) {
          final MutableVertex neighbour = database.newVertex(VERTEX_TYPE);
          neighbour.save();
          if (hubSide == Vertex.DIRECTION.IN)
            neighbour.newEdge(EDGE_TYPE, holder[0]);
          else
            holder[0].asVertex().newEdge(EDGE_TYPE, neighbour);
        }
      });

    database.transaction(() -> assertThat(database.lookupByRID(head(holder[0]), true))
        .as("hub must be promoted to the striped layout, otherwise these tests prove nothing")
        .isInstanceOf(StripeDirectory.class));
    return holder[0];
  }

  /**
   * Rewrites the hub's directory in place, either with an unknown hash version (as a future release would) or cut
   * short after generation 1's stripe count. The bytes go through a subclass because the directory's own buffer is
   * fixed-size: a truncated body cannot be produced through its API.
   */
  private RID rewriteDirectory(final RID hub, final boolean truncate) {
    return rewriteDirectory(hub, full -> {
      if (truncate)
        return Arrays.copyOf(full, TRUNCATED);
      full[1] = FUTURE_HASH;
      return full;
    });
  }

  private RID rewriteDirectory(final RID hub, final UnaryOperator<byte[]> transform) {
    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final RID dirRID = head(hub);
      final byte[] bytes = transform.apply(bucketOf(dirRID).getRecord(dirRID).copyOfContent().toByteArray());
      final Binary rewritten = new Binary(bytes, bytes.length);
      final StripeDirectory copy = new StripeDirectory(database, dirRID, bucketOf(dirRID).getRecord(dirRID).copyOfContent()) {
        @Override
        public Binary getContent() {
          rewritten.position(0);
          return rewritten;
        }
      };
      ((DatabaseInternal) database).updateRecord(copy);
      holder[0] = dirRID;
    });
    return holder[0];
  }

  /** Rebinds a RID to the reopened database instance. */
  private static RID rebind(final RID rid) {
    return new RID(rid.getBucketId(), rid.getPosition());
  }

  private RID anyNeighbour(final RID hub) {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = hub.asVertex().getVertices(hubSide, EDGE_TYPE).iterator().next().getIdentity());
    return holder[0];
  }

  private RID head(final RID hub) {
    final VertexInternal vertex = (VertexInternal) hub.asVertex(true);
    return hubSide == Vertex.DIRECTION.IN ? vertex.getInEdgesHeadChunk() : vertex.getOutEdgesHeadChunk();
  }

  private LocalBucket bucketOf(final RID rid) {
    return (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
  }

  @SuppressWarnings("unchecked")
  private static Collection<RID> unreadable(final Map<String, Object> result) {
    return (Collection<RID>) result.get("unreadableStripeDirectories");
  }

  @SuppressWarnings("unchecked")
  private static Collection<RID> corrupted(final Map<String, Object> result) {
    return (Collection<RID>) result.get("corruptedRecords");
  }

  @SuppressWarnings("unchecked")
  private static Collection<String> warnings(final Map<String, Object> result) {
    return (Collection<String>) result.get("warnings");
  }
}
