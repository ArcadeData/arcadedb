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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Record;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #4817: {@code TRUNCATE TYPE} was slow and incomplete. Under HA a large
 * batch produced a multi-MB Raft entry that stalled the leader heartbeat; the resulting leader change
 * interrupted a mid-scan commit, and {@code LocalBucket.scan} swallowed that failure as a per-record
 * "Error on loading record", so a partial truncate was reported as success and the dropped indexes were
 * rebuilt over the records that were never deleted. The fix buffers RIDs in a read-only scan, deletes in
 * small configurable batches AFTER the scan so each Raft entry stays small, and lets a delete/commit
 * failure propagate instead of masquerading as success.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TruncateType4817Test extends TestHelper {

  @Test
  void truncateEdgeTypeCompletelyAcrossManySmallBatches() {
    // Force several commit batches with a small dataset.
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 100);

    final int total = 1000;
    database.command("sql", "CREATE VERTEX TYPE Node");
    database.command("sql", "CREATE EDGE TYPE Link");

    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Node").set("name", "hub").save();
      for (int i = 0; i < total; i++) {
        final MutableVertex leaf = database.newVertex("Node").set("n", i).save();
        hub.newEdge("Link", leaf);
      }
    });

    assertThat(database.countType("Link", false)).isEqualTo((long) total);

    database.transaction(() -> database.command("sql", "TRUNCATE TYPE Link UNSAFE"));

    // Every edge must be gone, not just the first batch.
    assertThat(database.countType("Link", false)).isEqualTo(0L);

    // The hub vertex's out-edge list must be empty too (bidirectional cleanup ran).
    database.transaction(() -> {
      final var hub = database.query("sql", "SELECT FROM Node WHERE name = 'hub'").next().getVertex().get();
      assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "Link")).isEqualTo(0L);
    });

    // New edges can still be created after the truncate.
    database.transaction(() -> {
      final var a = database.newVertex("Node").set("name", "a").save();
      final var b = database.newVertex("Node").set("name", "b").save();
      a.newEdge("Link", b);
    });
    assertThat(database.countType("Link", false)).isEqualTo(1L);
  }

  @Test
  void truncateSurfacesDeleteFailureInsteadOfReportingSuccess() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 100);

    final int total = 500;
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.n INTEGER");
    database.command("sql", "CREATE INDEX ON Doc(n) NOTUNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < total; i++)
        database.command("sql", "INSERT INTO Doc SET n = ?", i);
    });

    // Simulate an interrupted truncate: blow up partway through the deletion phase.
    final AtomicInteger deletes = new AtomicInteger();
    final BeforeRecordDeleteListener boom = (final Record record) -> {
      if (deletes.incrementAndGet() == 250)
        throw new RuntimeException("simulated mid-truncate failure");
      return true;
    };
    database.getSchema().getType("Doc").getEvents().registerListener(boom);

    try {
      // The command MUST fail loudly rather than silently report a partial truncate as success.
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "TRUNCATE TYPE Doc")))
          .hasMessageContaining("simulated mid-truncate failure");
    } finally {
      database.getSchema().getType("Doc").getEvents().unregisterListener(boom);
    }

    // The index dropped during truncate must have been recreated (schema not left without it),
    // so the type stays usable.
    assertThat(database.getSchema().getType("Doc").getAllIndexes(true)).isNotEmpty();

    // A retry now completes the truncate cleanly.
    database.transaction(() -> database.command("sql", "TRUNCATE TYPE Doc"));
    assertThat(database.countType("Doc", false)).isEqualTo(0L);

    // Re-insert and the recreated index is consistent.
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET n = 1"));
    assertThat(database.query("sql", "SELECT FROM Doc WHERE n = 1").stream().count()).isEqualTo(1L);
  }
}
