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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11464: an INCREMENTAL view refreshes on the thread that committed, and the owner of the refresh used to keep making
 * passes for as long as other writers kept requesting one. Under a steady write load from several clients one of them never
 * got its commit back (a 30 s stall in the three-node load test, until its HTTP read timed out). The committing thread now
 * makes the one pass it owes and hands any further pass to the schema's scheduler thread.
 */
class Issue11464IncrementalRefreshBoundedOnCommitterTest extends TestHelper {
  /**
   * ORDER BY keeps the view off the per-record path (#11475): these tests count the writes of whole-view passes, each of which
   * rewrites every row.
   */
  private static final String LEGACY_QUERY = "SELECT value FROM Src11464 ORDER BY value";

  @Test
  void theCommitterMakesOnePassAndALaterPassRunsOffItsThread() throws Exception {
    final List<String> writers = runTwoOverlappingCommits(false);
    assertThat(writers.stream().filter("writer-11464"::equals).count())
        .as("the committer wrote one pass to the view, not the follow-up pass the other writer asked for (writers: %s)", writers)
        .isEqualTo(1);
  }

  /** A scheduler that cannot take the hand-off leaves the passes with the committer, as before: the request is still serviced. */
  @Test
  void aSchedulerThatCannotTakeTheHandOffLeavesThePassesWithTheCommitter() throws Exception {
    final List<String> writers = runTwoOverlappingCommits(true);
    assertThat(writers.stream().filter("writer-11464"::equals).count())
        .as("the committer made the follow-up pass itself (writers: %s)", writers)
        .isEqualTo(3);
  }

  /** A hand-off the scheduler accepted and a shutdown dropped before it ran must not leave the view looking VALID. */
  @Test
  void aHandOffDroppedByShutdownLeavesTheViewStale() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType("Src11464"));
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("Src11464View")
        .withQuery(LEGACY_QUERY)
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());
    final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Src11464View");
    final MaterializedViewScheduler scheduler = ((LocalSchema) database.getSchema()).getMaterializedViewScheduler();

    // occupy the scheduler thread so the hand-off stays queued
    final CountDownLatch busy = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    scheduler.handOff(database, view, () -> {
      busy.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    assertThat(busy.await(30, TimeUnit.SECONDS)).isTrue();

    assertThat(view.tryBeginRefresh()).isTrue();
    final AtomicBoolean ran = new AtomicBoolean();
    assertThat(scheduler.handOff(database, view, () -> ran.set(true))).isTrue();

    final List<MaterializedViewImpl> dropped = scheduler.shutdown();
    release.countDown();

    assertThat(dropped).containsExactly(view);
    assertThat(ran.get()).as("the queued hand-off was dropped, not run").isFalse();
  }

  private List<String> runTwoOverlappingCommits(final boolean schedulerShutDown) throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType("Src11464"));
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("Src11464View")
        .withQuery(LEGACY_QUERY)
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());

    // every write the refresh makes to the backing type is recorded with the thread that made it; the first one parks
    final List<String> writers = new CopyOnWriteArrayList<>();
    if (schedulerShutDown)
      ((LocalSchema) database.getSchema()).getMaterializedViewScheduler().shutdown();
    final CountDownLatch firstPassParked = new CountDownLatch(1);
    final CountDownLatch releaseFirstPass = new CountDownLatch(1);
    final DocumentTypeEvents events = new DocumentTypeEvents(database.getSchema().getType("Src11464View"));
    events.onWrite(() -> {
      writers.add(Thread.currentThread().getName());
      if (writers.size() == 1) {
        firstPassParked.countDown();
        try {
          releaseFirstPass.await(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });

    final Thread committer = new Thread(
        () -> database.transaction(() -> database.newDocument("Src11464").set("value", 1).save()), "writer-11464");
    committer.start();
    assertThat(firstPassParked.await(30, TimeUnit.SECONDS)).as("the committer is inside its refresh pass").isTrue();

    // a second writer commits while that pass runs: it hands its request to the running refresh and returns
    database.transaction(() -> database.newDocument("Src11464").set("value", 2).save());

    releaseFirstPass.countDown();
    committer.join(TimeUnit.SECONDS.toMillis(30));
    assertThat(committer.isAlive()).as("the committer got its commit back").isFalse();

    // the request it was handed is still serviced, by a pass that is not on the committer's thread
    final long deadline = System.currentTimeMillis() + 30_000;
    while (viewCount() != 2 && System.currentTimeMillis() < deadline)
      Thread.sleep(50);
    assertThat(viewCount()).as("the view reflects both commits").isEqualTo(2);

    return writers;
  }

  private long viewCount() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Src11464View")) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }

  /** Registers one callback for the creates and updates of a type. */
  private static final class DocumentTypeEvents {
    private final DocumentType type;

    private DocumentTypeEvents(final DocumentType type) {
      this.type = type;
    }

    private void onWrite(final Runnable callback) {
      type.getEvents().registerListener((BeforeRecordCreateListener) record -> {
        callback.run();
        return true;
      });
      type.getEvents().registerListener((BeforeRecordUpdateListener) record -> {
        callback.run();
        return true;
      });
    }
  }
}
