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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11470: the passes of an INCREMENTAL view handed to the schema's scheduler thread must not run as one unbounded loop on
 * it, because that single thread also runs every periodic refresh of every view of the database. A pass that leaves more owed
 * queues the next one behind whatever is already due.
 */
class Issue11470HandedOffRefreshYieldsTest extends TestHelper {

  @Test
  void aHandedOffPassQueuesTheNextOneBehindWorkAlreadyWaiting() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType("Src11470"));
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("Src11470View")
        .withQuery("SELECT value FROM Src11470")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());
    final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Src11470View");
    final MaterializedViewScheduler scheduler = ((LocalSchema) database.getSchema()).getMaterializedViewScheduler();

    final List<String> events = new CopyOnWriteArrayList<>();
    final CountDownLatch committerParked = new CountDownLatch(1);
    final CountDownLatch releaseCommitter = new CountDownLatch(1);
    final AtomicBoolean probeQueued = new AtomicBoolean();
    final Runnable onWrite = () -> {
      final String thread = Thread.currentThread().getName();
      if (thread.startsWith("ArcadeDB-MV-Scheduler")) {
        events.add("scheduler-write");
        // sustained writers: during the first handed-off pass another pass is requested, and other work is waiting for the thread
        if (probeQueued.compareAndSet(false, true)) {
          scheduler.handOff(database, view, () -> events.add("other-work"));
          view.markRefreshPendingIfRunning();
        }
      } else {
        events.add("committer-write");
        if (committerParked.getCount() > 0) {
          committerParked.countDown();
          try {
            releaseCommitter.await(60, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    };
    final DocumentType backing = database.getSchema().getType("Src11470View");
    backing.getEvents().registerListener((BeforeRecordCreateListener) record -> {
      onWrite.run();
      return true;
    });
    backing.getEvents().registerListener((BeforeRecordUpdateListener) record -> {
      onWrite.run();
      return true;
    });

    final Thread committer = new Thread(
        () -> database.transaction(() -> database.newDocument("Src11470").set("value", 1).save()), "writer-11470");
    committer.start();
    assertThat(committerParked.await(30, TimeUnit.SECONDS)).isTrue();
    database.transaction(() -> database.newDocument("Src11470").set("value", 2).save());
    releaseCommitter.countDown();
    committer.join(TimeUnit.SECONDS.toMillis(30));

    // two handed-off passes of two rows each, and the other work
    final long deadline = System.currentTimeMillis() + 30_000;
    while (events.stream().filter("scheduler-write"::equals).count() < 4 && System.currentTimeMillis() < deadline)
      Thread.sleep(50);

    assertThat(events).as("the other work ran between the two handed-off passes, not after both").containsSubsequence(
        "scheduler-write", "scheduler-write", "other-work", "scheduler-write", "scheduler-write");
  }

  /** A node that stopped being the leader gives the handed-off refresh up: it releases the ownership and runs nothing. */
  @Test
  void aFollowerGivesTheHandedOffRefreshUpAndReleasesTheView() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType("Follower11470"));
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("Follower11470View")
        .withQuery("SELECT value FROM Follower11470")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());
    final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Follower11470View");
    final MaterializedViewScheduler scheduler = ((LocalSchema) database.getSchema()).getMaterializedViewScheduler();
    final String statusBefore = view.getStatus();

    final DatabaseInternal follower = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if (method.getName().equals("isFollower"))
            return Boolean.TRUE;
          throw new UnsupportedOperationException(method.getName());
        });

    assertThat(view.tryBeginRefresh()).isTrue();
    assertThat(view.markRefreshPendingIfRunning()).isTrue();
    final AtomicBoolean ran = new AtomicBoolean();
    assertThat(scheduler.handOff(follower, view, () -> ran.set(true))).isTrue();

    final long deadline = System.currentTimeMillis() + 30_000;
    boolean released = false;
    while (!released && System.currentTimeMillis() < deadline) {
      released = view.tryBeginRefresh();
      if (!released)
        Thread.sleep(20);
    }
    assertThat(released).as("the ownership was released, so a later refresh can run").isTrue();
    assertThat(ran.get()).as("a follower does not refresh").isFalse();
    assertThat(view.getStatus()).as("the status stays as the last pass set it").isEqualTo(statusBefore);
    view.endRefresh();
  }
}
