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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.LocalBucketTestAccess;
import com.arcadedb.engine.PageManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11755, adversarial pass: the streaming re-read of a key ({@code GetValueFromIndexEntryStep.startReread}) waits for a
 * publishing commit to end, so the record that replaced one found deleted is among the entries it reads. A single bounded wait of
 * 20 ms left a longer publication open, and the re-read then read the index before the replacement reached the cache, missing it
 * with no warning. Before #11755 a primary waited for the whole local publication through the page-manager lock.
 */
class StreamingRereadPublicationWaitTest extends TestHelper {

  private PageManager pageManager() {
    return ((DatabaseInternal) database).getPageManager();
  }

  /**
   * A replicated apply (no page-manager lock) that publishes for longer than one 20 ms wait is still waited out. Before the fix the
   * re-read sampled the sequence odd and read the key at once.
   */
  @Test
  void rereadWaitsOutAReplicatedApplyLongerThanOneWait() throws Exception {
    LocalBucketTestAccess.beginPublication((DatabaseInternal) database);
    final CountDownLatch started = new CountDownLatch(1);
    final Thread ender = new Thread(() -> {
      started.countDown();
      try {
        Thread.sleep(60);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        LocalBucketTestAccess.endPublication((DatabaseInternal) database);
      }
    });
    ender.start();
    try {
      assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
      GetValueFromIndexEntryStep.awaitPublicationBeforeReread(pageManager(), WorkGuard.forDeadline("test", Long.MAX_VALUE));
      assertThat(pageManager().getPublicationSequence() & 1).as("the re-read must start with no publication open").isZero();
    } finally {
      ender.join(30_000);
    }
  }

  /**
   * A local commit, which publishes under the page-manager lock, is waited out however long it publishes (a synchronous flush writes
   * its pages inside the publication), as it was before #11755.
   */
  @Test
  void rereadWaitsOutALocalCommitPublishingLongerThanTheBoundedWaits() throws Exception {
    final CountDownLatch published = new CountDownLatch(1);
    final Thread committer = new Thread(() -> pageManager().executeInLock(() -> {
      LocalBucketTestAccess.beginPublication((DatabaseInternal) database);
      published.countDown();
      try {
        Thread.sleep(400);
      } finally {
        LocalBucketTestAccess.endPublication((DatabaseInternal) database);
      }
      return null;
    }));
    committer.start();
    try {
      assertThat(published.await(30, TimeUnit.SECONDS)).isTrue();
      GetValueFromIndexEntryStep.awaitPublicationBeforeReread(pageManager(), WorkGuard.forDeadline("test", Long.MAX_VALUE));
      assertThat(pageManager().getPublicationSequence() & 1).as("the re-read must start once the local commit published").isZero();
    } finally {
      committer.join(30_000);
    }
  }
}
