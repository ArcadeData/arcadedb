/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8390: {@link RuntimeJoinDetector#onConfiguration} bumped the state version under the
 * monitor but assigned the peer name only after releasing it. A concurrent install on the other Ratis call site could
 * then persist the bumped version with a {@code null} peer, and the arming thread's own write, finding that version
 * already persisted, wrote nothing: the marker named {@code peer=null} until some later change bumped the version.
 * <p>
 * The interleaving is forced deterministically: the arming thread is parked in the INFO log line it emits between
 * leaving the monitor and calling {@code persist()}, while the test thread records an install.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8390RuntimeJoinMarkerPeerTest {

  private static final RaftPeerId SELF   = RaftPeerId.valueOf("arcadedb-3");
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("arcadedb-1");

  @TempDir
  File tempDir;

  @Test
  void aConcurrentInstallPersistsTheArmedPeerNotNull() throws Exception {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, true);

    final CountDownLatch armLogged = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Logger original = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new ParkingLogger(original, detector, armLogged, release));
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread arming = new Thread(() -> {
      try {
        detector.onConfiguration(SELF, List.of(LEADER, SELF), List.of(LEADER), 5L);
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "issue8390-arming");
    try {
      arming.start();
      assertThat(armLogged.await(30, TimeUnit.SECONDS)).as("the arming thread reached its log line").isTrue();

      // The other Ratis call site, while the arming thread is between the version bump and its own persist().
      detector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, 10L);
    } finally {
      release.countDown();
      arming.join(30_000);
      LogManager.instance().setLogger(original);
    }

    assertThat(arming.isAlive()).isFalse();
    assertThat(failure.get()).isNull();
    assertThat(detector.hasJoinedAtRuntime()).isTrue();
    assertThat(peerLine(marker)).isEqualTo("peer=" + SELF);
  }

  /** A guard around the fix rather than a regression test: it passes before the fix too. */
  @Test
  void aRestartReadsBackThePeerTheArmWrote() throws IOException {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    final RuntimeJoinDetector before = new RuntimeJoinDetector(marker, true);
    before.onConfiguration(SELF, List.of(LEADER, SELF), List.of(LEADER), 5L);
    assertThat(peerLine(marker)).isEqualTo("peer=" + SELF);

    // A restarted node rewrites the marker on its next change: the restored peer must survive it.
    final RuntimeJoinDetector after = new RuntimeJoinDetector(marker, true);
    after.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, 12L);

    assertThat(peerLine(marker)).isEqualTo("peer=" + SELF);
  }

  private static String peerLine(final File marker) throws IOException {
    return Files.readAllLines(marker.toPath(), StandardCharsets.UTF_8).stream()
        .filter(l -> l.startsWith("peer="))
        .findFirst()
        .orElse(null);
  }

  /** Parks the thread that logs the detector's arm message until the test releases it; forwards everything. */
  private static final class ParkingLogger implements Logger {
    private final Logger         delegate;
    private final Object         requester;
    private final CountDownLatch reached;
    private final CountDownLatch release;

    ParkingLogger(final Logger delegate, final Object requester, final CountDownLatch reached,
        final CountDownLatch release) {
      this.delegate = delegate;
      this.requester = requester;
      this.reached = reached;
      this.release = release;
    }

    private void park(final Object iRequester, final String message) {
      // Matches the INFO line RuntimeJoinDetector.onConfiguration logs on the first arm. If that text is reworded,
      // update it here too: otherwise the test fails on the armLogged await instead of reaching the race.
      if (iRequester != requester || message == null || !message.contains("was added to the Raft configuration while running"))
        return;
      reached.countDown();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
        final Object arg17) {
      park(iRequester, iMessage);
      delegate.log(iRequester, iLevel, iMessage, iException, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
        final String context, final Object... args) {
      park(iRequester, iMessage);
      delegate.log(iRequester, iLevel, iMessage, iException, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
