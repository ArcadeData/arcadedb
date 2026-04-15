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

import com.arcadedb.network.binary.QuorumNotReachedException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RaftGroupCommitterTest {

  @Test
  void stopDrainsQueueWithErrors() {
    // Create a committer with no RaftClient (null) - entries will fail on flush
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 10_000);

    // Submit an entry in a background thread
    final var future = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
      try {
        committer.submitAndWait(new byte[] { 1, 2, 3 }, 5_000);
        return "success";
      } catch (final QuorumNotReachedException e) {
        return "failed: " + e.getMessage();
      }
    });

    // Give the background thread time to enqueue
    try {
      Thread.sleep(200);
    } catch (final InterruptedException ignored) {
    }

    // Stop should drain the queue and complete all futures with errors
    committer.stop();

    final String result = future.join();
    assertThat(result).startsWith("failed:");
  }

  @Test
  void allQuorumWatchFailureThrowsMajorityCommittedException() {
    final var ex = new MajorityCommittedAllFailedException("ALL quorum watch failed");
    assertThat(ex).isInstanceOf(QuorumNotReachedException.class);
    assertThat(ex.getMessage()).contains("ALL quorum");
  }
}
