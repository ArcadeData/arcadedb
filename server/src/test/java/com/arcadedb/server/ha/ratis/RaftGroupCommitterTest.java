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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.network.binary.ReplicationQueueFullException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RaftGroupCommitter}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftGroupCommitterTest {

  @Test
  void submitAndWaitThrowsReplicationQueueFullWhenQueueIsSaturated() throws Exception {
    // Create a committer with a tiny queue for testing. We don't start() the flusher
    // so nothing drains the queue, letting us fill it up.
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, 10);

    // Access the internal queue via reflection to fill it to capacity
    final Field queueField = RaftGroupCommitter.class.getDeclaredField("queue");
    queueField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final LinkedBlockingQueue<Object> queue = (LinkedBlockingQueue<Object>) queueField.get(committer);

    // Fill the queue to capacity (10,000 entries)
    final Field maxQueueField = RaftGroupCommitter.class.getDeclaredField("MAX_QUEUE_SIZE");
    maxQueueField.setAccessible(true);
    final int maxQueueSize = (int) maxQueueField.get(null);

    // Use dummy objects to fill the queue
    for (int i = 0; i < maxQueueSize; i++)
      queue.put(new Object());

    assertThat(queue.remainingCapacity()).isEqualTo(0);

    // Now submitAndWait should throw ReplicationQueueFullException, not QuorumNotReachedException
    assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }, 1000))
        .isInstanceOf(ReplicationQueueFullException.class)
        .hasMessageContaining("Replication queue is full");
  }
}
