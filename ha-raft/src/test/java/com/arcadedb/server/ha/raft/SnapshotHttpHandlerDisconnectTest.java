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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link SnapshotHttpHandler#isClientDisconnect} recognizes the benign follower-closed-the-
 * connection cases (so they are logged quietly) and does not misclassify genuine server-side errors.
 */
class SnapshotHttpHandlerDisconnectTest {

  @Test
  void brokenPipeIsAClientDisconnect() {
    assertThat(SnapshotHttpHandler.isClientDisconnect(new IOException("Broken pipe"))).isTrue();
  }

  @Test
  void connectionResetIsAClientDisconnect() {
    assertThat(SnapshotHttpHandler.isClientDisconnect(new IOException("Connection reset by peer"))).isTrue();
  }

  @Test
  void closedChannelIsAClientDisconnect() {
    assertThat(SnapshotHttpHandler.isClientDisconnect(new ClosedChannelException())).isTrue();
  }

  @Test
  void nestedBrokenPipeIsAClientDisconnect() {
    final Throwable wrapped = new RuntimeException("wrapper", new IOException("Broken pipe"));
    assertThat(SnapshotHttpHandler.isClientDisconnect(wrapped)).isTrue();
  }

  @Test
  void genuineIoErrorIsNotAClientDisconnect() {
    assertThat(SnapshotHttpHandler.isClientDisconnect(new IOException("No space left on device"))).isFalse();
  }

  @Test
  void unrelatedExceptionIsNotAClientDisconnect() {
    assertThat(SnapshotHttpHandler.isClientDisconnect(new NullPointerException())).isFalse();
  }

  @Test
  void nullIsNotAClientDisconnect() {
    assertThat(SnapshotHttpHandler.isClientDisconnect(null)).isFalse();
  }

  /**
   * #6116: the {@code /checksums} 500 body reports the DEEPEST cause. {@code executeInReadLock} rewraps every
   * checked exception into {@code ArcadeDBException("Error in execution in lock", cause)}, so reporting the
   * top-level message would answer a real "permission denied on file X" with a sentence about a lock.
   */
  @Test
  void theChecksumsFailureMessageComesFromTheDeepestCause() {
    final Throwable wrapped = new RuntimeException("Error in execution in lock",
        new IOException("Permission denied: /data/mydb/Doc_0.1.64.bucket"));
    assertThat(SnapshotHttpHandler.rootCauseMessage(wrapped))
        .isEqualTo("Permission denied: /data/mydb/Doc_0.1.64.bucket");
  }

  @Test
  void aFailureWithNoMessageStillReportsSomething() {
    assertThat(SnapshotHttpHandler.rootCauseMessage(new RuntimeException(new NullPointerException())))
        .isEqualTo("java.lang.NullPointerException");
  }

  /** A cause chain that loops back on itself must terminate, not hang the response thread. */
  @Test
  void aCyclicCauseChainTerminates() {
    final Throwable first = new RuntimeException("first");
    final Throwable second = new RuntimeException("second", first);
    first.initCause(second);

    assertThat(SnapshotHttpHandler.rootCauseMessage(first)).isNotNull();
  }
}
