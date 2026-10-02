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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link RaftPropertiesBuilder} correctly translates ArcadeDB configuration
 * into Ratis properties. Regression for issue #4752: AppendEntries element limit must be
 * configurable and bounded to prevent follower OOM during catch-up resync.
 */
class RaftPropertiesBuilderTest {

  @Test
  void defaultElementLimitIs64() {
    final ContextConfiguration config = new ContextConfiguration();
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    assertThat(RaftServerConfigKeys.Log.Appender.bufferElementLimit(props)).isEqualTo(64);
  }

  @Test
  void customElementLimitIsApplied() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_APPEND_ELEMENT_LIMIT, 128);
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    assertThat(RaftServerConfigKeys.Log.Appender.bufferElementLimit(props)).isEqualTo(128);
  }

  /**
   * Raised from 4MB to 32MB in issue #4743: Ratis enforces this limit per ENTRY as well as per batch, so
   * at 4MB a single legitimate record (the reporter's were 4-6.5MB) could not be replicated at all - and
   * attempting it made the leader step down, then the retry toppled its successor.
   */
  @Test
  void defaultBufferByteLimitIs32MB() {
    final ContextConfiguration config = new ContextConfiguration();
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    assertThat(RaftServerConfigKeys.Log.Appender.bufferByteLimit(props).getSizeInt())
        .isEqualTo(32 * 1024 * 1024);
  }

  @Test
  void defaultWriteBufferSizeIs40MB() {
    final ContextConfiguration config = new ContextConfiguration();
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    // Coupled to appendBufferSize: Ratis requires >= appendBufferSize + 8 and preallocates it directly.
    assertThat(RaftServerConfigKeys.Log.writeBufferSize(props).getSizeInt())
        .isEqualTo(40 * 1024 * 1024);
  }

  @Test
  void defaultGrpcMessageSizeMaxIs128MB() {
    final ContextConfiguration config = new ContextConfiguration();
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    assertThat(GrpcConfigKeys.messageSizeMax(props, msg -> {}).getSizeInt())
        .isEqualTo(128 * 1024 * 1024);
  }

  @Test
  void writeBufferSizeMustExceedAppendBufferPlusFraming() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "4MB");
    config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, "8MB");
    // Should not throw
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    assertThat(RaftServerConfigKeys.Log.writeBufferSize(props).getSizeInt())
        .isEqualTo(8 * 1024 * 1024);
  }

  @Test
  void zeroElementLimitThrowsConfigurationException() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_APPEND_ELEMENT_LIMIT, 0);
    assertThatThrownBy(() -> RaftPropertiesBuilder.build(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.appendElementLimit")
        .hasMessageContaining("must be >= 1");
  }

  @Test
  void negativeElementLimitThrowsConfigurationException() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_APPEND_ELEMENT_LIMIT, -1);
    assertThatThrownBy(() -> RaftPropertiesBuilder.build(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.appendElementLimit");
  }

  @Test
  void writeBufferSmallerThanAppendBufferThrowsConfigurationException() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "8MB");
    config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, "4MB");
    assertThatThrownBy(() -> RaftPropertiesBuilder.build(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.writeBufferSize")
        .hasMessageContaining("must be >= arcadedb.ha.appendBufferSize");
  }

  // ---- issue #8672: an election timeout window with no room for jitter livelocks the election ----

  /**
   * Found running the chaos harness with the low-timeout arm of issue #8672: {@code electionTimeoutMax=5000} against the
   * default {@code electionTimeoutMin=5000} leaves no jitter, so both surviving followers of a split time out at the same
   * instant, split the vote and do it again, with no leader elected for over a minute.
   */
  @Test
  void aMaximumEqualToTheMinimumIsWidenedSoElectionsKeepTheirJitter() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX, 5000);

    final RaftProperties props = RaftPropertiesBuilder.build(config);

    assertThat(RaftServerConfigKeys.Rpc.timeoutMin(props).toLong(TimeUnit.MILLISECONDS)).isEqualTo(5000L);
    assertThat(RaftServerConfigKeys.Rpc.timeoutMax(props).toLong(TimeUnit.MILLISECONDS)).isEqualTo(10_000L);
  }

  @Test
  void aMaximumBelowTheMinimumIsWidenedToo() {
    assertThat(RaftPropertiesBuilder.effectiveElectionTimeoutMaxMs(8000, 3000)).isEqualTo(16_000);
  }

  @Test
  void aValidWindowIsLeftAlone() {
    assertThat(RaftPropertiesBuilder.effectiveElectionTimeoutMaxMs(2500, 5000)).isEqualTo(5000);
    assertThat(RaftPropertiesBuilder.effectiveElectionTimeoutMaxMs(5000, 10_000)).isEqualTo(10_000);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN, 2500);
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX, 5000);
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    assertThat(RaftServerConfigKeys.Rpc.timeoutMax(props).toLong(TimeUnit.MILLISECONDS)).isEqualTo(5000L);
  }

  /**
   * Issue #8898: Ratis's own JVM-pause monitor closes the whole division when a pause exceeds
   * {@code raft.server.close.threshold} (60s by default), behind ArcadeDB's back, and the in-place restart that
   * follows never recovers the follower. By default ArcadeDB therefore keeps that close out of reach and leaves
   * long-pause recovery to its own monitors.
   */
  @Test
  void jvmPauseCloseThresholdIsOutOfReachByDefault() {
    final RaftProperties props = RaftPropertiesBuilder.build(new ContextConfiguration());
    assertThat(RaftServerConfigKeys.closeThreshold(props).toLong(TimeUnit.DAYS)).isGreaterThanOrEqualTo(365L);
  }

  @Test
  void zeroOrNegativeJvmPauseCloseThresholdDisablesTheClose() {
    for (final long value : new long[] { 0L, -1L }) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_JVM_PAUSE_CLOSE_THRESHOLD_MS, value);
      final RaftProperties props = RaftPropertiesBuilder.build(config);
      assertThat(RaftServerConfigKeys.closeThreshold(props).toLong(TimeUnit.DAYS)).isGreaterThanOrEqualTo(365L);
    }
  }

  @Test
  void jvmPauseCloseThresholdCanBeRestoredToACustomValue() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_JVM_PAUSE_CLOSE_THRESHOLD_MS, 90_000L);
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    assertThat(RaftServerConfigKeys.closeThreshold(props).toLong(TimeUnit.MILLISECONDS)).isEqualTo(90_000L);
  }
}
