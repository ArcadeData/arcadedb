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

  @Test
  void defaultBufferByteLimitIs4MB() {
    final ContextConfiguration config = new ContextConfiguration();
    final RaftProperties props = RaftPropertiesBuilder.build(config);
    assertThat(RaftServerConfigKeys.Log.Appender.bufferByteLimit(props).getSizeInt())
        .isEqualTo(4 * 1024 * 1024);
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
}
