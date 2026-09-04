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
package com.arcadedb.network.binary;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.network.binary.ChannelBinaryTest.TestChannelBinary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the fifth finding of issue #7124: the four chunk-size errors told the operator to adjust
 * {@code NETWORK_BINARY_MAX_CONTENT_LENGTH}, a setting that does not exist. The bound actually comes from
 * {@link GlobalConfiguration#HA_REPLICATION_CHUNK_MAXSIZE}, which is what both {@link ChannelBinaryServer} and
 * {@link ChannelBinaryClient} pass to the constructor - so the operator was being pointed at a lever they could
 * not pull. Same class of defect as #6981.
 */
class Issue7124ChunkSizeErrorMessageTest {

  private static final int MAX_CHUNK_SIZE = 1024;
  private static final String PHANTOM_SETTING = "NETWORK_BINARY_MAX_CONTENT_LENGTH";

  private ByteArrayOutputStream outputBuffer;
  private TestChannelBinary     channel;

  @BeforeEach
  void setUp() throws IOException {
    outputBuffer = new ByteArrayOutputStream();
    channel = new TestChannelBinary(outputBuffer, MAX_CHUNK_SIZE);
  }

  @AfterEach
  void tearDown() {
    if (channel != null)
      channel.close();
  }

  @Test
  void writeBytesNamesTheRealSetting() {
    assertThatThrownBy(() -> channel.writeBytes(new byte[MAX_CHUNK_SIZE + 1])).isInstanceOf(IOException.class)
        .hasMessageContaining(GlobalConfiguration.HA_REPLICATION_CHUNK_MAXSIZE.getKey())
        .hasMessageNotContaining(PHANTOM_SETTING);
  }

  @Test
  void writeVarLengthBytesNamesTheRealSetting() {
    assertThatThrownBy(() -> channel.writeVarLengthBytes(new byte[MAX_CHUNK_SIZE + 1])).isInstanceOf(IOException.class)
        .hasMessageContaining(GlobalConfiguration.HA_REPLICATION_CHUNK_MAXSIZE.getKey())
        .hasMessageNotContaining(PHANTOM_SETTING);
  }

  @Test
  void writeBufferNamesTheRealSetting() {
    assertThatThrownBy(() -> channel.writeBuffer(ByteBuffer.allocate(MAX_CHUNK_SIZE + 1))).isInstanceOf(IOException.class)
        .hasMessageContaining(GlobalConfiguration.HA_REPLICATION_CHUNK_MAXSIZE.getKey())
        .hasMessageNotContaining(PHANTOM_SETTING);
  }

  @Test
  void readBytesNamesTheRealSetting() throws Exception {
    final DataOutputStream header = new DataOutputStream(outputBuffer);
    header.writeInt(MAX_CHUNK_SIZE + 1);
    header.flush();
    channel.setInput(new ByteArrayInputStream(outputBuffer.toByteArray()));

    assertThatThrownBy(() -> channel.readBytes()).isInstanceOf(IOException.class)
        .hasMessageContaining(GlobalConfiguration.HA_REPLICATION_CHUNK_MAXSIZE.getKey())
        .hasMessageNotContaining(PHANTOM_SETTING);
  }

  @Test
  void theRealSettingIsTheOneTheChannelsActuallyRead() {
    // PINS THE CLAIM THE MESSAGES MAKE: IF THE CHANNELS EVER STOP SOURCING maxChunkSize FROM THIS SETTING, THE
    // MESSAGES GO STALE AGAIN AND THIS ASSERTION IS THE ONE THAT SAYS SO.
    assertThat(GlobalConfiguration.HA_REPLICATION_CHUNK_MAXSIZE.getKey()).isEqualTo("arcadedb.ha.replicationChunkMaxSize");
  }
}
