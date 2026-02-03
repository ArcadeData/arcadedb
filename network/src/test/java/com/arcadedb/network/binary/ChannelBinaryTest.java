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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChannelBinaryTest {

  private TestChannelBinary channel;
  private ByteArrayOutputStream outputBuffer;
  private DatabaseInternal database;

  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    outputBuffer = new ByteArrayOutputStream();
    channel = new TestChannelBinary(outputBuffer, 1024 * 1024);

    // Create a real database for RID tests
    database = (DatabaseInternal) new DatabaseFactory(tempDir.resolve("testdb").toString()).create();
  }

  @AfterEach
  void tearDown() {
    if (channel != null)
      channel.close();
    if (database != null)
      database.close();
  }

  @Test
  void writeAndReadByte() throws Exception {
    channel.writeByte((byte) 42);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readByte()).isEqualTo((byte) 42);
  }

  @Test
  void writeAndReadBoolean() throws Exception {
    channel.writeBoolean(true);
    channel.writeBoolean(false);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readBoolean()).isTrue();
    assertThat(channel.readBoolean()).isFalse();
  }

  @Test
  void writeAndReadInt() throws Exception {
    channel.writeInt(12345);
    channel.writeInt(-67890);
    channel.writeInt(Integer.MAX_VALUE);
    channel.writeInt(Integer.MIN_VALUE);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readInt()).isEqualTo(12345);
    assertThat(channel.readInt()).isEqualTo(-67890);
    assertThat(channel.readInt()).isEqualTo(Integer.MAX_VALUE);
    assertThat(channel.readInt()).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  void writeAndReadUnsignedInt() throws Exception {
    channel.writeUnsignedInt(12345);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readUnsignedInt()).isEqualTo(12345L);
  }

  @Test
  void writeAndReadLong() throws Exception {
    channel.writeLong(123456789012345L);
    channel.writeLong(-987654321098765L);
    channel.writeLong(Long.MAX_VALUE);
    channel.writeLong(Long.MIN_VALUE);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readLong()).isEqualTo(123456789012345L);
    assertThat(channel.readLong()).isEqualTo(-987654321098765L);
    assertThat(channel.readLong()).isEqualTo(Long.MAX_VALUE);
    assertThat(channel.readLong()).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void writeAndReadShort() throws Exception {
    channel.writeShort((short) 1234);
    channel.writeShort((short) -5678);
    channel.writeShort(Short.MAX_VALUE);
    channel.writeShort(Short.MIN_VALUE);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readShort()).isEqualTo((short) 1234);
    assertThat(channel.readShort()).isEqualTo((short) -5678);
    assertThat(channel.readShort()).isEqualTo(Short.MAX_VALUE);
    assertThat(channel.readShort()).isEqualTo(Short.MIN_VALUE);
  }

  @Test
  void writeAndReadUnsignedShort() throws Exception {
    channel.writeUnsignedShort((short) 1234);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readUnsignedShort()).isEqualTo(1234);
  }

  @Test
  void writeAndReadUnsignedByte() throws Exception {
    channel.writeByte((byte) 200);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readUnsignedByte()).isEqualTo(200);
  }

  @Test
  void writeAndReadString() throws Exception {
    channel.writeString("Hello, World!");
    channel.writeString("Test with unicode: äöü ñ 中文");
    channel.writeString("");
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readString()).isEqualTo("Hello, World!");
    assertThat(channel.readString()).isEqualTo("Test with unicode: äöü ñ 中文");
    assertThat(channel.readString()).isEqualTo("");
  }

  @Test
  void writeAndReadNullString() throws Exception {
    channel.writeString(null);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readString()).isNull();
  }

  @Test
  void writeAndReadVarLengthBytes() throws Exception {
    final byte[] data = new byte[] { 1, 2, 3, 4, 5 };
    channel.writeVarLengthBytes(data);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readBytes()).isEqualTo(data);
  }

  @Test
  void writeAndReadVarLengthBytesWithLength() throws Exception {
    final byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    channel.writeVarLengthBytes(data, 5);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readBytes()).isEqualTo(new byte[] { 1, 2, 3, 4, 5 });
  }

  @Test
  void writeVarLengthBytesNull() throws Exception {
    channel.writeVarLengthBytes(null);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readBytes()).isNull();
  }

  @Test
  void writeAndReadBytes() throws Exception {
    final byte[] data = new byte[] { 10, 20, 30, 40, 50 };
    channel.writeBytes(data);
    channel.flush();

    setupInputFromOutput();
    final byte[] result = new byte[5];
    channel.readBytes(result);
    assertThat(result).isEqualTo(data);
  }

  @Test
  void writeBytesNull() throws Exception {
    channel.writeBytes(null);
    channel.flush();
    // Should not throw, just writes nothing
    assertThat(outputBuffer.size()).isEqualTo(0);
  }

  @Test
  void writeAndReadRID() throws Exception {
    final RID rid = new RID(database, 5, 12345L);
    channel.writeRID(rid);
    channel.flush();

    setupInputFromOutput();
    final RID readRid = channel.readRID(database);
    assertThat(readRid.getBucketId()).isEqualTo(5);
    assertThat(readRid.getPosition()).isEqualTo(12345L);
  }

  @Test
  void writeAndReadVersion() throws Exception {
    channel.writeVersion(42);
    channel.flush();

    setupInputFromOutput();
    assertThat(channel.readVersion()).isEqualTo(42);
  }

  @Test
  void writeBuffer() throws Exception {
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 });
    channel.writeBuffer(buffer);
    channel.flush();

    setupInputFromOutput();
    final byte[] result = new byte[5];
    channel.readBytes(result);
    assertThat(result).isEqualTo(new byte[] { 1, 2, 3, 4, 5 });
  }

  @Test
  void inputHasDataWhenEmpty() {
    assertThat(channel.inputHasData()).isFalse();
  }

  @Test
  void inputHasDataWhenDataAvailable() throws Exception {
    channel.writeByte((byte) 1);
    channel.flush();
    setupInputFromOutput();

    assertThat(channel.inputHasData()).isTrue();
  }

  @Test
  void getDataInput() {
    assertThat(channel.getDataInput()).isNotNull();
  }

  @Test
  void getDataOutput() {
    assertThat(channel.getDataOutput()).isNotNull();
  }

  @Test
  void writeBytesExceedsMaxChunkSize() {
    final byte[] largeData = new byte[2 * 1024 * 1024]; // 2MB > 1MB max
    assertThatThrownBy(() -> channel.writeBytes(largeData))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Impossible to write a chunk");
  }

  @Test
  void writeVarLengthBytesExceedsMaxChunkSize() {
    final byte[] largeData = new byte[2 * 1024 * 1024]; // 2MB > 1MB max
    assertThatThrownBy(() -> channel.writeVarLengthBytes(largeData))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Impossible to write a chunk");
  }

  @Test
  void writeBufferExceedsMaxChunkSize() {
    final ByteBuffer buffer = ByteBuffer.allocate(2 * 1024 * 1024); // 2MB > 1MB max
    assertThatThrownBy(() -> channel.writeBuffer(buffer))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Impossible to write a chunk");
  }

  @Test
  void readBytesExceedsMaxChunkSize() throws Exception {
    // Write a length header that exceeds max chunk size
    final DataOutputStream dos = new DataOutputStream(outputBuffer);
    dos.writeInt(2 * 1024 * 1024); // Write length > max
    dos.flush();

    setupInputFromOutput();

    assertThatThrownBy(() -> channel.readBytes())
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Impossible to read a chunk");
  }

  @Test
  void closeChannel() throws Exception {
    channel.writeByte((byte) 1);
    channel.close();

    // After close, input/output should be null
    assertThat(channel.getDataInput()).isNull();
    assertThat(channel.getDataOutput()).isNull();
  }

  private void setupInputFromOutput() {
    final byte[] data = outputBuffer.toByteArray();
    channel.setInput(new ByteArrayInputStream(data));
  }

  /**
   * Test implementation of ChannelBinary that uses ByteArrayStreams instead of sockets.
   */
  static class TestChannelBinary extends ChannelBinary {
    private final ByteArrayOutputStream outputBuffer;

    TestChannelBinary(final ByteArrayOutputStream outputBuffer, final int maxChunkSize) throws IOException {
      super(createMockSocket(), maxChunkSize);
      this.outputBuffer = outputBuffer;
      this.outStream = outputBuffer;
      this.out = new DataOutputStream(outputBuffer);
      // Initialize with empty input
      final ByteArrayInputStream emptyInput = new ByteArrayInputStream(new byte[0]);
      this.inStream = emptyInput;
      this.in = new DataInputStream(emptyInput);
    }

    void setInput(final ByteArrayInputStream inputStream) {
      this.inStream = inputStream;
      this.in = new DataInputStream(inputStream);
    }

    private static Socket createMockSocket() throws IOException {
      final Socket mockSocket = mock(Socket.class);
      when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
      when(mockSocket.getOutputStream()).thenReturn(new ByteArrayOutputStream());
      return mockSocket;
    }

    @Override
    public synchronized void close() {
      try {
        if (in != null)
          in.close();
        if (out != null)
          out.close();
      } catch (final IOException e) {
        // ignore
      }
      in = null;
      out = null;
      inStream = null;
      outStream = null;
    }
  }
}
