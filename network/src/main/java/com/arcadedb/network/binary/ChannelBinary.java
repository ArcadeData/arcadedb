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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.logging.*;

/**
 * Abstract representation of a channel.
 **/
public abstract class ChannelBinary extends Channel implements ChannelDataInput, ChannelDataOutput {
  private static final int              MAX_LENGTH_DEBUG = 150;
  private final        int              maxChunkSize;
  protected            DataInputStream  in;
  protected            DataOutputStream out;

  public ChannelBinary(final Socket iSocket, final int chunkMaxSize) throws IOException {
    super(iSocket);

    maxChunkSize = chunkMaxSize;
  }

  public boolean inputHasData() {
    if (in != null)
      try {
        return in.available() > 0;
      } catch (final IOException e) {
        // RETURN FALSE
      }
    return false;
  }

  public byte readByte() throws IOException {
    updateMetricReceivedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return in.readByte();
  }

  public int readUnsignedByte() throws IOException {
    updateMetricReceivedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return in.readUnsignedByte();
  }

  public boolean readBoolean() throws IOException {
    updateMetricReceivedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return in.readBoolean();
  }

  public int readInt() throws IOException {
    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE);
    return in.readInt();
  }

  public long readUnsignedInt() throws IOException {
    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE);
    return in.readInt() & 0xffffffffL;
  }

  public long readLong() throws IOException {
    updateMetricReceivedBytes(Binary.LONG_SERIALIZED_SIZE);
    return in.readLong();
  }

  public short readShort() throws IOException {
    updateMetricReceivedBytes(Binary.SHORT_SERIALIZED_SIZE);
    return in.readShort();
  }

  public int readUnsignedShort() throws IOException {
    updateMetricReceivedBytes(Binary.SHORT_SERIALIZED_SIZE);
    return in.readUnsignedShort();
  }

  public String readString() throws IOException {
    final int len = in.readInt();
    if (len < 0)
      return null;

    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE + len);

    return new String(tmp, StandardCharsets.UTF_8);
  }

  public void readBytes(final byte[] buffer) throws IOException {
    in.readFully(buffer);
  }

  public byte[] readBytes() throws IOException {
    final int len = in.readInt();
    if (len > maxChunkSize) {
      throw new IOException(
          "Impossible to read a chunk of length:" + len + " max allowed chunk length:" + maxChunkSize + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
    }
    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE + len);

    if (len < 0)
      return null;

    // REUSE STATIC BUFFER?
    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    return tmp;
  }

  public RID readRID(final Database database) throws IOException {
    final int bucketId = readInt();
    final long clusterPosition = readLong();
    return new RID(database, bucketId, clusterPosition);
  }

  public int readVersion() throws IOException {
    return readInt();
  }

  public ChannelBinary writeByte(final byte iContent) throws IOException {
    out.write(iContent);
    updateMetricTransmittedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeBoolean(final boolean iContent) throws IOException {
    out.writeBoolean(iContent);
    updateMetricTransmittedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeInt(final int iContent) throws IOException {
    out.writeInt(iContent);
    updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeUnsignedInt(final int iContent) throws IOException {
    out.writeInt((int) Integer.toUnsignedLong(iContent));
    updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeLong(final long iContent) throws IOException {
    out.writeLong(iContent);
    updateMetricTransmittedBytes(Binary.LONG_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeShort(final short iContent) throws IOException {
    out.writeShort(iContent);
    updateMetricTransmittedBytes(Binary.SHORT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeUnsignedShort(final short iContent) throws IOException {
    out.writeShort(Short.toUnsignedInt(iContent));
    updateMetricTransmittedBytes(Binary.SHORT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeString(final String iContent) throws IOException {
    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    } else {
      final byte[] buffer = iContent.getBytes(StandardCharsets.UTF_8);
      out.writeInt(buffer.length);
      out.write(buffer, 0, buffer.length);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE + buffer.length);
    }

    return this;
  }

  public ChannelBinary writeVarLengthBytes(final byte[] iContent) throws IOException {
    return writeVarLengthBytes(iContent, iContent != null ? iContent.length : 0);
  }

  public ChannelBinary writeVarLengthBytes(final byte[] iContent, final int iLength) throws IOException {
    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    } else {
      if (iLength > maxChunkSize) {
        throw new IOException("Impossible to write a chunk of " + iLength + " bytes. Max allowed chunk is " + maxChunkSize
            + " bytes. See NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
      }

      out.writeInt(iLength);
      out.write(iContent, 0, iLength);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE + iLength);
    }
    return this;
  }

  public ChannelBinary writeBytes(final byte[] content) throws IOException {
    if (content != null) {
      final int length = content.length;
      if (length > maxChunkSize) {
        throw new IOException("Impossible to write a chunk of " + length + " bytes. Max allowed chunk is " + maxChunkSize
            + " bytes. See NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
      }

      out.write(content, 0, length);
      updateMetricTransmittedBytes(length);
    }
    return this;
  }

  public void writeRID(final RID iRID) throws IOException {
    writeInt(iRID.getBucketId());
    writeLong(iRID.getPosition());
  }

  public void writeVersion(final int version) throws IOException {
    writeInt(version);
  }

  public void clearInput() throws IOException {
    if (in == null)
      return;

    final StringBuilder dirtyBuffer = new StringBuilder(MAX_LENGTH_DEBUG);
    int i = 0;
    while (in.available() > 0) {
      final char c = (char) in.read();
      ++i;

      if (dirtyBuffer.length() < MAX_LENGTH_DEBUG)
        dirtyBuffer.append(c);
    }
    updateMetricReceivedBytes(i);

    final String message = "Received unread response from " + socket.getRemoteSocketAddress()
        + " probably corrupted data from the network connection. Cleared dirty data in the buffer (" + i + " bytes): [" + dirtyBuffer + (
        i > dirtyBuffer.length() ? "..." : "") + "]";
    LogManager.instance().log(this, Level.SEVERE, message);
    throw new IOException(message);

  }

  @Override
  public void flush() throws IOException {
    updateMetricFlushes();

    if (out != null)
      // IT ALREADY CALL THE UNDERLYING FLUSH
      out.flush();
    else
      super.flush();
  }

  @Override
  public synchronized void close() {
    try {
      if (in != null) {
        in.close();
      }
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.FINE, "Error during closing of input stream", e);
    }

    try {
      if (out != null) {
        out.close();
      }
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.FINE, "Error during closing of output stream", e);
    }

    super.close();
  }

  public DataOutputStream getDataOutput() {
    return out;
  }

  public DataInputStream getDataInput() {
    return in;
  }

  public ChannelBinary writeBuffer(final ByteBuffer buffer) throws IOException {
    final int length = buffer.limit();

    if (length > maxChunkSize)
      throw new IOException("Impossible to write a chunk of " + length + " bytes max allowed chunk is " + maxChunkSize
          + " bytes. See NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");

    out.write(buffer.array(), buffer.arrayOffset(), length);
    updateMetricTransmittedBytes(length);

    return this;
  }
}
