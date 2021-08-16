/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.network.binary;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Level;

/**
 * Abstract representation of a channel.
 **/
public abstract class ChannelBinary extends Channel implements ChannelDataInput, ChannelDataOutput {
  private static final int              MAX_LENGTH_DEBUG = 150;
  protected final      boolean          debug;
  private final        int              maxChunkSize;
  protected            DataInputStream  in;
  protected            DataOutputStream out;

  public ChannelBinary(final Socket iSocket, int chunkMaxSize) throws IOException {
    super(iSocket);

    maxChunkSize = chunkMaxSize;
    debug = false;

    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Connected", null, socket.getRemoteSocketAddress());
  }

  public boolean inputHasData() {
    if (in != null)
      try {
        return in.available() > 0;
      } catch (IOException e) {
        // RETURN FALSE
      }
    return false;
  }

  public byte readByte() throws IOException {
    updateMetricReceivedBytes(Binary.BYTE_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading byte (1 byte)...", null, socket.getRemoteSocketAddress());
      final byte value = in.readByte();
      LogManager.instance().log(this, Level.INFO, "%s - Read byte: %d", null, socket.getRemoteSocketAddress(), (int) value);
      return value;
    }

    return in.readByte();
  }

  public int readUnsignedByte() throws IOException {
    updateMetricReceivedBytes(Binary.BYTE_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading byte (1 byte)...", null, socket.getRemoteSocketAddress());
      final int value = in.readUnsignedByte();
      LogManager.instance().log(this, Level.INFO, "%s - Read byte: %d", null, socket.getRemoteSocketAddress(), (int) value);
      return value;
    }

    return in.readUnsignedByte();
  }

  public boolean readBoolean() throws IOException {
    updateMetricReceivedBytes(Binary.BYTE_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading boolean (1 byte)...", null, socket.getRemoteSocketAddress());
      final boolean value = in.readBoolean();
      LogManager.instance().log(this, Level.INFO, "%s - Read boolean: %b", null, socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readBoolean();
  }

  public int readInt() throws IOException {
    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading int (4 bytes)...", null, socket.getRemoteSocketAddress());
      final int value = in.readInt();
      LogManager.instance().log(this, Level.INFO, "%s - Read int: %d", null, socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readInt();
  }

  public long readUnsignedInt() throws IOException {
    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading unsigned int (4 bytes)...", null, socket.getRemoteSocketAddress());
      final long value = in.readInt() & 0xffffffffl;
      LogManager.instance().log(this, Level.INFO, "%s - Read unsigned int: %d", null, socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readInt() & 0xffffffffl;
  }

  public long readLong() throws IOException {
    updateMetricReceivedBytes(Binary.LONG_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading long (8 bytes)...", null, socket.getRemoteSocketAddress());
      final long value = in.readLong();
      LogManager.instance().log(this, Level.INFO, "%s - Read long: %d", null, socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readLong();
  }

  public short readShort() throws IOException {
    updateMetricReceivedBytes(Binary.SHORT_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading short (2 bytes)...", null, socket.getRemoteSocketAddress());
      final short value = in.readShort();
      LogManager.instance().log(this, Level.INFO, "%s - Read short: %d", null, socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readShort();
  }

  public int readUnsignedShort() throws IOException {
    updateMetricReceivedBytes(Binary.SHORT_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading ushort (2 bytes)...", null, socket.getRemoteSocketAddress());
      final int value = in.readUnsignedShort();
      LogManager.instance().log(this, Level.INFO, "%s - Read ushort: %d", null, socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readUnsignedShort();
  }

  public String readString() throws IOException {
    if (debug) {
      LogManager.instance().log(this, Level.INFO, "%s - Reading string (4+N bytes)...", null, socket.getRemoteSocketAddress());
      final int len = in.readInt();
      if (len < 0)
        return null;

      // REUSE STATIC BUFFER?
      final byte[] tmp = new byte[len];
      in.readFully(tmp);

      updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE + len);

      final String value = new String(tmp, "UTF-8");
      LogManager.instance().log(this, Level.INFO, "%s - Read string: %s", null, socket.getRemoteSocketAddress(), value);
      return value;
    }

    final int len = in.readInt();
    if (len < 0)
      return null;

    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE + len);

    return new String(tmp, "UTF-8");
  }

  public void readBytes(final byte[] buffer) throws IOException {
    in.readFully(buffer);
  }

  public byte[] readBytes() throws IOException {
    if (debug)
      LogManager.instance()
          .log(this, Level.INFO, "%s - Reading chunk of bytes. Reading chunk length as int (4 bytes)...", null, socket.getRemoteSocketAddress());

    final int len = in.readInt();
    if (len > maxChunkSize) {
      throw new IOException(
          "Impossible to read a chunk of length:" + len + " max allowed chunk length:" + maxChunkSize + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
    }
    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE + len);

    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Read chunk length: %d", null, socket.getRemoteSocketAddress(), len);

    if (len < 0)
      return null;

    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Reading %d bytes...", null, socket.getRemoteSocketAddress(), len);

    // REUSE STATIC BUFFER?
    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Read %d bytes: %s", null, socket.getRemoteSocketAddress(), len, new String(tmp));

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
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing byte (1 byte): %d", null, socket.getRemoteSocketAddress(), iContent);

    out.write(iContent);
    updateMetricTransmittedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeBoolean(final boolean iContent) throws IOException {
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing boolean (1 byte): %b", null, socket.getRemoteSocketAddress(), iContent);

    out.writeBoolean(iContent);
    updateMetricTransmittedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeInt(final int iContent) throws IOException {
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing int (4 bytes): %d", null, socket.getRemoteSocketAddress(), iContent);

    out.writeInt(iContent);
    updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeUnsignedInt(final int iContent) throws IOException {
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing uint (4 bytes): %d", null, socket.getRemoteSocketAddress(), iContent);

    out.writeInt((int) Integer.toUnsignedLong(iContent));
    updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeLong(final long iContent) throws IOException {
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing long (8 bytes): %d", null, socket.getRemoteSocketAddress(), iContent);

    out.writeLong(iContent);
    updateMetricTransmittedBytes(Binary.LONG_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeShort(final short iContent) throws IOException {
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing short (2 bytes): %d", null, socket.getRemoteSocketAddress(), iContent);

    out.writeShort(iContent);
    updateMetricTransmittedBytes(Binary.SHORT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeUnsignedShort(final short iContent) throws IOException {
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing ushort (2 bytes): %d", null, socket.getRemoteSocketAddress(), iContent);

    out.writeShort(Short.toUnsignedInt(iContent));
    updateMetricTransmittedBytes(Binary.SHORT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeString(final String iContent) throws IOException {
    if (debug)
      LogManager.instance()
          .log(this, Level.INFO, "%s - Writing string (4+%d=%d bytes): %s", null, socket.getRemoteSocketAddress(), iContent != null ? iContent.length() : 0,
              iContent != null ? iContent.length() + 4 : 4, iContent);

    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    } else {
      final byte[] buffer = iContent.getBytes("UTF-8");
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
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing bytes (4+%d=%d bytes): %s", null, socket.getRemoteSocketAddress(), iLength, iLength + 4,
          Arrays.toString(iContent));

    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    } else {
      if (iLength > maxChunkSize) {
        throw new IOException("Impossible to write a chunk of length:" + iLength + " max allowed chunk length:" + maxChunkSize
            + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
      }

      out.writeInt(iLength);
      out.write(iContent, 0, iLength);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE + iLength);
    }
    return this;
  }

  public ChannelBinary writeBytes(final byte[] content) throws IOException {
    final int length = content.length;

    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing bytes (%d bytes): %s", null, socket.getRemoteSocketAddress(), length, Arrays.toString(content));

    if (content != null) {
      if (length > maxChunkSize) {
        throw new IOException("Impossible to write a chunk of length:" + length + " max allowed chunk length:" + maxChunkSize
            + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
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
      char c = (char) in.read();
      ++i;

      if (dirtyBuffer.length() < MAX_LENGTH_DEBUG)
        dirtyBuffer.append(c);
    }
    updateMetricReceivedBytes(i);

    final String message = "Received unread response from " + socket.getRemoteSocketAddress()
        + " probably corrupted data from the network connection. Cleared dirty data in the buffer (" + i + " bytes): [" + dirtyBuffer + (
        i > dirtyBuffer.length() ? "..." : "") + "]";
    LogManager.instance().log(this, Level.SEVERE, message, null);
    throw new IOException(message);

  }

  @Override
  public void flush() throws IOException {
    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Flush", null, socket != null ? " null possible previous close" : socket.getRemoteSocketAddress());

    updateMetricFlushes();

    if (out != null)
      // IT ALREADY CALL THE UNDERLYING FLUSH
      out.flush();
    else
      super.flush();
  }

  @Override
  public void close() {
    if (debug)
      LogManager.instance()
          .log(this, Level.INFO, "%s - Closing socket...", null, socket != null ? " null possible previous close" : socket.getRemoteSocketAddress());

    try {
      if (in != null) {
        in.close();
      }
    } catch (IOException e) {
      LogManager.instance().log(this, Level.FINE, "Error during closing of input stream", e);
    }

    try {
      if (out != null) {
        out.close();
      }
    } catch (IOException e) {
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

    if (debug)
      LogManager.instance().log(this, Level.INFO, "%s - Writing bytes (%d bytes) from DirectBuffer", null, socket.getRemoteSocketAddress(), length);

    if (length > maxChunkSize)
      throw new IOException(
          "Impossible to write a chunk of length:" + length + " max allowed chunk length:" + maxChunkSize + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");

    out.write(buffer.array(), buffer.arrayOffset(), length);
    updateMetricTransmittedBytes(length);

    return this;
  }
}
