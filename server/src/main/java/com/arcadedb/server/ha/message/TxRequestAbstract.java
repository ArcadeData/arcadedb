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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.CompressionFactory;
import com.arcadedb.engine.WALFile;
import com.arcadedb.server.ha.ReplicationException;

public abstract class TxRequestAbstract extends HAAbstractCommand {
  protected String databaseName;
  protected int    changesUncompressedLength;
  protected Binary changesBuffer;

  protected TxRequestAbstract() {
  }

  protected TxRequestAbstract(final String dbName, final Binary changesBuffer) {
    this.databaseName = dbName;

    changesBuffer.rewind();
    this.changesUncompressedLength = changesBuffer.size();
    this.changesBuffer = CompressionFactory.getDefault().compress(changesBuffer);
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(databaseName);
    stream.putInt(changesUncompressedLength);
    stream.putBytes(changesBuffer.getContent(), changesBuffer.size());
  }

  @Override
  public void fromStream(final Binary stream) {
    databaseName = stream.getString();
    changesUncompressedLength = stream.getInt();
    changesBuffer = CompressionFactory.getDefault().decompress(new Binary(stream.getBytes()), changesUncompressedLength);
  }

  protected WALFile.WALTransaction readTxFromBuffer() {
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    final Binary bufferChange = changesBuffer;

    int pos = 0;
    tx.txId = bufferChange.getLong(pos);
    pos += Binary.LONG_SERIALIZED_SIZE;

    tx.timestamp = bufferChange.getLong(pos);
    pos += Binary.LONG_SERIALIZED_SIZE;

    final int pages = bufferChange.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;

    final int segmentSize = bufferChange.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;

    if (pos + segmentSize + Binary.LONG_SERIALIZED_SIZE > bufferChange.size())
      // TRUNCATED FILE
      throw new ReplicationException("Replicated transaction buffer is corrupted");

    tx.pages = new WALFile.WALPage[pages];

    for (int i = 0; i < pages; ++i) {
      if (pos > bufferChange.size())
        // INVALID
        throw new ReplicationException("Replicated transaction buffer is corrupted");

      tx.pages[i] = new WALFile.WALPage();

      tx.pages[i].fileId = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      tx.pages[i].pageNumber = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      tx.pages[i].changesFrom = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      tx.pages[i].changesTo = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final int deltaSize = tx.pages[i].changesTo - tx.pages[i].changesFrom + 1;

      tx.pages[i].currentPageVersion = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      tx.pages[i].currentPageSize = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final byte[] buffer = new byte[deltaSize];

      tx.pages[i].currentContent = new Binary(buffer);
      bufferChange.getByteArray(pos, buffer, 0, deltaSize);

      pos += deltaSize;
    }

    final long mn = bufferChange.getLong(pos + Binary.INT_SERIALIZED_SIZE);
    if (mn != WALFile.MAGIC_NUMBER)
      // INVALID
      throw new ReplicationException("Replicated transaction buffer is corrupted");

    return tx;
  }

}
