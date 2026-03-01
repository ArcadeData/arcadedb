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
package com.arcadedb.server.ha.message;

import com.arcadedb.compression.CompressionFactory;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;

import java.util.*;
import java.util.logging.*;

import static com.arcadedb.serializer.BinaryTypes.TYPE_NULL;

/**
 * Forward a transaction to the Leader server to be executed. Apart from the TX content (like with TxRequest), unique keys list is
 * needed to assure the index unique constraint.
 */
public class TxForwardRequest extends TxRequestAbstract {
  private int    isolationLevelIndex;
  private int    uniqueKeysUncompressedLength;
  private Binary uniqueKeysBuffer;

  public TxForwardRequest() {
  }

  public TxForwardRequest(final DatabaseInternal database, Database.TRANSACTION_ISOLATION_LEVEL transactionIsolationLevel,
      final Map<Integer, Integer> bucketRecordDelta, final Binary bufferChanges,
      final Map<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> keysTx) {
    super(database.getName(), bucketRecordDelta, bufferChanges);
    this.isolationLevelIndex = transactionIsolationLevel.ordinal();
    writeIndexKeysToBuffer(database, keysTx);
  }

  @Override
  public void toStream(final Binary stream) {
    super.toStream(stream);
    stream.putByte((byte) isolationLevelIndex);
    stream.putInt(uniqueKeysUncompressedLength);
    stream.putBytes(uniqueKeysBuffer.getContent(), uniqueKeysBuffer.size());
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    super.fromStream(server, stream);
    isolationLevelIndex = stream.getByte();
    uniqueKeysUncompressedLength = stream.getInt();
    uniqueKeysBuffer = CompressionFactory.getDefault().decompress(new Binary(stream.getBytes()), uniqueKeysUncompressedLength);
  }

  @Override
  public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServerName, final long messageNumber) {
    final DatabaseInternal db = server.getServer().getDatabase(databaseName);
    if (!db.isOpen())
      throw new ReplicationException("Database '" + databaseName + "' is closed");

    if (db.isTransactionActive())
      throw new ReplicationException("Transaction already begun in database '" + databaseName + "'");

    try {
      final WALFile.WALTransaction walTx = readTxFromBuffer();
      final Map<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> keysTx = readIndexKeysFromBuffer(
          db);

      // FORWARDED FROM A REPLICA
      db.begin(Database.TRANSACTION_ISOLATION_LEVEL.values()[isolationLevelIndex]);
      final TransactionContext tx = db.getTransaction();

      tx.commitFromReplica(walTx, keysTx, bucketRecordDelta);

      if (db.isTransactionActive())
        throw new ReplicationException(
            "Error on committing transaction in database '" + databaseName + "': a nested transaction occurred");

    } catch (final NeedRetryException | TransactionException e) {
      return new ErrorResponse(e);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error with the execution of the forwarded message %d", e, messageNumber);
      return new ErrorResponse(e);
    }

    return new TxForwardResponse();
  }

  @Override
  public String toString() {
    return "tx-forward(" + databaseName + ")";
  }

  protected void writeIndexKeysToBuffer(final DatabaseInternal database,
      final Map<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> indexesChanges) {
    final BinarySerializer serializer = database.getSerializer();

    uniqueKeysBuffer = new Binary();

    uniqueKeysBuffer.putUnsignedNumber(indexesChanges.size());

    for (final Map.Entry<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> entry : indexesChanges.entrySet()) {
      uniqueKeysBuffer.putString(entry.getKey());
      final Map<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> indexChanges = entry.getValue();

      uniqueKeysBuffer.putUnsignedNumber(indexChanges.size());

      for (final Map.Entry<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> keyChange : indexChanges.entrySet()) {
        final TransactionIndexContext.ComparableKey entryKey = keyChange.getKey();

        uniqueKeysBuffer.putUnsignedNumber(entryKey.values.length);
        for (int k = 0; k < entryKey.values.length; ++k) {
          Object entryValue = entryKey.values[k];
          byte keyType = BinaryTypes.getTypeFromValue(entryValue, null);
          if (keyType == -1) {
            // INVALID: SKIP IT
            LogManager.instance()
                .log(BinaryTypes.class, Level.WARNING,
                    "Cannot serialize property '%s' of type %s, value %s. The property will be ignored",
                    entryKey, entryValue.getClass(), entryValue);

            keyType = TYPE_NULL;
            entryValue = null;
          }

          uniqueKeysBuffer.putByte(keyType);
          serializer.serializeValue(database, uniqueKeysBuffer, keyType, entryValue);
        }

        final Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey> entryValue = keyChange.getValue();

        uniqueKeysBuffer.putUnsignedNumber(entryValue.size());

        for (final TransactionIndexContext.IndexKey key : entryValue.values()) {
          uniqueKeysBuffer.putByte((byte) key.operation.ordinal());
          uniqueKeysBuffer.putUnsignedNumber(key.rid.getBucketId());
          uniqueKeysBuffer.putUnsignedNumber(key.rid.getPosition());
          if (key.operation == TransactionIndexContext.IndexKey.IndexKeyOperation.REPLACE) {
            // Serialize oldRid for REPLACE entries (introduced to fix same-bucket REMOVE→ADD merge)
            final boolean hasOldRid = key.oldRid != null;
            uniqueKeysBuffer.putByte((byte) (hasOldRid ? 1 : 0));
            if (hasOldRid) {
              uniqueKeysBuffer.putUnsignedNumber(key.oldRid.getBucketId());
              uniqueKeysBuffer.putUnsignedNumber(key.oldRid.getPosition());
            }
          }
        }
      }
    }

    uniqueKeysUncompressedLength = uniqueKeysBuffer.size();
    uniqueKeysBuffer.rewind();
    uniqueKeysBuffer = CompressionFactory.getDefault().compress(uniqueKeysBuffer);
  }

  protected Map<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> readIndexKeysFromBuffer(
      final DatabaseInternal database) {
    final BinarySerializer serializer = database.getSerializer();

    uniqueKeysBuffer.position(0);

    final int totalIndexes = (int) uniqueKeysBuffer.getUnsignedNumber();

    final Map<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> indexesMap = new HashMap<>(
        totalIndexes);

    for (int indexIdx = 0; indexIdx < totalIndexes; ++indexIdx) {
      final String indexName = uniqueKeysBuffer.getString();

      final Index index = database.getSchema().getIndexByName(indexName);

      final int totalIndexEntries = (int) uniqueKeysBuffer.getUnsignedNumber();

      final TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> indexMap = new TreeMap<>();
      indexesMap.put(indexName, indexMap);

      for (int entryIndex = 0; entryIndex < totalIndexEntries; ++entryIndex) {
        // READ THE KEY
        final int keyEntryCount = (int) uniqueKeysBuffer.getUnsignedNumber();
        final Object[] keyValues = new Object[keyEntryCount];
        for (int k = 0; k < keyEntryCount; ++k) {
          final byte keyType = uniqueKeysBuffer.getByte();
          keyValues[k] = serializer.deserializeValue(database, uniqueKeysBuffer, keyType, null);
        }

        final int totalKeyEntries = (int) uniqueKeysBuffer.getUnsignedNumber();

        final Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey> values = new HashMap<>(totalKeyEntries);
        indexMap.put(new TransactionIndexContext.ComparableKey(keyValues), values);

        for (int i = 0; i < totalKeyEntries; ++i) {
          final TransactionIndexContext.IndexKey.IndexKeyOperation operation = TransactionIndexContext.IndexKey.IndexKeyOperation.values()[uniqueKeysBuffer.getByte()];

          final RID rid = new RID(database, (int) uniqueKeysBuffer.getUnsignedNumber(), uniqueKeysBuffer.getUnsignedNumber());

          final TransactionIndexContext.IndexKey v = new TransactionIndexContext.IndexKey(index.isUnique(), operation, keyValues,
              rid);
          if (operation == TransactionIndexContext.IndexKey.IndexKeyOperation.REPLACE) {
            final byte hasOldRidFlag = uniqueKeysBuffer.getByte();
            if (hasOldRidFlag == 1)
              v.oldRid = new RID(database, (int) uniqueKeysBuffer.getUnsignedNumber(), uniqueKeysBuffer.getUnsignedNumber());
          }
          values.put(v, v);
        }
      }
    }

    return indexesMap;
  }
}
