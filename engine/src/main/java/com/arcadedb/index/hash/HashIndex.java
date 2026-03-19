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
package com.arcadedb.index.hash;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.RWLockContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Extendible hash index for O(1) equality lookups.
 * <p>
 * Does NOT support ordered iteration / range queries.
 * Backed by {@link HashIndexBucket} which manages the actual paginated file.
 */
public class HashIndex implements IndexInternal {
  private static final IndexCursor EMPTY_CURSOR = new EmptyIndexCursor();

  private final String                        name;
  private final RWLockContext                  lock   = new RWLockContext();
  protected final AtomicReference<INDEX_STATUS> status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);
  private TypeIndex                            typeIndex;
  private boolean                              valid  = true;
  private IndexMetadata                        metadata;
  protected HashIndexBucket                    bucket;

  // ─── FACTORY HANDLERS ────────────────────────────────────

  public static class HashIndexFactoryHandler implements IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder<?> builder) {
      // Use 64KB default page size for hash indexes (vs 256KB for LSM)
      final int pageSize = builder.getPageSize() == LSMTreeIndexAbstract.DEF_PAGE_SIZE ?
          HashIndexBucket.DEF_PAGE_SIZE : builder.getPageSize();
      return new HashIndex(builder.getDatabase(), builder.getIndexName(), builder.isUnique(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE, builder.getKeyTypes(), pageSize, builder.getNullStrategy());
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      final HashIndex index = new HashIndex(database, name, true, filePath, id, mode, pageSize, version);
      return index.bucket;
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      final HashIndex index = new HashIndex(database, name, false, filePath, id, mode, pageSize, version);
      return index.bucket;
    }
  }

  // ─── CONSTRUCTORS ────────────────────────────────────────

  /**
   * Called at creation time.
   */
  public HashIndex(final DatabaseInternal database, final String name, final boolean unique, final String filePath,
      final ComponentFile.MODE mode, final Type[] keyTypes, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    try {
      this.name = name;
      this.metadata = new IndexMetadata(null, null, -1);
      this.bucket = new HashIndexBucket(this, database, name, unique, filePath, mode, keyTypes,
          pageSize > 0 ? pageSize : HashIndexBucket.DEF_PAGE_SIZE, nullStrategy);
    } catch (final IOException e) {
      throw new IndexException("Error on creating hash index '" + name + "'", e);
    }
  }

  /**
   * Called at load time.
   */
  public HashIndex(final DatabaseInternal database, final String name, final boolean unique, final String filePath,
      final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    this.name = FileUtils.encode(name, database.getSchema().getEncoding());
    this.metadata = new IndexMetadata(null, null, -1);
    this.bucket = new HashIndexBucket(this, database, name, unique, filePath, id, mode, pageSize, version);
  }

  // ─── INDEX INTERFACE ─────────────────────────────────────

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    checkIsValid();
    final Object[] convertedKeys = convertKeys(keys);

    if (getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
      Set<IndexCursorEntry> txChanges = null;
      Set<RID> removedRids = null;
      boolean hasRemoves = false;

      final Map<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> indexChanges =
          getDatabase().getTransaction().getIndexChanges().getIndexKeys(getName());
      if (indexChanges != null) {
        final Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey> values =
            indexChanges.get(new TransactionIndexContext.ComparableKey(convertedKeys));
        if (values != null) {
          for (final TransactionIndexContext.IndexKey value : values.values()) {
            if (value != null) {
              if (value.operation == TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE) {
                if (isUnique())
                  return EMPTY_CURSOR;

                hasRemoves = true;
                if (removedRids == null)
                  removedRids = new HashSet<>();
                if (value.rid != null)
                  removedRids.add(value.rid);
                continue;
              }

              if (txChanges == null)
                txChanges = new HashSet<>();

              txChanges.add(new IndexCursorEntry(convertedKeys, value.rid, 1));

              if (limit > -1 && txChanges.size() > limit)
                return new TempIndexCursor(txChanges);
            }
          }
        }
      }

      final IndexCursor result = lock.executeInReadLock(() -> getDiskResult(convertedKeys, limit));

      if (txChanges != null || hasRemoves) {
        if (txChanges == null)
          txChanges = new HashSet<>();

        while (result.hasNext()) {
          final Identifiable next = result.next();
          if (removedRids == null || !removedRids.contains(next.getIdentity()))
            txChanges.add(new IndexCursorEntry(convertedKeys, next, 1));
        }
        return new TempIndexCursor(txChanges);
      }

      return result;
    }

    return lock.executeInReadLock(() -> getDiskResult(convertedKeys, limit));
  }

  private IndexCursor getDiskResult(final Object[] convertedKeys, final int limit) {
    try {
      final List<RID> rids = bucket.get(convertedKeys, isUnique() ? 1 : limit);
      if (rids.isEmpty())
        return EMPTY_CURSOR;

      final List<IndexCursorEntry> entries = new ArrayList<>(rids.size());
      for (final RID rid : rids)
        entries.add(new IndexCursorEntry(convertedKeys, rid, 1));
      return new TempIndexCursor(entries);
    } catch (final IOException e) {
      throw new IndexException("Error on hash index lookup for '" + name + "'", e);
    }
  }

  @Override
  public void put(final Object[] keys, final RID[] rids) {
    checkIsValid();
    final Object[] convertedKeys = convertKeys(keys);

    if (getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
      final TransactionContext tx = getDatabase().getTransaction();
      for (final RID rid : rids)
        tx.addIndexOperation(this, TransactionIndexContext.IndexKey.IndexKeyOperation.ADD, convertedKeys, rid);
    } else
      lock.executeInReadLock(() -> {
        try {
          for (final RID rid : rids)
            bucket.put(convertedKeys, rid);
        } catch (final IOException e) {
          throw new IndexException("Error on hash index put for '" + name + "'", e);
        }
        return null;
      });
  }

  @Override
  public void remove(final Object[] keys) {
    checkIsValid();
    final Object[] convertedKeys = convertKeys(keys);

    if (getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
      getDatabase().getTransaction()
          .addIndexOperation(this, TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE, convertedKeys, null);
    else
      lock.executeInReadLock(() -> {
        try {
          bucket.remove(convertedKeys);
        } catch (final IOException e) {
          throw new IndexException("Error on hash index remove for '" + name + "'", e);
        }
        return null;
      });
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    checkIsValid();
    final Object[] convertedKeys = convertKeys(keys);

    if (getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
      getDatabase().getTransaction()
          .addIndexOperation(this, TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE, convertedKeys, rid.getIdentity());
    else
      lock.executeInReadLock(() -> {
        try {
          bucket.remove(convertedKeys, rid.getIdentity());
        } catch (final IOException e) {
          throw new IndexException("Error on hash index remove for '" + name + "'", e);
        }
        return null;
      });
  }

  @Override
  public long countEntries() {
    checkIsValid();
    try {
      return bucket.countEntries();
    } catch (final IOException e) {
      throw new IndexException("Error counting hash index entries for '" + name + "'", e);
    }
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.HASH;
  }

  @Override
  public String getTypeName() {
    return metadata.typeName;
  }

  @Override
  public List<String> getPropertyNames() {
    return metadata.propertyNames;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return bucket.nullStrategy;
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    checkIsValid();
    bucket.nullStrategy = nullStrategy;
  }

  @Override
  public boolean isUnique() {
    return bucket.isUnique();
  }

  @Override
  public int getAssociatedBucketId() {
    return metadata.associatedBucketId;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return metadata.propertyNames != null;
  }

  // ─── INDEX INTERNAL ──────────────────────────────────────

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    checkIsValid();
    final AtomicLong total = new AtomicLong();

    if (metadata.propertyNames == null || metadata.propertyNames.isEmpty())
      throw new IndexException("Cannot rebuild index '" + name + "' because metadata information are missing");

    final DatabaseInternal db = getDatabase();

    if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {
      LogManager.instance().log(this, Level.INFO, "Building hash index '%s' on %d properties...", name, metadata.propertyNames.size());

      final long startTime = System.currentTimeMillis();

      db.scanBucket(db.getSchema().getBucketById(metadata.associatedBucketId).getName(), record -> {
        db.getIndexer().addToIndex(HashIndex.this, record.getIdentity(), (Document) record);
        total.incrementAndGet();

        if (total.get() % 10_000 == 0) {
          final long elapsed = System.currentTimeMillis() - startTime;
          final double rate = total.get() / (elapsed / 1000.0);
          LogManager.instance().log(this, Level.INFO, "Building hash index '%s': processed %d records (%.0f records/sec)...",
              name, total.get(), rate);
        }

        if (total.get() % buildIndexBatchSize == 0) {
          db.getWrappedDatabaseInstance().commit();
          db.getWrappedDatabaseInstance().begin();
        }

        if (callback != null)
          callback.onDocumentIndexed((Document) record, total.get());

        return true;
      });

      final long elapsed = System.currentTimeMillis() - startTime;
      LogManager.instance().log(this, Level.INFO, "Completed building hash index '%s': processed %d records in %dms",
          name, total.get(), elapsed);

      status.set(INDEX_STATUS.AVAILABLE);
    } else
      throw new NeedRetryException("Error on building index '" + name + "' because not available");

    return total.get();
  }

  @Override
  public boolean compact() {
    // Hash indexes don't need compaction
    return false;
  }

  @Override
  public IndexMetadata getMetadata() {
    return metadata;
  }

  @Override
  public void setMetadata(final IndexMetadata metadata) {
    checkIsValid();
    this.metadata = metadata;
  }

  @Override
  public void setMetadata(final JSONObject indexJSON) {
    final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy =
        LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(
            indexJSON.getString("nullStrategy", LSMTreeIndexAbstract.NULL_STRATEGY.ERROR.name()));

    setNullStrategy(nullStrategy);

    if (indexJSON.has("typeName"))
      this.metadata.typeName = indexJSON.getString("typeName");
    if (indexJSON.has("properties")) {
      final var jsonArray = indexJSON.getJSONArray("properties");
      this.metadata.propertyNames = new ArrayList<>();
      for (int i = 0; i < jsonArray.length(); i++)
        metadata.propertyNames.add(jsonArray.getString(i));
    }
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    for (final INDEX_STATUS expectedStatus : expectedStatuses)
      if (this.status.compareAndSet(expectedStatus, newStatus))
        return true;
    return false;
  }

  @Override
  public void close() {
    checkIsValid();
    if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {
      lock.executeInWriteLock(() -> {
        if (bucket != null)
          bucket.close();
        return null;
      });
    }
  }

  @Override
  public void drop() {
    if (status.get() != INDEX_STATUS.UNAVAILABLE)
      if (!status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE))
        throw new NeedRetryException("Error on dropping index '" + name + "' because not available");

    if (bucket == null)
      return;

    lock.executeInWriteLock(() -> {
      try {
        bucket.drop();
        return null;
      } catch (final IOException e) {
        throw new IndexException("Error dropping hash index '" + name + "'", e);
      } finally {
        valid = false;
      }
    });
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("totalEntries", (long) bucket.getTotalEntries());
    stats.put("globalDepth", (long) bucket.getGlobalDepth());
    return stats;
  }

  @Override
  public int getFileId() {
    return bucket.getFileId();
  }

  @Override
  public PaginatedComponent getComponent() {
    return bucket;
  }

  @Override
  public Type[] getKeyTypes() {
    return bucket.keyTypes;
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return bucket.binaryKeyTypes;
  }

  @Override
  public List<Integer> getFileIds() {
    return Collections.singletonList(getFileId());
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    this.typeIndex = typeIndex;
  }

  @Override
  public TypeIndex getTypeIndex() {
    return typeIndex;
  }

  @Override
  public int getPageSize() {
    return bucket.getPageSize();
  }

  @Override
  public boolean isCompacting() {
    return false;
  }

  @Override
  public boolean isValid() {
    return valid;
  }

  @Override
  public boolean scheduleCompaction() {
    return false; // no compaction needed
  }

  @Override
  public String getMostRecentFileName() {
    return bucket.getName();
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("type", getType());
    json.put("bucket", getDatabase().getSchema().getBucketById(getAssociatedBucketId()).getName());
    json.put("properties", getPropertyNames());
    json.put("nullStrategy", getNullStrategy());
    json.put("unique", isUnique());
    return json;
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    if (typeIndex != null)
      return typeIndex.getAssociatedIndex();
    return null;
  }

  @Override
  public void updateTypeName(final String newTypeName) {
    metadata.typeName = newTypeName;
    if (bucket != null) {
      try {
        bucket.getComponentFile().rename(newTypeName);
      } catch (final IOException e) {
        throw new IndexException("Error on renaming index file for hash index '" + name + "'", e);
      }
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, metadata.associatedBucketId, metadata.typeName, metadata.propertyNames);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this)
      return true;

    if (!(obj instanceof HashIndex other))
      return false;

    if (!BinaryComparator.equalsString(name, other.name))
      return false;

    if (!BinaryComparator.equalsString(metadata.typeName, other.metadata.typeName))
      return false;

    if (metadata.associatedBucketId != other.metadata.associatedBucketId)
      return false;

    return metadata.propertyNames.equals(other.metadata.propertyNames);
  }

  @Override
  public String toString() {
    return name;
  }

  // ─── INTERNAL HELPERS ────────────────────────────────────

  private Object[] convertKeys(final Object[] keys) {
    if (keys != null) {
      final byte[] keyTypes = bucket.binaryKeyTypes;
      final Object[] convertedKeys = new Object[keys.length];
      for (int i = 0; i < keys.length; ++i) {
        if (keys[i] == null)
          continue;
        convertedKeys[i] = Type.convert(getDatabase(), keys[i], BinaryTypes.getClassFromType(keyTypes[i]));
      }
      return convertedKeys;
    }
    return null;
  }

  private void checkIsValid() {
    if (!valid)
      throw new IndexException("Hash index '" + name + "' is not valid. Probably has been dropped or rebuilt");
  }

  DatabaseInternal getDatabase() {
    return bucket.getDatabase();
  }
}
