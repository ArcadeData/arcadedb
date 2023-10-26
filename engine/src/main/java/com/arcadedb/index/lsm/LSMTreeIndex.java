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
package com.arcadedb.index.lsm;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockManager;
import com.arcadedb.utility.RWLockContext;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * LSM-Tree index implementation. It relies on a mutable index and its underlying immutable, compacted index.
 */
public class LSMTreeIndex implements RangeIndex, IndexInternal {
  private static final IndexCursor                   EMPTY_CURSOR       = new EmptyIndexCursor();
  private final        String                        name;
  private final        RWLockContext                 lock               = new RWLockContext();
  private              TypeIndex                     typeIndex;
  private              int                           associatedBucketId = -1;
  private              String                        typeName;
  protected            List<String>                  propertyNames;
  protected            LSMTreeIndexMutable           mutable;
  protected final      AtomicReference<INDEX_STATUS> status             = new AtomicReference<>(INDEX_STATUS.AVAILABLE);
  private              boolean                       valid              = true;

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
      return new LSMTreeIndex(builder.getDatabase(), builder.getIndexName(), builder.isUnique(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE, builder.getKeyTypes(), builder.getPageSize(), builder.getNullStrategy());
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      if (filePath.endsWith(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT))
        return new LSMTreeIndexCompacted(null, database, name, true, filePath, id, mode, pageSize, version);

      return new LSMTreeIndex(database, name, true, filePath, id, mode, pageSize, version).mutable;
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      if (filePath.endsWith(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT))
        return new LSMTreeIndexCompacted(null, database, name, false, filePath, id, mode, pageSize, version);

      return new LSMTreeIndex(database, name, false, filePath, id, mode, pageSize, version).mutable;
    }
  }

  /**
   * Called at creation time.
   */
  public LSMTreeIndex(final DatabaseInternal database, final String name, final boolean unique, final String filePath,
      final ComponentFile.MODE mode, final Type[] keyTypes, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    try {
      this.name = name;
      this.mutable = new LSMTreeIndexMutable(this, database, name, unique, filePath, mode, keyTypes, pageSize, nullStrategy);
    } catch (final IOException e) {
      throw new IndexException("Error on creating index '" + name + "'", e);
    }
  }

  /**
   * Called at load time (1st page only).
   */
  public LSMTreeIndex(final DatabaseInternal database, final String name, final boolean unique, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    this.name = FileUtils.encode(name, database.getSchema().getEncoding());
    this.mutable = new LSMTreeIndexMutable(this, database, name, unique, filePath, id, mode, pageSize, version);
  }

  public boolean scheduleCompaction() {
    checkIsValid();
    if (getDatabase().getPageManager().isPageFlushingSuspended())
      return false;
    return status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.COMPACTION_SCHEDULED);
  }

  public void setMetadata(final String typeName, final String[] propertyNames, final int associatedBucketId) {
    checkIsValid();
    this.typeName = typeName;
    this.propertyNames = Collections.unmodifiableList(Arrays.asList(propertyNames));
    this.associatedBucketId = associatedBucketId;
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return mutable.binaryKeyTypes;
  }

  @Override
  public List<Integer> getFileIds() {
    return lock.executeInReadLock(() -> {
      final List<Integer> ids = new ArrayList<>(2);
      ids.add(getFileId());
      if (mutable.getSubIndex() != null)
        ids.add(mutable.getSubIndex().getFileId());
      return ids;
    });
  }

  @Override
  public TypeIndex getTypeIndex() {
    return typeIndex;
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    this.typeIndex = typeIndex;
  }

  @Override
  public Type[] getKeyTypes() {
    return mutable.keyTypes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, associatedBucketId, typeName, propertyNames);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this)
      return true;

    if (!(obj instanceof LSMTreeIndex))
      return false;

    final LSMTreeIndex m2 = (LSMTreeIndex) obj;

    if (!BinaryComparator.equalsString(name, m2.name))
      return false;

    if (!BinaryComparator.equalsString(typeName, m2.typeName))
      return false;

    if (associatedBucketId != m2.associatedBucketId)
      return false;

    return propertyNames.equals(m2.propertyNames);
  }

  @Override
  public EmbeddedSchema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.LSM_TREE;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public List<String> getPropertyNames() {
    return propertyNames;
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    checkIsValid();
    if (getDatabase().getMode() == ComponentFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + getName() + "'");

    if (getDatabase().getPageManager().isPageFlushingSuspended())
      // POSTPONE COMPACTING (DATABASE BACKUP IN PROGRESS?)
      return false;

    if (!status.compareAndSet(INDEX_STATUS.COMPACTION_SCHEDULED, INDEX_STATUS.COMPACTION_IN_PROGRESS))
      // COMPACTION NOT SCHEDULED
      return false;

    try {
      return new LSMTreeIndexCompactor().compact(this);
    } catch (final TimeoutException e) {
      // IGNORE IT, WILL RETRY LATER
      return false;
    } finally {
      status.set(INDEX_STATUS.AVAILABLE);
    }
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("bucket", getDatabase().getSchema().getBucketById(getAssociatedBucketId()).getName());
    json.put("properties", getPropertyNames());
    json.put("nullStrategy", getNullStrategy());
    return json;
  }

  @Override
  public boolean isCompacting() {
    return status.get() == INDEX_STATUS.COMPACTION_IN_PROGRESS;
  }

  @Override
  public boolean isValid() {
    return valid;
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    for (INDEX_STATUS expectedStatus : expectedStatuses)
      if (this.status.compareAndSet(expectedStatus, newStatus))
        return true;
    return false;
  }

  @Override
  public void close() {
    checkIsValid();
    if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {
      lock.executeInWriteLock(() -> {
        if (mutable != null)
          mutable.close();
        return null;
      });
    } else
      throw new NeedRetryException("Error on closing index '" + name + "' because not available");
  }

  public void drop() {
    if (status.get() != INDEX_STATUS.UNAVAILABLE)
      if (!status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE))
        throw new NeedRetryException("Error on dropping index '" + name + "' because not available");

    if (mutable == null)
      return;

    lock.executeInWriteLock(() -> {
      final LSMTreeIndexCompacted subIndex = mutable.getSubIndex();
      if (subIndex != null)
        subIndex.drop();

      mutable.drop();

      valid = false;

      return null;
    });
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getMostRecentFileName() {
    return mutable.getName();
  }

  @Override
  public long countEntries() {
    checkIsValid();
    long total = 0;
    for (final IndexCursor it = iterator(true); it.hasNext(); ) {
      it.next();
      ++total;
    }
    return total;
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    checkIsValid();
    return lock.executeInReadLock(() -> new LSMTreeIndexCursor(mutable, ascendingOrder));
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    checkIsValid();
    return lock.executeInReadLock(() -> mutable.iterator(ascendingOrder, fromKeys, inclusive));
  }

  @Override
  public IndexCursor range(final boolean ascendingOrder, final Object[] beginKeys, final boolean beginKeysInclusive,
      final Object[] endKeys, final boolean endKeysInclusive) {
    checkIsValid();
    return lock.executeInReadLock(() -> mutable.range(ascendingOrder, beginKeys, beginKeysInclusive, endKeys, endKeysInclusive));
  }

  @Override
  public boolean supportsOrderedIterations() {
    return true;
  }

  @Override
  public boolean isAutomatic() {
    return propertyNames != null;
  }

  @Override
  public int getPageSize() {
    return mutable.getPageSize();
  }

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

      final Map<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> indexChanges = getDatabase().getTransaction()
          .getIndexChanges().getIndexKeys(getName());
      if (indexChanges != null) {
        final Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey> values = indexChanges.get(
            new TransactionIndexContext.ComparableKey(convertedKeys));
        if (values != null) {
          for (final TransactionIndexContext.IndexKey value : values.values()) {
            if (value != null) {
              if (!value.addOperation)
                // REMOVED
                return EMPTY_CURSOR;

              if (txChanges == null)
                txChanges = new HashSet<>();

              txChanges.add(new IndexCursorEntry(convertedKeys, value.rid, 1));

              if (limit > -1 && txChanges.size() > limit)
                // LIMIT REACHED
                return new TempIndexCursor(txChanges);
            }
          }
        }
      }

      final IndexCursor result = lock.executeInReadLock(() -> mutable.get(convertedKeys, limit));

      if (txChanges != null) {
        // MERGE SETS
        while (result.hasNext())
          txChanges.add(new IndexCursorEntry(convertedKeys, result.next(), 1));
        return new TempIndexCursor(txChanges);
      }

      return result;
    }

    return lock.executeInReadLock(() -> mutable.get(convertedKeys, limit));
  }

  @Override
  public void put(final Object[] keys, final RID[] rids) {
    checkIsValid();
    final Object[] convertedKeys = convertKeys(keys);

    if (getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
// KEY ADDED AT COMMIT TIME (IN A LOCK)
      final TransactionContext tx = getDatabase().getTransaction();
      for (final RID rid : rids)
        tx.addIndexOperation(this, true, convertedKeys, rid);
    } else
      lock.executeInReadLock(() -> {
        mutable.put(convertedKeys, rids);
        return null;
      });
  }

  @Override
  public void remove(final Object[] keys) {
    checkIsValid();
    final Object[] convertedKeys = convertKeys(keys);

    if (getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
      // KEY REMOVED AT COMMIT TIME (IN A LOCK)
      getDatabase().getTransaction().addIndexOperation(this, false, convertedKeys, null);
    else
      lock.executeInReadLock(() -> {
        mutable.remove(convertedKeys);
        return null;
      });
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    checkIsValid();
    final Object[] convertedKeys = convertKeys(keys);

    if (getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
      // KEY REMOVED AT COMMIT TIME (IN A LOCK)
      getDatabase().getTransaction().addIndexOperation(this, false, convertedKeys, rid.getIdentity());
    else
      lock.executeInReadLock(() -> {
        mutable.remove(convertedKeys, rid);
        return null;
      });
  }

  @Override
  public Map<String, Long> getStats() {
    return mutable.getStats();
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return mutable.nullStrategy;
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    checkIsValid();
    mutable.nullStrategy = nullStrategy;
  }

  @Override
  public int getFileId() {
    return mutable.getFileId();
  }

  @Override
  public boolean isUnique() {
    return mutable.isUnique();
  }

  public LSMTreeIndexMutable getMutableIndex() {
    return mutable;
  }

  @Override
  public PaginatedComponent getComponent() {
    return mutable;
  }

  @Override
  public int getAssociatedBucketId() {
    return associatedBucketId;
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    if (typeIndex != null)
      return typeIndex.getAssociatedIndex();
    return null;
  }

  @Override
  public String toString() {
    return name;
  }

  protected LSMTreeIndexMutable splitIndex(final int startingFromPage, final LSMTreeIndexCompacted compactedIndex) {
    checkIsValid();
    final DatabaseInternal database = getDatabase();
    if (database.isTransactionActive())
      throw new IllegalStateException("Cannot replace compacted index because a transaction is active");

    final int fileId = mutable.getFileId();

    final LockManager.LOCK_STATUS locked = database.getTransactionManager().tryLockFile(fileId, 0);
    if (locked == LockManager.LOCK_STATUS.NO)
      throw new IllegalStateException("Cannot replace compacted index because cannot lock index file " + fileId);

    try {
      final LSMTreeIndexMutable prevMutable = mutable;

// COPY MUTABLE PAGES TO THE NEW FILE
      final LSMTreeIndexMutable result = lock.executeInWriteLock(() -> {
        final int pageSize = mutable.getPageSize();

        final int last_ = mutable.getName().lastIndexOf('_');
        final String newName = mutable.getName().substring(0, last_) + "_" + System.nanoTime();

        final LSMTreeIndexMutable newMutableIndex = new LSMTreeIndexMutable(this, database, newName, mutable.isUnique(),
            database.getDatabasePath() + File.separator + newName, mutable.getKeyTypes(), mutable.getBinaryKeyTypes(), pageSize,
            LSMTreeIndexMutable.CURRENT_VERSION, compactedIndex);
        database.getSchema().getEmbedded().registerFile(newMutableIndex);

        final List<MutablePage> modifiedPages = new ArrayList<>(2 + mutable.getTotalPages() - startingFromPage);

        final MutablePage subIndexMainPage = compactedIndex.setCompactedTotalPages();
        modifiedPages.add(database.getPageManager().updatePageVersion(subIndexMainPage, false));

// KEEP METADATA AND LEAVE IT EMPTY
        final MutablePage rootPage = newMutableIndex.createNewPage();
        modifiedPages.add(database.getPageManager().updatePageVersion(rootPage, true));

        newMutableIndex.setPageCount(1);

        for (int i = 0; i < mutable.getTotalPages() - startingFromPage; ++i) {
          final BasePage currentPage = database.getTransaction()
              .getPage(new PageId(mutable.getFileId(), i + startingFromPage), pageSize);

// COPY THE ENTIRE PAGE TO THE NEW INDEX
          final MutablePage newPage = newMutableIndex.createNewPage();

          final ByteBuffer pageContent = currentPage.getContent();
          pageContent.rewind();
          newPage.getContent().put(pageContent);

          modifiedPages.add(database.getPageManager().updatePageVersion(newPage, true));
          newMutableIndex.setPageCount(i + 2);
        }

        database.getPageManager().writePages(modifiedPages, false);

        newMutableIndex.setCurrentMutablePages(newMutableIndex.getTotalPages() - 1);
        if (compactedIndex.getTotalPages() < 1)
          compactedIndex.setPageCount(1);

        // SWAP OLD WITH NEW INDEX IN EXCLUSIVE LOCK (NO READ/WRITE ARE POSSIBLE IN THE MEANTIME)
        newMutableIndex.removeTempSuffix();

        mutable = newMutableIndex;

        database.getSchema().getEmbedded().saveConfiguration();
        return newMutableIndex;
      });

      if (prevMutable != null) {
        try {
          prevMutable.drop();
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.WARNING, "Error on deleting old copy of mutable index file %s", e, prevMutable);
        }
      }

      return result;

    } finally {
      if (locked == LockManager.LOCK_STATUS.YES)
        // RELEASE THE DELETED FILE ONLY IF THE LOCK WAS ACQUIRED HERE
        database.getTransactionManager().unlockFile(fileId);
    }
  }

  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    checkIsValid();
    final AtomicLong total = new AtomicLong();

    if (propertyNames == null || propertyNames.isEmpty())
      throw new IndexException("Cannot rebuild index '" + name + "' because metadata information are missing");

    final DatabaseInternal db = getDatabase();

    if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {

      db.scanBucket(db.getSchema().getBucketById(associatedBucketId).getName(), record -> {
        db.getIndexer().addToIndex(LSMTreeIndex.this, record.getIdentity(), (Document) record);
        total.incrementAndGet();

        if (total.get() % buildIndexBatchSize == 0) {
          // CHUNK OF 100K
          db.getWrappedDatabaseInstance().commit();
          db.getWrappedDatabaseInstance().begin();
        }

        if (callback != null)
          callback.onDocumentIndexed((Document) record, total.get());

        return true;
      });

      status.set(INDEX_STATUS.AVAILABLE);

    } else
      throw new NeedRetryException("Error on building index '" + name + "' because not available");

    return total.get();
  }

  protected RWLockContext getLock() {
    return lock;
  }

  private Object[] convertKeys(final Object[] keys) {
    if (keys != null) {
      final byte[] keyTypes = mutable.binaryKeyTypes;
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
      throw new IndexException("Index '" + name + "' is not valid. Probably has been drop or rebuilt");
  }

  private DatabaseInternal getDatabase() {
    return mutable.getDatabase();
  }
}
