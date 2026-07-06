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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;

import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.RidHashSet;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.arcadedb.database.Binary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * Abstract class for LSM-based indexes. The first page contains 2 bytes to store key and value types. The pages are populated from the head of the page
 * with the pointers to the pair key/value that starts from the tail. A page is full when there is no space between the head
 * (key pointers) and the tail (key/value pairs).
 * <br>
 * When a page is full, another page is created, waiting for a compaction.
 * <br>
 * HEADER ROOT PAGE (1st) = [offsetFreeKeyValueContent(int:4),numberOfEntries(int:4),mutable(boolean:1),compactedPageNumberOfSeries(int:4),subIndexFileId(int:4),numberOfKeys(byte:1),keyType(byte:1)*]
 * <br>
 * HEADER Nst PAGE        = [offsetFreeKeyValueContent(int:4),numberOfEntries(int:4),mutable(boolean:1),compactedPageNumberOfSeries(int:4)]
 * <p>
 * <p>
 * The page content size and available space API are not valid in the index pages, because the whole page is used from start to end.
 */
public abstract class LSMTreeIndexAbstract extends PaginatedComponent {
  public enum NULL_STRATEGY {ERROR, SKIP, INDEX}

  public static final int DEF_PAGE_SIZE = 262_144;
  public final        RID REMOVED_ENTRY_RID;

  protected static final LSMTreeIndexCompacted.LookupResult LOWER     = new LSMTreeIndexCompacted.LookupResult(false, true, 0,
      null);
  protected static final LSMTreeIndexCompacted.LookupResult HIGHER    = new LSMTreeIndexCompacted.LookupResult(false, true, 0,
      null);
  protected static final byte                               valueType = BinaryTypes.TYPE_COMPRESSED_RID;

  protected       LSMTreeIndex     mainIndex;
  protected final BinaryComparator comparator;
  protected final BinarySerializer serializer;
  protected final boolean          unique;
  protected       Type[]           keyTypes;
  protected       byte[]           binaryKeyTypes;
  protected       boolean[]        caseInsensitiveKeys;
  protected       NULL_STRATEGY    nullStrategy       = NULL_STRATEGY.SKIP;
  protected final AtomicLong       statsAdjacentSteps = new AtomicLong();

  /**
   * When true, each posting value is serialized as {@code compressedRID + tf(varint) + docLength(varint)} instead of just the
   * RID, and is deserialized into a {@link FullTextPostingRID}. Used exclusively by full-text indexes configured with BM25 similarity.
   * Defaults to false so every other LSM index keeps the byte-identical RID-only format.
   * <p>
   * NOTE: this flag is NOT stored in the page header; it is set from the persisted schema (similarity = BM25) during the
   * single-threaded schema load, before the index serves any query or compaction is scheduled - so the write happens-before any
   * concurrent reader. The schema and index files must therefore stay consistent: reading a BM25 index's pages with the flag off
   * (or vice-versa) would misinterpret the value bytes. In normal operation schema and index files travel together.
   * <p>
   * volatile: set once (single writer) but read on every read/write path, possibly from other threads; the visibility guarantee
   * avoids a reader seeing a stale {@code false} and misparsing the value bytes. Private: accessed by subclasses only through
   * {@link #isStoreTermFrequency()} / {@link #setStoreTermFrequency(boolean)}.
   */
  private volatile boolean         storeTermFrequency = false;

  protected static class LookupResult {
    public final boolean found;
    public final boolean outside;
    public final int     keyIndex;
    public final int[]   valueBeginPositions;

    public LookupResult(final boolean found, final boolean outside, final int keyIndex, final int[] valueBeginPositions) {
      this.found = found;
      this.outside = outside;
      this.keyIndex = keyIndex;
      this.valueBeginPositions = valueBeginPositions;
    }
  }

  /**
   * Called at creation time.
   */
  protected LSMTreeIndexAbstract(final LSMTreeIndex mainIndex, final DatabaseInternal database, final String name,
      final boolean unique, final String filePath, final String ext, final ComponentFile.MODE mode, final Type[] keyTypes,
      final int pageSize, final int version, final NULL_STRATEGY nullStrategy) throws IOException {
    super(database, name, filePath, ext, mode, pageSize, version);

    if (nullStrategy == null)
      throw new IllegalArgumentException("Index null strategy is null");

    if (keyTypes == null || keyTypes.length == 0)
      throw new IllegalArgumentException("Key types empty ");

    this.mainIndex = mainIndex;
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.keyTypes = keyTypes;

    this.binaryKeyTypes = new byte[keyTypes.length];
    for (int i = 0; i < keyTypes.length; i++)
      this.binaryKeyTypes[i] = keyTypes[i].getBinaryType();

    this.nullStrategy = nullStrategy;
    REMOVED_ENTRY_RID = new RID(-1, -1L);
  }

  /**
   * Called at cloning time.
   */
  protected LSMTreeIndexAbstract(final LSMTreeIndex mainIndex, final DatabaseInternal database, final String name,
      final boolean unique, final String filePath, final String ext, final Type[] keyTypes, final byte[] binaryKeyTypes,
      final int pageSize, final int version) throws IOException {
    super(database, name, filePath, TEMP_EXT + ext, ComponentFile.MODE.READ_WRITE, pageSize, version);
    this.mainIndex = mainIndex;
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.keyTypes = keyTypes;
    this.binaryKeyTypes = binaryKeyTypes;
    REMOVED_ENTRY_RID = new RID(-1, -1L);
  }

  /**
   * Called at load time (1st page only).
   */
  protected LSMTreeIndexAbstract(final LSMTreeIndex mainIndex, final DatabaseInternal database, final String name,
      final boolean unique, final String filePath, final int id, final ComponentFile.MODE mode, final int pageSize,
      final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    this.mainIndex = mainIndex;
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    REMOVED_ENTRY_RID = new RID(-1, -1L);
  }

  /**
   * @param purpose 0 = exists, 1 = retrieve, 2 = ascending iterator, 3 = descending iterator
   */
  protected abstract LookupResult compareKey(final Binary currentPageBuffer, final int startIndexArray,
      final Object[] convertedKeys, int mid, final int count, final int purpose);

  public PaginatedComponent getPaginatedComponent() {
    return this;
  }

  public boolean isUnique() {
    return unique;
  }

  public int getFileId() {
    return file.getFileId();
  }

  public NULL_STRATEGY getNullStrategy() {
    return nullStrategy;
  }

  @Override
  public String toString() {
    return componentName + "(" + getFileId() + ")";
  }

  public Type[] getKeyTypes() {
    return keyTypes;
  }

  public byte[] getBinaryKeyTypes() {
    return binaryKeyTypes;
  }

  public boolean isDeletedEntry(final RID rid) {
    return rid.getBucketId() < 0;
  }

  public void drop() throws IOException {
    if (database.isOpen()) {
      database.getPageManager().deleteFile(database, file.getFileId());
      database.getFileManager().dropFile(file.getFileId());
      database.getSchema().getEmbedded().removeFile(file.getFileId());
    } else {
      if (!new File(file.getFilePath()).delete())
        LogManager.instance().log(this, Level.WARNING, "Error on deleting index file '%s'", null, file.getFilePath());
    }
  }

  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("pages", (long) getTotalPages());
    return stats;
  }

  @Override
  public Object getMainComponent() {
    return mainIndex;
  }

  /**
   * Checks if all the key values are NULL.
   */
  public static boolean isKeyNull(final Object[] keys) {
    if (keys == null)
      return true;

    for (int i = 0; i < keys.length; ++i)
      if (keys[i] != null)
        return false;

    return true;
  }

  /**
   * Lookups for an entry in the index by using dichotomy search.
   *
   * @param purpose 0 = exists, 1 = retrieve, 2 = ascending iterator, 3 = descending iterator
   *
   * @return LookupResult object, never null
   */
  protected LookupResult lookupInPage(final int pageNum, final int count, final Binary currentPageBuffer,
      final Object[] convertedKeys, final int purpose) {
    if (binaryKeyTypes.length == 0)
      throw new IllegalArgumentException("No key types found");

    if (convertedKeys.length > binaryKeyTypes.length)
      throw new IllegalArgumentException(
          "key is composed of " + convertedKeys.length + " items, while the index defined " + binaryKeyTypes.length + " items");

    if ((purpose == 0 || purpose == 1) && convertedKeys.length != binaryKeyTypes.length)
      throw new IllegalArgumentException(
          "key is composed of " + convertedKeys.length + " items, while the index defined " + binaryKeyTypes.length + " items");

    if (count == 0)
      // EMPTY, NOT FOUND
      return new LookupResult(false, true, 0, null);

    int low = 0;
    int high = count - 1;

    final int startIndexArray = getHeaderSize(pageNum);

    LookupResult result;

    // CHECK THE BOUNDARIES FIRST (LOWER THAN THE FIRST)
    result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, low, count, purpose);
    if (result == LOWER) {
      if (purpose == 2)
        // BROWSE ASCENDING
        return new LookupResult(false, false, low, new int[] { currentPageBuffer.position() });

      return new LookupResult(false, true, low, null);
    } else if (result != HIGHER)
      return result;

    // CHECK THE BOUNDARIES FIRST (HIGHER THAN THE LAST)
    result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, high, count, purpose);
    if (result == HIGHER) {
      if (purpose == 3)
        // BROWSE DESCENDING
        return new LookupResult(false, false, high, new int[] { currentPageBuffer.position() });

      return new LookupResult(false, true, count, null);
    } else if (result != LOWER)
      return result;

    int mid;
    while (low <= high) {
      mid = (low + high) >>> 1;

      result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, mid, count, purpose);

      if (result == HIGHER)
        low = mid + 1;
      else if (result == LOWER)
        high = mid - 1;
      else
        return result;
    }

    if (purpose == 3) {
      return new LookupResult(false, false, high, null);
    }

    return new LookupResult(false, false, low, null);
  }

  protected void writeEntrySingleValue(final Binary buffer, final Object[] keys, final Object rid, final int pageUsableSpace) {
    buffer.clear();
    writeKeys(buffer, keys);
    writeEntryValue(buffer, rid);

    if (buffer.size() > pageUsableSpace)
      throw new IndexException(
          "Key/value size (" + FileUtils.getSizeAsString(buffer.size()) + ") is too big to fit in a single page ("
              + FileUtils.getSizeAsString(pageUsableSpace) + "). Define the index with larger pages");
  }

  /**
   * Serializes the key plus as many of {@code rids} as fit into the scratch {@code buffer} and returns how many values were
   * written (0 if not even a single value fits). The caller decides what to do with a partial/zero result (e.g. flush the current
   * page and retry on a fresh one).
   * <p>
   * INVARIANT (relied upon by {@link LSMTreeIndexCompacted#appendDuringCompaction}): this method writes ONLY to {@code buffer}
   * (the scratch {@code keyValueContent}); it never touches any database page. So when it returns a partial or zero count, no
   * bytes have been committed to a page and the caller can safely move this key to a new page without orphaning data on the old
   * one. Keep this true if refactoring - the compaction continuation-page fix depends on it.
   */
  protected int writeEntryMultipleValues(final Binary buffer, final Object[] keys, Object[] rids, final int availableSpaceInPage,
      final int pageUsableSpace, final PageId pageId) {
    Object[] values = rids;
    // Termination invariant: each iteration either breaks (all values written),
    // returns (single value still doesn't fit), or strictly shrinks `values` via
    // Arrays.copyOf(values, written) where written < values.length. Therefore the
    // loop runs at most O(rids.length) times.
    do {
      buffer.clear();
      writeKeys(buffer, keys);

      if (buffer.size() > pageUsableSpace)
        throw new IndexException("Key size (" + FileUtils.getSizeAsString(buffer.size()) + ") is too big to fit in a single page ("
            + FileUtils.getSizeAsString(pageUsableSpace) + "). Define the index with larger pages");

      final int written = writeEntryValues(buffer, values, availableSpaceInPage);
      if (written == values.length)
        // ALL ENTRIES WRITTEN
        break;

      if (values.length <= 1)
        return 0;

      LogManager.instance()
          .log(this, Level.FINE, "Cannot fit %d values in index page %s, saving only %d values",
              values.length, pageId, written);

      // NOT ENOUGH SPACE: Split the array with the max number of values that fit in the page
      final int prevLen = values.length;
      values = Arrays.copyOf(values, written);
      assert values.length < prevLen : "writeEntryMultipleValues must shrink values each iteration to terminate";

    } while (true);

    return values.length;
  }

  /**
   * Reads the keys and returns the serialized size.
   */
  protected int getSerializedKeySize(final Binary buffer, final int keyLength) {
    final int startsAt = buffer.position();
    for (int keyIndex = 0; keyIndex < keyLength; ++keyIndex) {
      final boolean notNull = version < 1 || buffer.getByte() == 1;
      if (notNull)
        serializer.deserializeValue(database, buffer, binaryKeyTypes[keyIndex], null);
    }

    return buffer.position() - startsAt;
  }

  protected Object[] convertKeys(final Object[] keys, final byte[] keyTypes) {
    if (keys == null)
      return null;

    final Object[] convertedKeys = new Object[keys.length];
    for (int i = 0; i < keys.length; ++i) {
      if (keys[i] == null)
        continue;

      convertedKeys[i] = Type.convert(database, keys[i], BinaryTypes.getClassFromType(keyTypes[i]));

      if (convertedKeys[i] instanceof String string) {
        // Apply case-insensitive collation: lowercase before storing/searching
        if (caseInsensitiveKeys != null && i < caseInsensitiveKeys.length && caseInsensitiveKeys[i])
          string = string.toLowerCase(Locale.ROOT);
        // OPTIMIZATION: ALWAYS CONVERT STRINGS TO BYTE[]
        convertedKeys[i] = string.getBytes(DatabaseFactory.getDefaultCharset());
      }
    }
    return convertedKeys;
  }

  protected Object[] getPageKeyRange(final BasePage currentPage) {
    final Binary currentPageBuffer = new Binary(currentPage.slice());
    final Object[] min = getKeyInPagePosition(currentPage.getPageId().getPageNumber(), currentPageBuffer, 0);

    final int count = getCount(currentPage);

    if (count > 1)
      return new Object[] { min, getKeyInPagePosition(currentPage.getPageId().getPageNumber(), currentPageBuffer, count - 1) };
    else
      return new Object[] { min, min };
  }

  protected Object[] getKeyInPagePosition(final int pageNum, final Binary currentPageBuffer, final int position) {
    final int startIndexArray = getHeaderSize(pageNum);
    final int contentPos = currentPageBuffer.getInt(startIndexArray + (position * INT_SERIALIZED_SIZE));
    currentPageBuffer.position(contentPos);

    final Object[] key = new Object[binaryKeyTypes.length];

    for (int keyIndex = 0; keyIndex < binaryKeyTypes.length; ++keyIndex) {
      final boolean notNull = version < 1 || currentPageBuffer.getByte() == 1;
      if (notNull)
        key[keyIndex] = serializer.deserializeValue(database, currentPageBuffer, binaryKeyTypes[keyIndex], null);
      else
        key[keyIndex] = null;
    }

    return key;
  }

  protected int compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object[] keys, final int mid,
      final int count) {
    final int contentPos = currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE));
    if (contentPos < startIndexArray + (count * INT_SERIALIZED_SIZE))
      throw new IndexException("Internal error: invalid content position " + contentPos + " is < of " + (startIndexArray + (count
          * INT_SERIALIZED_SIZE)));

    currentPageBuffer.position(contentPos);

    int result = 0;
    for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
      // GET THE KEY
      final Object key = keys[keyIndex];

      final boolean notNull = version < 1 || currentPageBuffer.getByte() == 1;
      if (!notNull) {
        if (key == null)
          // BOTH NULL
          continue;
        else
          return 1;
      }

      if (key == null)
        return -1;

      if (binaryKeyTypes[keyIndex] == BinaryTypes.TYPE_STRING) {
        // OPTIMIZATION: SPECIAL CASE, LAZY EVALUATE BYTE PER BYTE THE STRING
        result = comparator.compareBytes((byte[]) key, currentPageBuffer);
      } else {
        final Object keyValue = serializer.deserializeValue(database, currentPageBuffer, binaryKeyTypes[keyIndex], null);
        result = comparator.compare(key, binaryKeyTypes[keyIndex], keyValue, binaryKeyTypes[keyIndex]);
      }

      if (result != 0)
        break;
    }

    return result;
  }

  protected int getHeaderSize(final int pageNum) {
    int size = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
    if (pageNum == 0)
      size += INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE + binaryKeyTypes.length;

    return size;
  }

  public static int compareKeys(final BinaryComparator comparator, final byte[] keyTypes, final Object[] keys1,
      final Object[] keys2) {
    final int minKeySize = Math.min(keys1.length, keys2.length);

    for (int k = 0; k < minKeySize; ++k) {
      final int result = comparator.compare(keys1[k], keyTypes[k], keys2[k], keyTypes[k]);
      if (result < 0)
        return -1;
      else if (result > 0)
        return 1;
    }
    return 0;
  }

  private int writeEntryValues(final Binary buffer, final Object[] values, final int availableSpaceInPage) {
    // WRITE NUMBER OF VALUES
    serializer.serializeValue(database, buffer, BinaryTypes.TYPE_INT, values.length);

    // WRITE VALUES
    for (int i = 0; i < values.length; ++i) {
      serializer.serializeValue(database, buffer, valueType, values[i]);
      if (storeTermFrequency)
        writeTermFrequency(buffer, values[i]);
      // Overflow check AFTER the whole i-th entry (RID + optional tf/docLength varints) is in the buffer: the entry is written as
      // a unit, never split. On overflow we return i (= i complete entries the caller may keep), but the buffer now physically
      // holds i+1 entries' bytes. That trailing entry is harmless: callers re-serialize from scratch (writeEntryMultipleValues
      // clears the buffer and rewrites the kept subset before committing to a page), so the over-written bytes are discarded.
      if (buffer.size() > availableSpaceInPage)
        return i;
    }
    return values.length;
  }

  private void writeEntryValue(final Binary buffer, final Object value) {
    // WRITE NUMBER OF VALUES
    serializer.serializeValue(database, buffer, BinaryTypes.TYPE_INT, 1);

    // WRITE VALUES
    serializer.serializeValue(database, buffer, valueType, value);
    if (storeTermFrequency)
      writeTermFrequency(buffer, value);
  }

  /**
   * Appends the BM25 per-posting statistics (term frequency and document length) right after the serialized RID. Values that are
   * not {@link FullTextPostingRID} (e.g. deletion markers and compacted root-page pointers) carry no statistics and are written as
   * zeroes so the on-disk layout stays uniform and symmetric with {@link #readEntryValues(Binary)}.
   * <p>
   * Alignment invariant: a given index instance writes and reads every page - leaf <i>and</i> compacted root - with the same
   * {@link #storeTermFrequency} flag, so the two zero varints written here for a root-page pointer are always consumed back by
   * {@link #readEntryValue(Binary)}. The pointer is reconstructed as a {@code FullTextPostingRID(tf=0, docLength=0)} (its bucket id
   * is non-negative), but the compacted-root read path uses only its position (the target page number), so this is harmless.
   */
  private void writeTermFrequency(final Binary buffer, final Object value) {
    if (value instanceof FullTextPostingRID posting) {
      buffer.putUnsignedNumber(posting.getTf());
      buffer.putUnsignedNumber(posting.getDocLength());
    } else {
      buffer.putUnsignedNumber(0);
      buffer.putUnsignedNumber(0);
    }
  }

  protected RID[] readEntryValues(final Binary buffer) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT, null);

    final RID[] rids = new RID[items];

    for (int i = 0; i < rids.length; ++i)
      rids[i] = readEntryValue(buffer);

    return rids;
  }

  private void readEntryValues(final Binary buffer, final List<RID> list) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT, null);

    for (int i = 0; i < items; ++i)
      list.add(readEntryValue(buffer));
  }

  /**
   * Reads a single posting value, reconstructing a {@link FullTextPostingRID} when {@link #storeTermFrequency} is enabled so full-text
   * scoring can read the term frequency and document length back from the cursor without an extra lookup.
   */
  private RID readEntryValue(final Binary buffer) {
    final RID rid = (RID) serializer.deserializeValue(database, buffer, valueType, null);
    if (!storeTermFrequency)
      return rid;

    // The tf/docLength varints are present for every stored value, so they must always be read to keep the buffer aligned.
    // Narrowed to int: tf and docLength are token counts within one document, so they fit comfortably in a (signed) int - a
    // document with > ~2.1 billion analyzed tokens is not representable and the FullTextPostingRID constructor would reject the
    // wrapped-negative value. If postings ever need to carry a larger statistic, widen these to long here and at the write side.
    final int tf = (int) buffer.getUnsignedNumber();
    final int docLength = (int) buffer.getUnsignedNumber();
    // Deletion markers (negative bucket id) carry no real statistics; keep them as a plain RID so nothing downstream mistakes a
    // marker for a scorable posting (the marker's tf/docLength are 0 and are discarded here).
    if (rid.getBucketId() < 0)
      return rid;
    return new FullTextPostingRID(database, rid.getBucketId(), rid.getPosition(), tf, docLength);
  }

  protected List<RID> readAllValuesFromResult(final Binary currentPageBuffer, final LookupResult result) {
    final List<RID> allValues = new ArrayList<>();
    for (int i = 0; i < result.valueBeginPositions.length; ++i) {
      currentPageBuffer.position(result.valueBeginPositions[i]);
      readEntryValues(currentPageBuffer, allValues);
    }
    return allValues;
  }

  protected int getCount(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE);
  }

  protected void setCount(final MutablePage currentPage, final int newCount) {
    currentPage.writeInt(INT_SERIALIZED_SIZE, newCount);
  }

  public void setStoreTermFrequency(final boolean storeTermFrequency) {
    this.storeTermFrequency = storeTermFrequency;
  }

  public boolean isStoreTermFrequency() {
    return storeTermFrequency;
  }

  protected boolean isMutable(final BasePage currentPage) {
    return currentPage.readByte(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE) == 1;
  }

  protected void setMutable(final MutablePage currentPage, final boolean mutable) {
    currentPage.writeByte(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE, (byte) (mutable ? 1 : 0));
  }

  protected void checkForNulls(final Object[] keys) {
    if (nullStrategy != NULL_STRATEGY.ERROR)
      return;

    if (keys != null)
      for (int i = 0; i < keys.length; ++i)
        if (keys[i] == null)
          throw new IllegalArgumentException(
              "Indexed key " + mainIndex.getTypeName() + mainIndex.getPropertyNames() + " cannot be NULL (" + Arrays.toString(
                  keys)
                  + ")");
  }

  protected boolean lookupInPageAndAddInResultset(final BasePage currentPage, final Binary currentPageBuffer, final int count,
      final Object[] originalKeys, final Object[] convertedKeys, final int limit, final Set<IndexCursorEntry> set,
      final Set<TransactionIndexContext.ComparableKey> removedKeys, final RidHashSet deletedRIDs) {
    final LookupResult result = lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, convertedKeys, 1);
    if (result.found) {
      // REAL ALL THE ENTRIES
      final List<RID> allValues = readAllValuesFromResult(currentPageBuffer, result);

      // #4945: deletedRIDs is threaded from the caller across pages and the compacted sub-index, exactly like
      // removedKeys. For a non-unique index a per-RID tombstone and its ADD frequently live in different pages,
      // so a page-local deletedRIDs set resurrected the deleted RID once the walk reached the older page.
      final TransactionIndexContext.ComparableKey keys = new TransactionIndexContext.ComparableKey(convertedKeys);

      // START FROM THE LAST ENTRY
      for (int i = allValues.size() - 1; i > -1; --i) {
        final RID rid = allValues.get(i);

        if (rid.getBucketId() < 0) {
          // This is a deletion marker - convert to original RID
          final RID originalRID = getOriginalRID(rid);
          deletedRIDs.add(originalRID);

          // For unique indexes, also mark the entire key as removed
          if (mainIndex.isUnique()) {
            removedKeys.add(keys);
          }
          continue;
        }

        // For unique indexes, check if the entire key has been removed
        if (mainIndex.isUnique() && removedKeys.contains(keys)) {
          // Skipping rid because key is in removedKeys (unique index)
          continue;
        }

        // For all indexes, check if this specific RID has been deleted
        if (deletedRIDs.contains(rid)) {
          // Skipping rid because it is in deletedRIDs
          continue;
        }

        set.add(new IndexCursorEntry(originalKeys, rid, 1));

        if (limit > -1 && set.size() >= limit) {
          return false;
        }
      }
    }
    return true;
  }

  protected int getValuesFreePosition(final BasePage currentPage) {
    return currentPage.readInt(0);
  }

  protected void setValuesFreePosition(final MutablePage currentPage, final int newValuesFreePosition) {
    currentPage.writeInt(0, newValuesFreePosition);
  }

  protected RID getOriginalRID(final RID rid) {
    return new RID((rid.getBucketId() * -1) - 2, rid.getPosition());
  }

  private int findEntryOfSameKey(final Binary currentPageBuffer, final Object[] keys, final int startIndexArray, int mid, int start,
      int end, int step) {
    int result;
    for (int i = start; i != end; i += step) {
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE)));

      result = 1;
      for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
        final boolean notNull = version < 1 || currentPageBuffer.getByte() == 1;
        if (!notNull) {
          // #4947 (pre-existing corruption): the stored component is NULL. Breaking here with the PREVIOUS
          // component's result (0 when the prefix matched) declared (k,null) the "same key" as (k,v), and
          // the purpose-1 caller then applied MID's key size to every pointer of the run - (k,null)'s key
          // is shorter, so its value position landed mid-bytes and the read underflowed. Same key only if
          // the search component is null too; otherwise nulls sort LOW, matching compareKey().
          result = keys[keyIndex] == null ? 0 : 1;
          if (result != 0)
            break;
          continue;
        }

        if (keys[keyIndex] == null) {
          // Search component null vs stored non-null: not the same key (nulls sort LOW). The old code fed
          // the null into compareBytes/compare below - an NPE for string keys.
          result = -1;
          break;
        }

        final byte keyType = binaryKeyTypes[keyIndex];
        if (keyType == BinaryTypes.TYPE_STRING) {
          // OPTIMIZATION: SPECIAL CASE, LAZY EVALUATE BYTE PER BYTE THE STRING
          result = comparator.compareBytes((byte[]) keys[keyIndex], currentPageBuffer);
        } else {
          final Object key = serializer.deserializeValue(database, currentPageBuffer, keyType, null);
          result = comparator.compare(keys[keyIndex], keyType, key, keyType);
        }

        if (result != 0)
          break;
      }

      if (result == 0) {
        mid = i;
        statsAdjacentSteps.incrementAndGet();
      } else
        break;
    }
    return mid;
  }

  protected int findFirstEntryOfSameKey(final Binary currentPageBuffer, final Object[] keys, final int startIndexArray, int mid) {
    return findEntryOfSameKey(currentPageBuffer, keys, startIndexArray, mid, mid - 1, -1, -1);
  }

  protected int findLastEntryOfSameKey(final int count, final Binary currentPageBuffer, final Object[] keys,
      final int startIndexArray, int mid) {
    return findEntryOfSameKey(currentPageBuffer, keys, startIndexArray, mid, mid + 1, count, 1);
  }

  private void writeKeys(final Binary buffer, final Object[] keys) {
    // WRITE KEYS
    for (int i = 0; i < binaryKeyTypes.length; ++i) {
      final Object value = keys[i];

      if (value == null)
        // WRITE 0 IF NULL
        buffer.putByte((byte) 0);
      else {
        // WRITE 1 + THE ACTUAL VALUE
        buffer.putByte((byte) 1);
        serializer.serializeValue(database, buffer, binaryKeyTypes[i], value);
      }
    }
  }
}
