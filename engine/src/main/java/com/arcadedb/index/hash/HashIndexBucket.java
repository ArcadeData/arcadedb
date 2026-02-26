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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Disk-backed extendible hash index using {@link PaginatedComponent} for page management.
 * <p>
 * File layout:
 * <pre>
 *   Page 0:   Metadata page (global depth, key types, bucket/directory start pages, etc.)
 *   Page 1+:  Directory pages (array of int bucket page numbers)
 *   Page D+:  Bucket pages (sorted entries of compressed key + compressed RID)
 * </pre>
 * <p>
 * Each bucket page stores entries sorted by serialized key for binary search within the bucket.
 * Overflow pages are chained when a bucket is full and cannot split (same hash prefix collision).
 */
public class HashIndexBucket extends PaginatedComponent {
  public static final String UNIQUE_INDEX_EXT    = "uhashidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nhashidx";

  public static final int DEF_PAGE_SIZE     = 65_536;
  public static final int CURRENT_VERSION   = 1;
  public static final int NO_OVERFLOW_PAGE  = -1;

  // Metadata page (page 0) layout offsets (relative to PAGE_HEADER_SIZE)
  static final int META_GLOBAL_DEPTH      = 0;                      // int (4)
  static final int META_TOTAL_ENTRIES      = 4;                      // int (4)
  static final int META_NUMBER_OF_KEYS     = 8;                      // byte (1)
  static final int META_KEY_TYPES_START    = 9;                      // byte[] (variable)
  // After key types: nullStrategy(1), unique(1), dirStartPage(4), bucketsStartPage(4), bucketCount(4)

  // Bucket page header offsets (relative to PAGE_HEADER_SIZE)
  static final int BUCKET_LOCAL_DEPTH     = 0;                       // short (2)
  static final int BUCKET_ENTRY_COUNT     = 2;                       // short (2)
  static final int BUCKET_OVERFLOW_PAGE   = 4;                       // int (4)
  static final int BUCKET_DATA_END       = 8;                        // short (2): offset past last entry data
  static final int BUCKET_CONTENT_START   = 10;                      // entries start here

  // Slot directory: 2-byte entry offsets stored at the END of the page, growing downward.
  // slot[i] is at pageOffset = (pageSize - PAGE_HEADER_SIZE) - (i + 1) * 2
  // Each slot stores the byte offset (relative to PAGE_HEADER_SIZE) of the entry's data.
  static final int SLOT_SIZE = 2;

  final HashIndex mainIndex;
  final BinarySerializer serializer;
  final BinaryComparator comparator;
  final boolean unique;

  Type[]  keyTypes;
  byte[]  binaryKeyTypes;
  LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy;

  // Cached metadata from page 0
  private int globalDepth;
  private int totalEntries;
  private int directoryStartPage;
  private int bucketsStartPage;
  private int bucketCount;

  /**
   * Called at creation time.
   */
  HashIndexBucket(final HashIndex mainIndex, final DatabaseInternal database, final String name, final boolean unique,
      final String filePath, final ComponentFile.MODE mode, final Type[] keyTypes, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) throws IOException {
    super(database, name, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, pageSize, CURRENT_VERSION);

    this.mainIndex = mainIndex;
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.keyTypes = keyTypes;
    this.binaryKeyTypes = new byte[keyTypes.length];
    for (int i = 0; i < keyTypes.length; i++)
      this.binaryKeyTypes[i] = keyTypes[i].getBinaryType();
    this.nullStrategy = nullStrategy;

    // Initialize the file: metadata page + 1 directory page + 1 initial bucket
    initializeNewIndex();
  }

  /**
   * Called at load time.
   */
  HashIndexBucket(final HashIndex mainIndex, final DatabaseInternal database, final String name, final boolean unique,
      final String filePath, final int id, final ComponentFile.MODE mode, final int pageSize,
      final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);

    this.mainIndex = mainIndex;
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;

    // Read metadata from page 0 (called during construction, like LSMTreeIndexMutable.onAfterLoad)
    onAfterLoad();
  }

  @Override
  public void onAfterLoad() {
    try {
      loadMetadata();
    } catch (final IOException e) {
      throw new IndexException("Error loading hash index metadata for '" + getName() + "'", e);
    }
  }

  @Override
  public Object getMainComponent() {
    return mainIndex;
  }

  public void drop() throws IOException {
    if (database.isOpen()) {
      database.getPageManager().deleteFile(database, file.getFileId());
      database.getFileManager().dropFile(file.getFileId());
      database.getSchema().getEmbedded().removeFile(file.getFileId());
    } else {
      if (!new java.io.File(file.getFilePath()).delete())
        LogManager.instance().log(this, Level.WARNING, "Error on deleting hash index file '%s'", null, file.getFilePath());
    }
  }

  @Override
  public void onAfterSchemaLoad() {
    try {
      loadMetadata();
    } catch (final IOException e) {
      throw new IndexException("Error loading hash index metadata for '" + getName() + "'", e);
    }
  }

  public boolean isUnique() {
    return unique;
  }

  public int getGlobalDepth() {
    return globalDepth;
  }

  public int getTotalEntries() {
    return totalEntries;
  }

  // ─── LOOKUP ──────────────────────────────────────────────

  /**
   * Looks up all RIDs for the given key(s).
   */
  List<RID> get(final Object[] keys, final int limit) throws IOException {
    final byte[] serializedKey = serializeKeys(keys);
    final long hash = murmurHash64(serializedKey);
    final int dirIndex = directoryIndex(hash, globalDepth);
    final int bucketPageNum = readDirectoryEntry(dirIndex);

    return searchBucket(bucketPageNum, serializedKey, limit);
  }

  /**
   * Searches a bucket page (and its overflow chain) for entries matching the given keys.
   */
  private List<RID> searchBucket(final int bucketPageNum, final byte[] searchKey,
      final int limit) throws IOException {
    final List<RID> result = new ArrayList<>();
    int currentPage = bucketPageNum;

    while (currentPage != NO_OVERFLOW_PAGE) {
      final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, currentPage), pageSize);
      final int entryCount = page.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
      final int overflowPage = page.readInt(BUCKET_OVERFLOW_PAGE);

      if (entryCount > 0)
        searchInPage(page, entryCount, searchKey, result, limit);

      if (limit > 0 && result.size() >= limit)
        break;

      currentPage = overflowPage;
    }
    return result;
  }

  /**
   * Binary search within a single bucket page for entries matching the given key.
   */
  private void searchInPage(final BasePage page, final int entryCount, final byte[] searchKey,
      final List<RID> result, final int limit) {
    int pos = findFirstEntry(page, entryCount, searchKey);
    if (pos < 0)
      return;

    while (pos < entryCount) {
      int offset = readSlot(page, pos);
      final int keyLen = computeKeyLengthFromPage(page, offset);

      if (!keysMatch(page, offset, searchKey))
        break;

      offset += keyLen;

      if (unique) {
        result.add(readCompressedRID(page, offset));
        return;
      } else {
        final int ridCount = readVarIntFromPage(page, offset);
        offset += varIntSize(ridCount);
        for (int r = 0; r < ridCount; r++) {
          final RID rid = readCompressedRID(page, offset);
          offset += compressedRIDSize(rid);
          result.add(rid);
          if (limit > 0 && result.size() >= limit)
            return;
        }
      }
      pos++;
    }
  }

  // ─── PUT ─────────────────────────────────────────────────

  /**
   * Inserts a key-RID pair into the index. For non-unique indexes, adds the RID to the existing entry if the key exists.
   */
  void put(final Object[] keys, final RID rid) throws IOException {
    final byte[] serializedKey = serializeKeys(keys);
    final long hash = murmurHash64(serializedKey);

    putInternal(serializedKey, rid, hash);
  }

  private void putInternal(final byte[] serializedKey, final RID rid, final long hash) throws IOException {
    final int dirIndex = directoryIndex(hash, globalDepth);
    final int bucketPageNum = readDirectoryEntry(dirIndex);

    final MutablePage bucketPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, bucketPageNum), pageSize, false);
    final int entryCount = bucketPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
    final int localDepth = bucketPage.readShort(BUCKET_LOCAL_DEPTH) & 0xFFFF;

    final byte[] serializedRID = serializeCompressedRID(rid);

    // For non-unique indexes, check if key already exists and append RID
    if (!unique) {
      final int existingPos = findExactEntry(bucketPage, entryCount, serializedKey);
      if (existingPos >= 0) {
        addRIDToExistingEntry(bucketPageNum, bucketPage, entryCount, existingPos, serializedKey, serializedRID);
        updateTotalEntries(1);
        return;
      }
    }

    // Calculate entry size (data + slot)
    final int entryDataSize = unique ?
        serializedKey.length + serializedRID.length :
        serializedKey.length + varIntSize(1) + serializedRID.length;
    final int totalNeeded = entryDataSize + SLOT_SIZE;

    // Try to insert into the bucket
    final int available = freeSpace(bucketPage, entryCount);

    if (totalNeeded <= available) {
      insertEntryInSlottedPage(bucketPage, entryCount, serializedKey, serializedRID);
      updateTotalEntries(1);
    } else {
      // Bucket is full — try to split if it would help
      if (localDepth < globalDepth || localDepth < 30) {
        final boolean splitWillHelp = canSplitHelp(bucketPageNum, entryCount, localDepth);

        if (splitWillHelp) {
          splitBucket(bucketPageNum, localDepth, dirIndex, hash);
          putInternal(serializedKey, rid, hash);
        } else {
          insertIntoOverflow(bucketPage, bucketPageNum, serializedKey, serializedRID);
          updateTotalEntries(1);
        }
      } else {
        insertIntoOverflow(bucketPage, bucketPageNum, serializedKey, serializedRID);
        updateTotalEntries(1);
      }
    }
  }

  // ─── REMOVE ──────────────────────────────────────────────

  /**
   * Removes all entries for the given key.
   */
  void remove(final Object[] keys) throws IOException {
    final byte[] serializedKey = serializeKeys(keys);
    final long hash = murmurHash64(serializedKey);
    final int dirIndex = directoryIndex(hash, globalDepth);
    final int bucketPageNum = readDirectoryEntry(dirIndex);

    removeFromBucket(bucketPageNum, serializedKey, null);
  }

  /**
   * Removes a specific key-RID pair.
   */
  void remove(final Object[] keys, final RID rid) throws IOException {
    final byte[] serializedKey = serializeKeys(keys);
    final long hash = murmurHash64(serializedKey);
    final int dirIndex = directoryIndex(hash, globalDepth);
    final int bucketPageNum = readDirectoryEntry(dirIndex);

    removeFromBucket(bucketPageNum, serializedKey, rid);
  }

  private void removeFromBucket(final int bucketPageNum, final byte[] serializedKey, final RID specificRID) throws IOException {
    int currentPageNum = bucketPageNum;

    while (currentPageNum != NO_OVERFLOW_PAGE) {
      final MutablePage page = database.getTransaction()
          .getPageToModify(new PageId(database, fileId, currentPageNum), pageSize, false);
      final int entryCount = page.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
      final int overflowPage = page.readInt(BUCKET_OVERFLOW_PAGE);

      final int pos = findExactEntry(page, entryCount, serializedKey);
      if (pos >= 0) {
        if (specificRID != null && !unique) {
          final int removed = removeRIDFromEntry(page, entryCount, pos, specificRID);
          updateTotalEntries(-removed);
        } else {
          final int removedCount = removeEntryFromPage(page, entryCount, pos);
          updateTotalEntries(-removedCount);
        }
        return;
      }

      currentPageNum = overflowPage;
    }
  }

  // ─── COUNTING ────────────────────────────────────────────

  long countEntries() throws IOException {
    // Re-read from metadata page for accuracy
    final BasePage metaPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);
    return metaPage.readInt(META_TOTAL_ENTRIES);
  }

  /**
   * Checks whether splitting the bucket will actually distribute entries across two buckets.
   * If all entries have the same next hash bit, splitting won't help.
   */
  private boolean canSplitHelp(final int bucketPageNum, final int entryCount, final int localDepth) throws IOException {
    final int newLocalDepth = localDepth + 1;
    final int effectiveGlobalDepth = Math.max(globalDepth, newLocalDepth);
    final int splitBit = 1 << (effectiveGlobalDepth - newLocalDepth);

    boolean seenZero = false;
    boolean seenOne = false;

    // Check entries in main page and overflow
    int currentPage = bucketPageNum;
    while (currentPage != NO_OVERFLOW_PAGE) {
      final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, currentPage), pageSize);
      final int count = page.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
      final int overflowPage = page.readInt(BUCKET_OVERFLOW_PAGE);

      for (int i = 0; i < count; i++) {
        final int offset = readSlot(page, i);
        final int keyLen = computeKeyLengthFromPage(page, offset);
        final byte[] keyBytes = new byte[keyLen];
        page.readByteArray(offset, keyBytes);
        final long h = murmurHash64(keyBytes);
        final int dirIdx = directoryIndex(h, effectiveGlobalDepth);
        if ((dirIdx & splitBit) != 0)
          seenOne = true;
        else
          seenZero = true;

        if (seenZero && seenOne)
          return true;
      }
      currentPage = overflowPage;
    }

    return seenZero && seenOne;
  }

  // ─── SPLIT ───────────────────────────────────────────────

  /**
   * Splits a bucket that has overflowed. Creates a new bucket, redistributes entries.
   */
  private void splitBucket(final int bucketPageNum, final int localDepth, final int dirIndex,
      final long hash) throws IOException {
    final MutablePage oldBucketPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, bucketPageNum), pageSize, false);
    final int entryCount = oldBucketPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
    final int newLocalDepth = localDepth + 1;

    // If localDepth == globalDepth, we need to double the directory
    if (newLocalDepth > globalDepth)
      doubleDirectory();

    // Allocate new bucket page
    final int newBucketPageNum = allocateBucketPage(newLocalDepth);

    // Update old bucket local depth
    oldBucketPage.writeShort(BUCKET_LOCAL_DEPTH, (short) newLocalDepth);

    // Collect all entries from old bucket (including overflow chain)
    final List<byte[]> allEntries = collectAllEntries(bucketPageNum, entryCount);

    // Clear old bucket entries and reset data area
    oldBucketPage.writeShort(BUCKET_ENTRY_COUNT, (short) 0);
    oldBucketPage.writeShort(BUCKET_DATA_END, (short) BUCKET_CONTENT_START);
    // Clear overflow chain (entries will be redistributed)
    clearOverflowChain(oldBucketPage);

    // Update directory pointers: all entries that were pointing to old bucket
    // and have the new bit set should now point to the new bucket
    updateDirectoryAfterSplit(bucketPageNum, newBucketPageNum, localDepth, newLocalDepth);

    // Redistribute entries between old and new bucket
    for (final byte[] entry : allEntries) {
      final long entryHash = hashSerializedKey(entry);
      final int newDirIndex = directoryIndex(entryHash, globalDepth);
      final int targetBucketPage = readDirectoryEntry(newDirIndex);
      insertRawEntry(targetBucketPage, entry);
    }
  }

  /**
   * Doubles the directory size by incrementing globalDepth.
   */
  private void doubleDirectory() throws IOException {
    final int oldSize = 1 << globalDepth;
    final int newSize = oldSize * 2;
    globalDepth++;

    // Read old directory entries
    final int[] oldEntries = new int[oldSize];
    for (int i = 0; i < oldSize; i++)
      oldEntries[i] = readDirectoryEntry(i);

    // Ensure directory pages have enough space
    ensureDirectoryCapacity(newSize);

    // Write doubled directory: each old entry is duplicated
    for (int i = 0; i < oldSize; i++) {
      writeDirectoryEntry(2 * i, oldEntries[i]);
      writeDirectoryEntry(2 * i + 1, oldEntries[i]);
    }

    // Update metadata
    writeGlobalDepth(globalDepth);
  }

  // ─── OVERFLOW PAGES ──────────────────────────────────────

  private void insertIntoOverflow(final MutablePage bucketPage, final int bucketPageNum,
      final byte[] serializedKey, final byte[] serializedRID) throws IOException {
    int overflowPageNum = bucketPage.readInt(BUCKET_OVERFLOW_PAGE);

    if (overflowPageNum == NO_OVERFLOW_PAGE) {
      // Allocate new overflow page with same local depth
      final int localDepth = bucketPage.readShort(BUCKET_LOCAL_DEPTH) & 0xFFFF;
      overflowPageNum = allocateOverflowPage(localDepth);
      bucketPage.writeInt(BUCKET_OVERFLOW_PAGE, overflowPageNum);
    }

    final MutablePage overflowPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, overflowPageNum), pageSize, false);
    final int entryCount = overflowPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
    final int entryDataSize = unique ?
        serializedKey.length + serializedRID.length :
        serializedKey.length + varIntSize(1) + serializedRID.length;
    final int totalNeeded = entryDataSize + SLOT_SIZE;

    if (totalNeeded <= freeSpace(overflowPage, entryCount)) {
      insertEntryInPage(overflowPage, entryCount, serializedKey, serializedRID);
    } else {
      // Chain another overflow page
      insertIntoOverflow(overflowPage, overflowPageNum, serializedKey, serializedRID);
    }
  }

  private void clearOverflowChain(final MutablePage bucketPage) throws IOException {
    int overflowPageNum = bucketPage.readInt(BUCKET_OVERFLOW_PAGE);
    bucketPage.writeInt(BUCKET_OVERFLOW_PAGE, NO_OVERFLOW_PAGE);

    // Note: overflow pages are leaked here but will be reclaimed on next compaction/rebuild.
    // For correctness, the entries from overflow pages are already collected before this call.
  }

  // ─── PAGE ALLOCATION ─────────────────────────────────────

  private int allocateBucketPage(final int localDepth) throws IOException {
    final int newPageNum = getTotalPages();
    final MutablePage newPage = database.getTransaction().addPage(new PageId(database, fileId, newPageNum), pageSize);
    updatePageCount(newPageNum + 1);

    newPage.writeShort(BUCKET_LOCAL_DEPTH, (short) localDepth);
    newPage.writeShort(BUCKET_ENTRY_COUNT, (short) 0);
    newPage.writeInt(BUCKET_OVERFLOW_PAGE, NO_OVERFLOW_PAGE);
    newPage.writeShort(BUCKET_DATA_END, (short) BUCKET_CONTENT_START);

    bucketCount++;
    writeBucketCount(bucketCount);

    return newPageNum;
  }

  private int allocateOverflowPage(final int localDepth) throws IOException {
    return allocateBucketPage(localDepth);
  }

  // ─── INITIALIZATION ──────────────────────────────────────

  private void initializeNewIndex() throws IOException {
    this.globalDepth = 0;
    this.totalEntries = 0;
    this.bucketCount = 0;

    // Page 0: metadata
    final MutablePage metaPage = database.getTransaction().addPage(new PageId(database, fileId, 0), pageSize);
    updatePageCount(1);

    int pos = META_GLOBAL_DEPTH;
    metaPage.writeInt(pos, 0);                           // globalDepth = 0
    pos += Binary.INT_SERIALIZED_SIZE;
    metaPage.writeInt(pos, 0);                           // totalEntries = 0
    pos += Binary.INT_SERIALIZED_SIZE;
    metaPage.writeByte(pos, (byte) binaryKeyTypes.length); // numberOfKeys
    pos += Binary.BYTE_SERIALIZED_SIZE;
    for (final byte binaryKeyType : binaryKeyTypes) {
      metaPage.writeByte(pos, binaryKeyType);
      pos += Binary.BYTE_SERIALIZED_SIZE;
    }
    metaPage.writeByte(pos, (byte) nullStrategy.ordinal());
    pos += Binary.BYTE_SERIALIZED_SIZE;
    metaPage.writeByte(pos, (byte) (unique ? 1 : 0));
    pos += Binary.BYTE_SERIALIZED_SIZE;

    this.directoryStartPage = 1;
    metaPage.writeInt(pos, directoryStartPage);
    pos += Binary.INT_SERIALIZED_SIZE;

    this.bucketsStartPage = 2;
    metaPage.writeInt(pos, bucketsStartPage);
    pos += Binary.INT_SERIALIZED_SIZE;

    metaPage.writeInt(pos, 0); // bucketCount = 0

    // Page 1: directory (initially 1 entry pointing to bucket at page 2)
    final MutablePage dirPage = database.getTransaction().addPage(new PageId(database, fileId, 1), pageSize);
    updatePageCount(2);
    dirPage.writeInt(0, bucketsStartPage); // directory[0] → bucket page 2

    // Page 2: initial bucket (local depth 0, empty)
    final MutablePage bucketPage = database.getTransaction().addPage(new PageId(database, fileId, 2), pageSize);
    updatePageCount(3);
    bucketPage.writeShort(BUCKET_LOCAL_DEPTH, (short) 0);
    bucketPage.writeShort(BUCKET_ENTRY_COUNT, (short) 0);
    bucketPage.writeInt(BUCKET_OVERFLOW_PAGE, NO_OVERFLOW_PAGE);
    bucketPage.writeShort(BUCKET_DATA_END, (short) BUCKET_CONTENT_START);

    bucketCount = 1;
    writeBucketCount(1);
  }

  private void loadMetadata() throws IOException {
    final BasePage metaPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);

    globalDepth = metaPage.readInt(META_GLOBAL_DEPTH);
    totalEntries = metaPage.readInt(META_TOTAL_ENTRIES);

    int pos = META_KEY_TYPES_START;
    final int numKeys = metaPage.readByte(META_NUMBER_OF_KEYS) & 0xFF;
    binaryKeyTypes = new byte[numKeys];
    keyTypes = new Type[numKeys];
    for (int i = 0; i < numKeys; i++) {
      binaryKeyTypes[i] = metaPage.readByte(pos);
      keyTypes[i] = Type.getByBinaryType(binaryKeyTypes[i]);
      pos += Binary.BYTE_SERIALIZED_SIZE;
    }
    nullStrategy = LSMTreeIndexAbstract.NULL_STRATEGY.values()[metaPage.readByte(pos) & 0xFF];
    pos += Binary.BYTE_SERIALIZED_SIZE;
    // unique is already set from constructor
    pos += Binary.BYTE_SERIALIZED_SIZE;

    directoryStartPage = metaPage.readInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;
    bucketsStartPage = metaPage.readInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;
    bucketCount = metaPage.readInt(pos);
  }

  // ─── DIRECTORY OPERATIONS ────────────────────────────────

  /**
   * Reads the bucket page number from the directory at the given index.
   */
  int readDirectoryEntry(final int index) throws IOException {
    final int entriesPerPage = (pageSize - BasePage.PAGE_HEADER_SIZE) / Binary.INT_SERIALIZED_SIZE;
    final int dirPageOffset = index / entriesPerPage;
    final int entryOffset = (index % entriesPerPage) * Binary.INT_SERIALIZED_SIZE;

    final BasePage dirPage = database.getTransaction()
        .getPage(new PageId(database, fileId, directoryStartPage + dirPageOffset), pageSize);
    return dirPage.readInt(entryOffset);
  }

  /**
   * Writes a bucket page number to the directory at the given index.
   */
  private void writeDirectoryEntry(final int index, final int bucketPageNum) throws IOException {
    final int entriesPerPage = (pageSize - BasePage.PAGE_HEADER_SIZE) / Binary.INT_SERIALIZED_SIZE;
    final int dirPageOffset = index / entriesPerPage;
    final int entryOffset = (index % entriesPerPage) * Binary.INT_SERIALIZED_SIZE;

    final MutablePage dirPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, directoryStartPage + dirPageOffset), pageSize, false);
    dirPage.writeInt(entryOffset, bucketPageNum);
  }

  /**
   * Ensures the directory has enough pages for the given number of entries.
   */
  private void ensureDirectoryCapacity(final int numEntries) throws IOException {
    final int entriesPerPage = (pageSize - BasePage.PAGE_HEADER_SIZE) / Binary.INT_SERIALIZED_SIZE;
    final int neededPages = (numEntries + entriesPerPage - 1) / entriesPerPage;
    final int currentDirPages = bucketsStartPage - directoryStartPage;

    if (neededPages > currentDirPages) {
      // We need more directory pages. Since bucket pages come after directory pages,
      // we need to shift things. For simplicity, allocate directory pages at the end
      // and update directoryStartPage. Actually, since the directory may be small and
      // we'd need to move bucket pointers, let's use a simpler approach:
      // Allocate new directory pages at the end of the file and update the start page.

      final int newDirStartPage = getTotalPages();
      for (int i = 0; i < neededPages; i++) {
        final MutablePage newDirPage = database.getTransaction()
            .addPage(new PageId(database, fileId, newDirStartPage + i), pageSize);
        updatePageCount(newDirStartPage + i + 1);
      }

      // Copy existing directory entries to new location
      final int oldDirSize = 1 << (globalDepth - 1); // size before doubling
      for (int i = 0; i < oldDirSize; i++) {
        final int entriesPerOldPage = (pageSize - BasePage.PAGE_HEADER_SIZE) / Binary.INT_SERIALIZED_SIZE;
        final int oldPageOffset = i / entriesPerOldPage;
        final int oldEntryOffset = (i % entriesPerOldPage) * Binary.INT_SERIALIZED_SIZE;

        final BasePage oldDirPage = database.getTransaction()
            .getPage(new PageId(database, fileId, directoryStartPage + oldPageOffset), pageSize);
        final int bucketNum = oldDirPage.readInt(oldEntryOffset);

        final int newPageOffset = i / entriesPerPage;
        final int newEntryOffset = (i % entriesPerPage) * Binary.INT_SERIALIZED_SIZE;
        final MutablePage newDirPage = database.getTransaction()
            .getPageToModify(new PageId(database, fileId, newDirStartPage + newPageOffset), pageSize, false);
        newDirPage.writeInt(newEntryOffset, bucketNum);
      }

      directoryStartPage = newDirStartPage;
      writeDirectoryStartPage(directoryStartPage);
    }
  }

  private void updateDirectoryAfterSplit(final int oldBucketPage, final int newBucketPage,
      final int oldLocalDepth, final int newLocalDepth) throws IOException {
    final int directorySize = 1 << globalDepth;
    // The split bit is the newLocalDepth-th bit from the MSB of the hash.
    // In the directory index (top globalDepth bits), this maps to bit (globalDepth - newLocalDepth) from LSB.
    final int splitBit = 1 << (globalDepth - newLocalDepth);

    for (int i = 0; i < directorySize; i++) {
      final int bucketPageForEntry = readDirectoryEntry(i);
      if (bucketPageForEntry == oldBucketPage) {
        if ((i & splitBit) != 0)
          writeDirectoryEntry(i, newBucketPage);
      }
    }
  }

  // ─── ENTRY SERIALIZATION ─────────────────────────────────

  /**
   * Serializes composite keys into a byte array using BinarySerializer.
   */
  byte[] serializeKeys(final Object[] keys) {
    final Binary buffer = new Binary(64, true);
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == null) {
        buffer.putByte(buffer.position(), (byte) 0); // null marker
        buffer.position(buffer.position() + 1);
      } else {
        buffer.putByte(buffer.position(), (byte) 1); // not null marker
        buffer.position(buffer.position() + 1);
        serializer.serializeValue(database, buffer, binaryKeyTypes[i], keys[i]);
      }
    }
    final byte[] result = new byte[buffer.position()];
    buffer.getByteBuffer().position(0);
    buffer.getByteBuffer().get(result, 0, result.length);
    return result;
  }

  /**
   * Serializes a RID in compressed format.
   */
  byte[] serializeCompressedRID(final RID rid) {
    final Binary buffer = new Binary(12, true);
    serializer.serializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, rid);
    final byte[] result = new byte[buffer.position()];
    buffer.getByteBuffer().position(0);
    buffer.getByteBuffer().get(result, 0, result.length);
    return result;
  }

  // ─── HASHING ─────────────────────────────────────────────

  /**
   * Hashes the serialized key using a 64-bit hash function.
   * Uses the same serialization as serializeKeys() for consistency.
   */
  long hashKeys(final Object[] keys) {
    final byte[] serialized = serializeKeys(keys);
    return murmurHash64(serialized);
  }

  /**
   * Hashes an already-serialized key (raw entry bytes; key portion only).
   */
  private long hashSerializedKey(final byte[] rawEntry) {
    // rawEntry contains the full entry: key + value. We need to hash just the key part.
    // For redistribution, we need to extract the key portion and hash it.
    final int keyLen = computeKeyLengthFromEntry(rawEntry, 0);
    return murmurHash64(rawEntry, 0, keyLen);
  }

  /**
   * Extracts the directory index from a hash given the current global depth.
   * Uses the top bits of the hash for better distribution.
   */
  static int directoryIndex(final long hash, final int globalDepth) {
    if (globalDepth == 0)
      return 0;
    return (int) ((hash >>> (64 - globalDepth)) & ((1L << globalDepth) - 1));
  }

  /**
   * MurmurHash3 finalization mix for 64-bit hashing.
   */
  static long murmurHash64(final byte[] data) {
    return murmurHash64(data, 0, data.length);
  }

  static long murmurHash64(final byte[] data, final int offset, final int length) {
    long h = 0xcafebabe_deadbeefL;
    final int nblocks = length / 8;

    for (int i = 0; i < nblocks; i++) {
      int idx = offset + i * 8;
      long k = ((long) data[idx] & 0xff)
          | (((long) data[idx + 1] & 0xff) << 8)
          | (((long) data[idx + 2] & 0xff) << 16)
          | (((long) data[idx + 3] & 0xff) << 24)
          | (((long) data[idx + 4] & 0xff) << 32)
          | (((long) data[idx + 5] & 0xff) << 40)
          | (((long) data[idx + 6] & 0xff) << 48)
          | (((long) data[idx + 7] & 0xff) << 56);

      k *= 0xff51afd7ed558ccdL;
      k = Long.rotateLeft(k, 31);
      k *= 0xc4ceb9fe1a85ec53L;
      h ^= k;
      h = Long.rotateLeft(h, 27);
      h = h * 5 + 0x52dce729;
    }

    long k1 = 0;
    final int tail = offset + nblocks * 8;
    switch (length & 7) {
    case 7: k1 ^= ((long) data[tail + 6] & 0xff) << 48;
    case 6: k1 ^= ((long) data[tail + 5] & 0xff) << 40;
    case 5: k1 ^= ((long) data[tail + 4] & 0xff) << 32;
    case 4: k1 ^= ((long) data[tail + 3] & 0xff) << 24;
    case 3: k1 ^= ((long) data[tail + 2] & 0xff) << 16;
    case 2: k1 ^= ((long) data[tail + 1] & 0xff) << 8;
    case 1:
      k1 ^= ((long) data[tail] & 0xff);
      k1 *= 0xff51afd7ed558ccdL;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= 0xc4ceb9fe1a85ec53L;
      h ^= k1;
    }

    h ^= length;
    // Finalization mix
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }

  // ─── PAGE-LEVEL ENTRY OPERATIONS ─────────────────────────

  /**
   * Inserts a new entry into a bucket page at the correct sorted position (builds offsets internally).
   * Used by overflow insertion where offsets aren't pre-computed.
   */
  private void insertEntryInPage(final MutablePage page, final int entryCount, final byte[] serializedKey,
      final byte[] serializedRID) {
    insertEntryInSlottedPage(page, entryCount, serializedKey, serializedRID);
  }

  /**
   * Inserts a raw entry (already serialized) into a bucket page, finding the correct position.
   */
  private void insertRawEntry(final int bucketPageNum, final byte[] rawEntry) throws IOException {
    final MutablePage page = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, bucketPageNum), pageSize, false);
    final int entryCount = page.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
    final int totalNeeded = rawEntry.length + SLOT_SIZE;

    if (totalNeeded <= freeSpace(page, entryCount)) {
      final int keyLen = computeKeyLengthFromEntry(rawEntry, 0);
      final byte[] serializedKey = new byte[keyLen];
      System.arraycopy(rawEntry, 0, serializedKey, 0, keyLen);

      final int insertPos = findInsertionPoint(page, entryCount, serializedKey);

      // Append data at dataEnd
      final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
      page.writeByteArray(dataEnd, rawEntry);
      page.writeShort(BUCKET_DATA_END, (short) (dataEnd + rawEntry.length));

      // Shift slots right and insert
      for (int i = entryCount; i > insertPos; i--)
        writeSlot(page, i, readSlot(page, i - 1));
      writeSlot(page, insertPos, dataEnd);

      page.writeShort(BUCKET_ENTRY_COUNT, (short) (entryCount + 1));
    } else {
      final int keyLen = computeKeyLengthFromEntry(rawEntry, 0);
      final byte[] serializedKey = new byte[keyLen];
      System.arraycopy(rawEntry, 0, serializedKey, 0, keyLen);
      final byte[] serializedRID = new byte[rawEntry.length - keyLen];
      System.arraycopy(rawEntry, keyLen, serializedRID, 0, rawEntry.length - keyLen);
      insertIntoOverflow(page, bucketPageNum, serializedKey, serializedRID);
    }
  }

  /**
   * Removes an entire entry at the given position. Returns the number of RIDs removed.
   */
  private int removeEntryFromPage(final MutablePage page, final int entryCount, final int pos) {
    final int entryOffset = readSlot(page, pos);
    final int entrySize = getEntrySize(page, entryOffset);

    // Count RIDs being removed
    int removedCount;
    if (unique) {
      removedCount = 1;
    } else {
      final int keyLen = computeKeyLengthFromPage(page, entryOffset);
      removedCount = readVarIntFromPage(page, entryOffset + keyLen);
    }

    // For slotted pages, we don't need to shift entry data (it becomes a hole).
    // We only need to shift the slot directory to remove the slot.
    // The hole will be reclaimed on the next page rebuild (during split/compaction).
    // For simplicity, just shift slots left.
    for (int i = pos; i < entryCount - 1; i++)
      writeSlot(page, i, readSlot(page, i + 1));

    // Note: dataEnd stays the same (dead space). We'll recover it during splits.
    page.writeShort(BUCKET_ENTRY_COUNT, (short) (entryCount - 1));
    return removedCount;
  }

  /**
   * For non-unique index: adds a RID to an existing entry for the same key.
   */
  private void addRIDToExistingEntry(final int bucketPageNum, final MutablePage page, final int entryCount,
      final int pos, final byte[] serializedKey, final byte[] serializedRID) throws IOException {
    final int entryStart = readSlot(page, pos);
    final int oldEntrySize = getEntrySize(page, entryStart);

    // Skip the key
    final int keyLen = computeKeyLengthFromPage(page, entryStart);
    final int ridCountOffset = entryStart + keyLen;

    // Read current RID count
    final int currentRidCount = readVarIntFromPage(page, ridCountOffset);
    final int oldRidCountSize = varIntSize(currentRidCount);
    final int newRidCount = currentRidCount + 1;
    final int newRidCountSize = varIntSize(newRidCount);

    // For slotted page: we need to write the updated entry. If the RID count encoding
    // size doesn't change, we can append the RID in-place if this entry is at the dataEnd.
    // Otherwise, we remove the old entry and re-insert a new one.
    final int ridCountDiff = newRidCountSize - oldRidCountSize;

    // Check if there's space for the extra data
    final int availableSpace = freeSpace(page, entryCount);

    final int extraSpace = serializedRID.length + ridCountDiff;
    if (extraSpace > availableSpace) {
      throw new IndexException("Hash index bucket full when adding RID to existing entry");
    }

    // In a slotted page, we write the updated entry as a new copy at dataEnd.
    // Read old entry data, build new entry with extra RID, write at dataEnd, update slot.
    final byte[] oldEntryBytes = new byte[oldEntrySize];
    page.readByteArray(entryStart, oldEntryBytes);

    // Build new entry: key + newRidCount + old RIDs + new RID
    final int oldRidsStart = keyLen + oldRidCountSize;
    final int oldRidsLen = oldEntrySize - oldRidsStart;
    final byte[] newRidCountBytes = encodeVarInt(newRidCount);

    final int newEntrySize = keyLen + newRidCountBytes.length + oldRidsLen + serializedRID.length;
    final byte[] newEntry = new byte[newEntrySize];
    System.arraycopy(oldEntryBytes, 0, newEntry, 0, keyLen);
    System.arraycopy(newRidCountBytes, 0, newEntry, keyLen, newRidCountBytes.length);
    System.arraycopy(oldEntryBytes, oldRidsStart, newEntry, keyLen + newRidCountBytes.length, oldRidsLen);
    System.arraycopy(serializedRID, 0, newEntry, keyLen + newRidCountBytes.length + oldRidsLen, serializedRID.length);

    // Write new entry at dataEnd
    final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
    page.writeByteArray(dataEnd, newEntry);
    page.writeShort(BUCKET_DATA_END, (short) (dataEnd + newEntrySize));

    // Update slot to point to the new location (old entry data becomes a hole)
    writeSlot(page, pos, dataEnd);
  }

  /**
   * For non-unique index: removes a specific RID from an entry. Returns 1 if removed, 0 otherwise.
   * If the entry has only one RID left, removes the entire entry.
   */
  private int removeRIDFromEntry(final MutablePage page, final int entryCount, final int pos,
      final RID targetRID) {
    final int entryStart = readSlot(page, pos);
    final int keyLen = computeKeyLengthFromPage(page, entryStart);
    int offset = entryStart + keyLen;

    final int ridCount = readVarIntFromPage(page, offset);

    if (ridCount <= 1) {
      removeEntryFromPage(page, entryCount, pos);
      return 1;
    }

    // Read old entry, rebuild without the target RID, write at dataEnd
    final int oldEntrySize = getEntrySize(page, entryStart);
    final byte[] oldEntry = new byte[oldEntrySize];
    page.readByteArray(entryStart, oldEntry);

    // Find and skip the target RID in the byte array
    final int ridCountSize = varIntSize(ridCount);
    int ridOffset = keyLen + ridCountSize;
    for (int r = 0; r < ridCount; r++) {
      final RID rid = readCompressedRID(page, entryStart + ridOffset);
      final int ridSize = compressedRIDSize(rid);
      if (rid.equals(targetRID)) {
        // Build new entry without this RID
        final int newRidCount = ridCount - 1;
        final byte[] newRidCountBytes = encodeVarInt(newRidCount);
        final int newEntrySize = oldEntrySize - ridSize - (ridCountSize - newRidCountBytes.length);
        final byte[] newEntry = new byte[newEntrySize];

        // Copy key
        System.arraycopy(oldEntry, 0, newEntry, 0, keyLen);
        // Write new rid count
        System.arraycopy(newRidCountBytes, 0, newEntry, keyLen, newRidCountBytes.length);
        // Copy RIDs before the removed one
        final int ridsBeforeLen = ridOffset - keyLen - ridCountSize;
        if (ridsBeforeLen > 0)
          System.arraycopy(oldEntry, keyLen + ridCountSize, newEntry, keyLen + newRidCountBytes.length, ridsBeforeLen);
        // Copy RIDs after the removed one
        final int ridsAfterStart = ridOffset + ridSize;
        final int ridsAfterLen = oldEntrySize - ridsAfterStart;
        if (ridsAfterLen > 0)
          System.arraycopy(oldEntry, ridsAfterStart, newEntry,
              keyLen + newRidCountBytes.length + ridsBeforeLen, ridsAfterLen);

        // Write new entry at dataEnd, update slot
        final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
        page.writeByteArray(dataEnd, newEntry);
        page.writeShort(BUCKET_DATA_END, (short) (dataEnd + newEntrySize));
        writeSlot(page, pos, dataEnd);

        return 1;
      }
      ridOffset += ridSize;
    }
    return 0;
  }

  // ─── SLOTTED PAGE ACCESS ────────────────────────────────

  /**
   * Returns the page-relative offset where slot[i] is stored.
   * Slots grow from the end of the usable page area downward.
   */
  private int slotPosition(final int index) {
    return (pageSize - BasePage.PAGE_HEADER_SIZE) - (index + 1) * SLOT_SIZE;
  }

  /**
   * Reads the data offset stored in slot[index].
   */
  private int readSlot(final BasePage page, final int index) {
    return page.readShort(slotPosition(index)) & 0xFFFF;
  }

  /**
   * Writes a data offset into slot[index].
   */
  private void writeSlot(final MutablePage page, final int index, final int dataOffset) {
    page.writeShort(slotPosition(index), (short) dataOffset);
  }

  /**
   * Returns the available free space in a bucket page.
   * Free space = gap between data end and the start of the slot directory.
   */
  private int freeSpace(final BasePage page, final int entryCount) {
    final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
    final int slotStart = (pageSize - BasePage.PAGE_HEADER_SIZE) - entryCount * SLOT_SIZE;
    return slotStart - dataEnd;
  }

  /**
   * Binary search using slot directory. Returns position of first match or -1.
   */
  private int findFirstEntry(final BasePage page, final int entryCount, final byte[] searchKey) {
    if (entryCount == 0)
      return -1;

    int low = 0;
    int high = entryCount - 1;
    int result = -1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int cmp = compareKeyBytes(page, readSlot(page, mid), searchKey);

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else {
        result = mid;
        high = mid - 1;
      }
    }
    return result;
  }

  private int findExactEntry(final BasePage page, final int entryCount, final byte[] searchKey) {
    return findFirstEntry(page, entryCount, searchKey);
  }

  /**
   * Finds insertion point using slot directory. Returns position for new entry.
   */
  private int findInsertionPoint(final BasePage page, final int entryCount, final byte[] searchKey) {
    if (entryCount == 0)
      return 0;

    int low = 0;
    int high = entryCount - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int cmp = compareKeyBytes(page, readSlot(page, mid), searchKey);

      if (cmp < 0)
        low = mid + 1;
      else
        high = mid - 1;
    }
    return low;
  }

  /**
   * Inserts entry into page using slotted page layout. Appends data at dataEnd,
   * inserts slot at correct sorted position by shifting subsequent slots.
   */
  private void insertEntryInSlottedPage(final MutablePage page, final int entryCount,
      final byte[] serializedKey, final byte[] serializedRID) {
    final byte[] entryBytes;
    if (unique) {
      entryBytes = new byte[serializedKey.length + serializedRID.length];
      System.arraycopy(serializedKey, 0, entryBytes, 0, serializedKey.length);
      System.arraycopy(serializedRID, 0, entryBytes, serializedKey.length, serializedRID.length);
    } else {
      final byte[] ridCountBytes = encodeVarInt(1);
      entryBytes = new byte[serializedKey.length + ridCountBytes.length + serializedRID.length];
      System.arraycopy(serializedKey, 0, entryBytes, 0, serializedKey.length);
      System.arraycopy(ridCountBytes, 0, entryBytes, serializedKey.length, ridCountBytes.length);
      System.arraycopy(serializedRID, 0, entryBytes, serializedKey.length + ridCountBytes.length, serializedRID.length);
    }

    final int insertPos = findInsertionPoint(page, entryCount, serializedKey);

    // Append entry data at the end of the data area
    final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
    page.writeByteArray(dataEnd, entryBytes);
    final int newDataEnd = dataEnd + entryBytes.length;
    page.writeShort(BUCKET_DATA_END, (short) newDataEnd);

    // Shift slots from insertPos..entryCount-1 right by one position
    for (int i = entryCount; i > insertPos; i--)
      writeSlot(page, i, readSlot(page, i - 1));

    // Write new slot pointing to the appended data
    writeSlot(page, insertPos, dataEnd);

    page.writeShort(BUCKET_ENTRY_COUNT, (short) (entryCount + 1));
  }

  // ─── ENTRY SCANNING HELPERS ──────────────────────────────

  /**
   * Returns the total size of an entry starting at the given page offset.
   */
  private int getEntrySize(final BasePage page, final int offset) {
    final int keyLen = computeKeyLengthFromPage(page, offset);
    int total = keyLen;

    if (unique) {
      total += compressedRIDSizeFromPage(page, offset + keyLen);
    } else {
      final int ridCount = readVarIntFromPage(page, offset + keyLen);
      total += varIntSize(ridCount);
      int ridOffset = offset + keyLen + varIntSize(ridCount);
      for (int r = 0; r < ridCount; r++) {
        final int ridSize = compressedRIDSizeFromPage(page, ridOffset);
        total += ridSize;
        ridOffset += ridSize;
      }
    }
    return total;
  }

  /**
   * Same as getEntrySize but for raw byte array.
   */
  private int getEntrySizeFromBytes(final byte[] data, final int offset) {
    final int keyLen = computeKeyLengthFromBytes(data, offset);
    int total = keyLen;

    if (unique) {
      total += compressedRIDSizeFromBytes(data, offset + keyLen);
    } else {
      final int ridCount = readVarIntFromBytes(data, offset + keyLen);
      total += varIntSize(ridCount);
      int ridOffset = offset + keyLen + varIntSize(ridCount);
      for (int r = 0; r < ridCount; r++) {
        final int ridSize = compressedRIDSizeFromBytes(data, ridOffset);
        total += ridSize;
        ridOffset += ridSize;
      }
    }
    return total;
  }

  /**
   * Computes the serialized key length by scanning through all key components.
   */
  private int computeKeyLengthFromPage(final BasePage page, final int startOffset) {
    int offset = startOffset;
    for (int i = 0; i < binaryKeyTypes.length; i++) {
      final byte nullMarker = page.readByte(offset);
      offset += Binary.BYTE_SERIALIZED_SIZE;
      if (nullMarker != 0)
        offset += getSerializedValueSize(page, offset, binaryKeyTypes[i]);
    }
    return offset - startOffset;
  }

  private int computeKeyLengthFromBytes(final byte[] data, final int startOffset) {
    int offset = startOffset;
    for (int i = 0; i < binaryKeyTypes.length; i++) {
      final byte nullMarker = data[offset];
      offset += 1;
      if (nullMarker != 0)
        offset += getSerializedValueSizeFromBytes(data, offset, binaryKeyTypes[i]);
    }
    return offset - startOffset;
  }

  private int computeKeyLengthFromEntry(final byte[] data, final int startOffset) {
    return computeKeyLengthFromBytes(data, startOffset);
  }

  /**
   * Computes how many bytes a serialized value occupies in the page.
   */
  private int getSerializedValueSize(final BasePage page, final int offset, final byte type) {
    switch (type) {
    case BinaryTypes.TYPE_BOOLEAN:
    case BinaryTypes.TYPE_BYTE:
      return 1;
    case BinaryTypes.TYPE_SHORT:
    case BinaryTypes.TYPE_INT:
    case BinaryTypes.TYPE_LONG:
    case BinaryTypes.TYPE_FLOAT:
    case BinaryTypes.TYPE_DOUBLE:
    case BinaryTypes.TYPE_DATE:
    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATETIME_MICROS:
    case BinaryTypes.TYPE_DATETIME_NANOS:
    case BinaryTypes.TYPE_DATETIME_SECOND:
      return getVarNumberSize(page, offset);
    case BinaryTypes.TYPE_STRING:
    case BinaryTypes.TYPE_BINARY: {
      // Length-prefixed: read the varint length, then the bytes
      final int[] lenAndSize = readVarIntAndSize(page, offset);
      return lenAndSize[1] + lenAndSize[0]; // varIntSize + dataLength
    }
    case BinaryTypes.TYPE_COMPRESSED_RID:
      return compressedRIDSizeFromPage(page, offset);
    case BinaryTypes.TYPE_DECIMAL: {
      // scale (varInt) + unscaledValue bytes (length-prefixed)
      final int scaleSize = getVarNumberSize(page, offset);
      final int[] lenAndSize = readVarIntAndSize(page, offset + scaleSize);
      return scaleSize + lenAndSize[1] + lenAndSize[0];
    }
    case BinaryTypes.TYPE_UUID:
      return 16; // Two longs
    default:
      throw new IndexException("Unsupported key type for hash index: " + type);
    }
  }

  private int getSerializedValueSizeFromBytes(final byte[] data, final int offset, final byte type) {
    switch (type) {
    case BinaryTypes.TYPE_BOOLEAN:
    case BinaryTypes.TYPE_BYTE:
      return 1;
    case BinaryTypes.TYPE_SHORT:
    case BinaryTypes.TYPE_INT:
    case BinaryTypes.TYPE_LONG:
    case BinaryTypes.TYPE_FLOAT:
    case BinaryTypes.TYPE_DOUBLE:
    case BinaryTypes.TYPE_DATE:
    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATETIME_MICROS:
    case BinaryTypes.TYPE_DATETIME_NANOS:
    case BinaryTypes.TYPE_DATETIME_SECOND:
      return getVarNumberSizeFromBytes(data, offset);
    case BinaryTypes.TYPE_STRING:
    case BinaryTypes.TYPE_BINARY: {
      final int[] lenAndSize = readVarIntAndSizeFromBytes(data, offset);
      return lenAndSize[1] + lenAndSize[0];
    }
    case BinaryTypes.TYPE_COMPRESSED_RID:
      return compressedRIDSizeFromBytes(data, offset);
    case BinaryTypes.TYPE_DECIMAL: {
      final int scaleSize = getVarNumberSizeFromBytes(data, offset);
      final int[] lenAndSize = readVarIntAndSizeFromBytes(data, offset + scaleSize);
      return scaleSize + lenAndSize[1] + lenAndSize[0];
    }
    case BinaryTypes.TYPE_UUID:
      return 16;
    default:
      throw new IndexException("Unsupported key type for hash index: " + type);
    }
  }

  /**
   * Compares serialized key bytes at the given page offset against the search key bytes.
   */
  private int compareKeyBytes(final BasePage page, final int offset, final byte[] searchKey) {
    final int keyLen = computeKeyLengthFromPage(page, offset);

    // Read page key bytes
    final byte[] pageKey = new byte[keyLen];
    page.readByteArray(offset, pageKey);

    // Compare byte by byte (unsigned)
    return BinaryComparator.compareBytes(pageKey, searchKey);
  }

  /**
   * Checks if the key at the given page offset matches the search key exactly.
   */
  private boolean keysMatch(final BasePage page, final int offset, final byte[] searchKey) {
    return compareKeyBytes(page, offset, searchKey) == 0;
  }

  // ─── COLLECTING ENTRIES ──────────────────────────────────

  /**
   * Collects all raw entry bytes from a bucket page and its overflow chain.
   */
  private List<byte[]> collectAllEntries(final int bucketPageNum, final int entryCount) throws IOException {
    final List<byte[]> entries = new ArrayList<>();
    collectEntriesFromPage(bucketPageNum, entries);
    return entries;
  }

  private void collectEntriesFromPage(final int pageNum, final List<byte[]> entries) throws IOException {
    final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, pageNum), pageSize);
    final int entryCount = page.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
    final int overflowPage = page.readInt(BUCKET_OVERFLOW_PAGE);

    for (int i = 0; i < entryCount; i++) {
      final int offset = readSlot(page, i);
      final int entrySize = getEntrySize(page, offset);
      final byte[] entry = new byte[entrySize];
      page.readByteArray(offset, entry);
      entries.add(entry);
    }

    if (overflowPage != NO_OVERFLOW_PAGE)
      collectEntriesFromPage(overflowPage, entries);
  }

  // ─── RID READING ─────────────────────────────────────────

  private RID readCompressedRID(final BasePage page, final int offset) {
    // Compressed RID: bucketId (varInt) + position (varInt)
    final Binary view = page.getImmutableView(offset, 20); // max ~20 bytes for 2 varInts
    final long bucketId = view.getNumber();
    final long position = view.getNumber();
    return new RID(database, (int) bucketId, position);
  }

  private int compressedRIDSize(final RID rid) {
    return Binary.getNumberSpace(rid.getBucketId()) + Binary.getNumberSpace(rid.getPosition());
  }

  private int compressedRIDSizeFromPage(final BasePage page, final int offset) {
    final Binary view = page.getImmutableView(offset, 20);
    final int startPos = view.position();
    view.getNumber(); // bucketId
    view.getNumber(); // position
    return view.position() - startPos;
  }

  private int compressedRIDSizeFromBytes(final byte[] data, final int offset) {
    final Binary view = new Binary(data);
    view.position(offset);
    view.getNumber(); // bucketId
    view.getNumber(); // position
    return view.position() - offset;
  }

  // ─── VARINT HELPERS ──────────────────────────────────────

  private int readVarIntFromPage(final BasePage page, final int offset) {
    final Binary view = page.getImmutableView(offset, 10);
    return (int) view.getNumber();
  }

  private int readVarIntFromBytes(final byte[] data, final int offset) {
    final Binary view = new Binary(data);
    view.position(offset);
    return (int) view.getNumber();
  }

  /**
   * Returns [dataLength, varIntByteSize].
   */
  private int[] readVarIntAndSize(final BasePage page, final int offset) {
    final Binary view = page.getImmutableView(offset, 10);
    final int startPos = view.position();
    final long value = view.getUnsignedNumber();
    return new int[] { (int) value, view.position() - startPos };
  }

  private int[] readVarIntAndSizeFromBytes(final byte[] data, final int offset) {
    final Binary view = new Binary(data);
    view.position(offset);
    final int startPos = view.position();
    final long value = view.getUnsignedNumber();
    return new int[] { (int) value, view.position() - startPos };
  }

  private int getVarNumberSize(final BasePage page, final int offset) {
    final Binary view = page.getImmutableView(offset, 10);
    final int startPos = view.position();
    view.getNumber();
    return view.position() - startPos;
  }

  private int getVarNumberSizeFromBytes(final byte[] data, final int offset) {
    final Binary view = new Binary(data);
    view.position(offset);
    final int startPos = view.position();
    view.getNumber();
    return view.position() - startPos;
  }

  static int varIntSize(final long value) {
    return Binary.getNumberSpace(value);
  }

  static byte[] encodeVarInt(final long value) {
    final Binary buffer = new Binary(10, false);
    buffer.putNumber(value);
    final byte[] result = new byte[buffer.position()];
    buffer.getByteBuffer().position(0);
    buffer.getByteBuffer().get(result, 0, result.length);
    return result;
  }

  // ─── METADATA WRITING ────────────────────────────────────

  private void updateTotalEntries(final int delta) throws IOException {
    totalEntries += delta;
    final MutablePage metaPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    metaPage.writeInt(META_TOTAL_ENTRIES, totalEntries);
  }

  private void writeGlobalDepth(final int depth) throws IOException {
    final MutablePage metaPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    metaPage.writeInt(META_GLOBAL_DEPTH, depth);
  }

  private void writeBucketCount(final int count) throws IOException {
    final MutablePage metaPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    final int pos = META_KEY_TYPES_START + binaryKeyTypes.length + 2 + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;
    metaPage.writeInt(pos, count);
  }

  private void writeDirectoryStartPage(final int startPage) throws IOException {
    final MutablePage metaPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    final int pos = META_KEY_TYPES_START + binaryKeyTypes.length + 2; // after nullStrategy + unique bytes
    metaPage.writeInt(pos, startPage);
  }
}
