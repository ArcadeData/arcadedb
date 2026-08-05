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
import com.arcadedb.database.TransactionContext;
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

import java.io.File;
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

  // Page size used when the caller does not ask for one. It currently coincides with MAX_PAGE_SIZE, but the two are
  // independent: this one is a tuning choice, MAX_PAGE_SIZE is a hard limit of the on-page addressing.
  public static final int DEF_PAGE_SIZE     = 65_536;
  public static final int CURRENT_VERSION   = 1;
  public static final int NO_OVERFLOW_PAGE  = -1;

  /**
   * Largest page size a bucket page can address (issue #5713).
   * <p>
   * Everything inside a bucket page is addressed with 16-bit fields: the slot directory entries ({@link #SLOT_SIZE}),
   * {@link #BUCKET_DATA_END} and {@link #BUCKET_ENTRY_COUNT} are all written as a {@code short} and read back through
   * {@code & 0xFFFF}, so the largest offset representable is 65535. With the 8-byte {@link BasePage#PAGE_HEADER_SIZE},
   * a 65536-byte page tops out at content offset 65528 and always fits.
   * <p>
   * Above this, the data offsets truncate: {@code dataEnd} wraps back to a low value, {@link #freeSpace} reports the
   * page as almost empty, and the next entry is written over the bucket header - including the overflow pointer at
   * {@link #BUCKET_OVERFLOW_PAGE}, which is how the failure used to surface, as the cycle detector reporting a
   * "corrupted" chain at a wrapped page number rather than as the invalid configuration it is.
   */
  public static final int MAX_PAGE_SIZE = 65_536;

  /**
   * Smallest page size a hash index can be created with.
   * <p>
   * What this floor guarantees is that the index is DESCRIBABLE: the metadata page needs {@code PAGE_HEADER_SIZE +
   * META_KEY_TYPES_START + numberOfKeys + 2 + 3 * INT_SERIALIZED_SIZE} bytes - 95 at the {@link #MAX_SANE_KEY_COUNT}
   * ceiling - so a page size that cannot even hold page 0 is refused up front instead of writing metadata past the end
   * of it.
   * <p>
   * It does NOT guarantee that any given key fits a bucket page. Key width is not known at creation for
   * {@code STRING}/{@code BINARY}/{@code DECIMAL} columns, so no static floor could promise that; an entry too large
   * for an empty page is reported at insert by {@link #entryTooLarge}, which names the usable space per page. That is
   * a property of small pages with wide keys generally, not of this bound.
   */
  public static final int MIN_PAGE_SIZE = 256;

  // Metadata page (page 0) layout offsets (relative to PAGE_HEADER_SIZE)
  static final int META_GLOBAL_DEPTH      = 0;                      // int (4)
  static final int META_TOTAL_ENTRIES      = 4;                      // int (4)
  static final int META_NUMBER_OF_KEYS     = 8;                      // byte (1)
  static final int META_KEY_TYPES_START    = 9;                      // byte[] (variable)
  // After key types: nullStrategy(1), unique(1), dirStartPage(4), bucketsStartPage(4), bucketCount(4)

  // Upper bound on the number of key components used to sanity-check the (possibly corrupt) count read
  // from the metadata page before it is trusted to size arrays and walk the page. Composite indexes have
  // very few columns in practice; this is a generous ceiling that still catches a garbage byte.
  static final int MAX_SANE_KEY_COUNT = 64;

  // Number of times a lookup re-reads the metadata + directory before declaring the index corrupted (#4743).
  static final int MAX_LOOKUP_RETRIES = 3;

  // Upper bound on the problems reported by a single structural check, so a badly damaged index does not build a
  // huge list (the first problems are enough to know it must be rebuilt).
  static final int MAX_REPORTED_PROBLEMS = 20;

  // The schema types usable as a hash index key, listed in the creation-time refusal. Fixed for the life of the JVM.
  private static final String SUPPORTED_KEY_TYPE_NAMES = supportedKeyTypeNames();

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
  // Binary type declared by the schema for each key column: this is what is persisted on the metadata page and
  // what diagnostics report. Use binaryKeyTypes (below) for anything that touches the on-page encoding.
  byte[]  declaredKeyTypes;
  // Binary type actually used to encode each key column on the page. It only differs from the declared one for
  // LINK keys: see storageKeyType().
  byte[]  binaryKeyTypes;
  LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy;

  // NOTE (#4743): the structural metadata (global depth, directory start page, bucket count, total entries) is
  // NEVER cached in instance fields. It lives only on the metadata page (page 0) and is always read through the
  // current transaction. Caching it in fields was a correctness bug: the fields were mutated in the middle of a
  // commit, so (a) concurrent readers saw structural changes that were not published yet (a directory doubling
  // made them read a not-yet-written directory slot, i.e. page 0, which the overflow walker then reported as a
  // cyclic chain), and (b) a rolled back or retried commit left the cached depth permanently ahead of the
  // persisted one, poisoning every later lookup on that index until the database was reopened.
  //
  // Number of key columns never changes after creation, so the offset of the trailing int triplet
  // (dirStartPage, bucketsStartPage, bucketCount) on the metadata page is stable and computed once.
  private int metaTailOffset;

  /**
   * Called at creation time.
   */
  HashIndexBucket(final HashIndex mainIndex, final DatabaseInternal database, final String name, final boolean unique,
      final String filePath, final ComponentFile.MODE mode, final Type[] keyTypes, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) throws IOException {
    // The page size is validated inside the super() argument list on purpose: this constructor is the ONLY path that
    // creates the file, and super() creates it, so checking here - before super() runs - is what guarantees no hash
    // index file can exist with a page size the bucket cannot address (#5713).
    super(database, name, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode,
        checkSupportedPageSize(name, pageSize), CURRENT_VERSION);

    this.mainIndex = mainIndex;
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.keyTypes = checkSupportedKeyTypes(name, keyTypes);
    this.declaredKeyTypes = new byte[keyTypes.length];
    this.binaryKeyTypes = new byte[keyTypes.length];
    for (int i = 0; i < keyTypes.length; i++) {
      this.declaredKeyTypes[i] = keyTypes[i].getBinaryType();
      this.binaryKeyTypes[i] = storageKeyType(this.declaredKeyTypes[i]);
    }
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
      if (!new File(file.getFilePath()).delete())
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
    try {
      return readGlobalDepth();
    } catch (final IOException e) {
      throw new IndexException("Error on reading metadata of hash index '" + getName() + "'", e);
    }
  }

  public int getTotalEntries() {
    try {
      return metaPage().readInt(META_TOTAL_ENTRIES);
    } catch (final IOException e) {
      throw new IndexException("Error on reading metadata of hash index '" + getName() + "'", e);
    }
  }

  // ─── METADATA ACCESS (ALWAYS TRANSACTIONAL, SEE THE NOTE ON metaTailOffset) ───

  private BasePage metaPage() throws IOException {
    return readPage(0);
  }

  /**
   * Reads a page of this index through the current transaction, so the changes of the transaction itself are seen.
   * Outside a transaction (e.g. getStats() from the profiler, CHECK DATABASE on a background thread) it goes
   * straight to the page manager, which returns the same last-committed version the transactional read would.
   */
  private BasePage readPage(final int pageNumber) throws IOException {
    final PageId pageId = new PageId(database, fileId, pageNumber);
    final TransactionContext tx = database.getTransactionIfExists();
    return tx != null ? tx.getPage(pageId, pageSize) : database.getPageManager().getImmutablePage(pageId, pageSize, false, true);
  }

  private int readGlobalDepth() throws IOException {
    return metaPage().readInt(META_GLOBAL_DEPTH);
  }

  private int readDirectoryStartPage() throws IOException {
    return metaPage().readInt(metaTailOffset);
  }

  private int readBucketsStartPage() throws IOException {
    return metaPage().readInt(metaTailOffset + Binary.INT_SERIALIZED_SIZE);
  }

  private int readBucketCount() throws IOException {
    return metaPage().readInt(metaTailOffset + 2 * Binary.INT_SERIALIZED_SIZE);
  }

  /**
   * A directory entry or an overflow pointer must always reference a page that exists in the file and that is not
   * the metadata page (page 0). A pointer outside this range means either a corrupted index or - for a lookup
   * running outside the commit lock - a torn read taken while a concurrent commit was publishing a directory
   * doubling; {@link #get} retries such a lookup before giving up.
   */
  private boolean isValidBucketPage(final int pageNum) {
    return pageNum > 0 && pageNum < getTotalPages();
  }

  // ─── LOOKUP ──────────────────────────────────────────────

  /**
   * Looks up all RIDs for the given key(s).
   */
  List<RID> get(final Object[] keys, final int limit) throws IOException {
    final byte[] serializedKey = serializeKeys(keys);
    final long hash = murmurHash64(serializedKey);

    // A lookup issued outside the commit path holds no file lock. The global depth and the directory start page
    // are read from the same page 0 snapshot, and a doubling publishes a brand new directory region (see
    // doubleDirectory), so the pair is always consistent. An entry can still be read while a split is switching
    // it in place, hence the bounded retry before declaring the index corrupted (#4743).
    for (int attempt = 0; ; attempt++) {
      final BasePage metaPage = metaPage();
      final int dirIndex = directoryIndex(hash, metaPage.readInt(META_GLOBAL_DEPTH));
      final int bucketPageNum = readDirectoryEntry(metaPage.readInt(metaTailOffset), dirIndex);

      if (isValidBucketPage(bucketPageNum))
        return searchBucket(bucketPageNum, serializedKey, limit);

      if (attempt >= MAX_LOOKUP_RETRIES)
        throw new IndexException(
            "Invalid entry " + bucketPageNum + " at position " + dirIndex + " in the directory of hash index '" + getName()
                + "' (fileId=" + fileId + ", totalPages=" + getTotalPages()
                + "). The index is corrupted, please rebuild it (DROP and recreate it).");
    }
  }

  /**
   * Searches a bucket page (and its overflow chain) for entries matching the given keys.
   */
  private List<RID> searchBucket(final int bucketPageNum, final byte[] searchKey,
      final int limit) throws IOException {
    final List<RID> result = new ArrayList<>();
    int currentPage = bucketPageNum;

    // Guard against a corrupted (cyclic) overflow chain: without this a chain that loops back on itself spins
    // this loop forever, pinning a CPU core at 100% inside getPage() and never returning (issue #4743).
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;

    while (currentPage != NO_OVERFLOW_PAGE) {
      if (++chainSteps > maxChainPages || !isValidBucketPage(currentPage))
        throw corruptedOverflowChain(currentPage);
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
    final BasePage metaPage = metaPage();
    final int globalDepth = metaPage.readInt(META_GLOBAL_DEPTH);
    final int dirIndex = directoryIndex(hash, globalDepth);
    final int bucketPageNum = readDirectoryEntry(metaPage.readInt(metaTailOffset), dirIndex);

    if (!isValidBucketPage(bucketPageNum))
      throw new IndexException(
          "Invalid entry " + bucketPageNum + " at position " + dirIndex + " in the directory of hash index '" + getName()
              + "' (fileId=" + fileId + ", totalPages=" + getTotalPages()
              + "). The index is corrupted, please rebuild it (DROP and recreate it).");

    final MutablePage bucketPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, bucketPageNum), pageSize, false);
    final int entryCount = bucketPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
    final int localDepth = bucketPage.readShort(BUCKET_LOCAL_DEPTH) & 0xFFFF;

    final byte[] serializedRID = serializeCompressedRID(rid);

    // For non-unique indexes, check if key already exists (on primary page or overflow chain) and append RID
    if (!unique) {
      int currentPageNum = bucketPageNum;
      final int maxChainPages = getTotalPages();
      int chainSteps = 0;
      while (currentPageNum != NO_OVERFLOW_PAGE) {
        if (++chainSteps > maxChainPages || !isValidBucketPage(currentPageNum))
          throw corruptedOverflowChain(currentPageNum);
        final MutablePage currentPage = database.getTransaction()
            .getPageToModify(new PageId(database, fileId, currentPageNum), pageSize, false);
        final int currentEntryCount = currentPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
        final int existingPos = findExactEntry(currentPage, currentEntryCount, serializedKey);
        if (existingPos >= 0) {
          addRIDToExistingEntry(currentPageNum, currentPage, currentEntryCount, existingPos, serializedKey, serializedRID);
          updateTotalEntries(1);
          return;
        }
        currentPageNum = currentPage.readInt(BUCKET_OVERFLOW_PAGE);
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
    final BasePage metaPage = metaPage();
    final int dirIndex = directoryIndex(hash, metaPage.readInt(META_GLOBAL_DEPTH));
    final int bucketPageNum = readDirectoryEntry(metaPage.readInt(metaTailOffset), dirIndex);

    removeFromBucket(bucketPageNum, serializedKey, null);
  }

  /**
   * Removes a specific key-RID pair.
   */
  void remove(final Object[] keys, final RID rid) throws IOException {
    final byte[] serializedKey = serializeKeys(keys);
    final long hash = murmurHash64(serializedKey);
    final BasePage metaPage = metaPage();
    final int dirIndex = directoryIndex(hash, metaPage.readInt(META_GLOBAL_DEPTH));
    final int bucketPageNum = readDirectoryEntry(metaPage.readInt(metaTailOffset), dirIndex);

    removeFromBucket(bucketPageNum, serializedKey, rid);
  }

  private void removeFromBucket(final int bucketPageNum, final byte[] serializedKey, final RID specificRID) throws IOException {
    int currentPageNum = bucketPageNum;
    int totalRemoved = 0;
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;

    while (currentPageNum != NO_OVERFLOW_PAGE) {
      if (++chainSteps > maxChainPages || !isValidBucketPage(currentPageNum))
        throw corruptedOverflowChain(currentPageNum);
      final MutablePage page = database.getTransaction()
          .getPageToModify(new PageId(database, fileId, currentPageNum), pageSize, false);
      int entryCount = page.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
      final int overflowPage = page.readInt(BUCKET_OVERFLOW_PAGE);

      final int pos = findExactEntry(page, entryCount, serializedKey);
      if (pos >= 0) {
        if (specificRID != null && !unique) {
          // Search all matching entries on this page (entries for the same key may be split)
          for (int p = pos; p < entryCount; p++) {
            if (!keysMatch(page, readSlot(page, p), serializedKey))
              break;
            final int removed = removeRIDFromEntry(page, entryCount, p, specificRID);
            if (removed > 0) {
              updateTotalEntries(-removed);
              return;
            }
          }
          // RID not in any entry on this page - continue to overflow pages
        } else {
          // Remove all entries matching the key on this page. For unique indexes only one
          // exists in the whole chain; for non-unique indexes, entries for the same key
          // may be split across multiple overflow pages (see addRIDToExistingEntry when
          // space runs out), so we must keep scanning subsequent pages too.
          int p = pos;
          while (p < entryCount && keysMatch(page, readSlot(page, p), serializedKey)) {
            totalRemoved += removeEntryFromPage(page, entryCount, p);
            entryCount--;
            // next entry has shifted into position p, do not advance
          }
          if (unique) {
            updateTotalEntries(-totalRemoved);
            return;
          }
        }
      }

      currentPageNum = overflowPage;
    }

    if (totalRemoved > 0)
      updateTotalEntries(-totalRemoved);
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
    final int effectiveGlobalDepth = Math.max(readGlobalDepth(), newLocalDepth);
    final int splitBit = 1 << (effectiveGlobalDepth - newLocalDepth);

    boolean seenZero = false;
    boolean seenOne = false;

    // Check entries in main page and overflow
    int currentPage = bucketPageNum;
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;
    while (currentPage != NO_OVERFLOW_PAGE) {
      if (++chainSteps > maxChainPages || !isValidBucketPage(currentPage))
        throw corruptedOverflowChain(currentPage);
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
    if (newLocalDepth > readGlobalDepth())
      doubleDirectory();

    final int globalDepth = readGlobalDepth();

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
    final int directoryStartPage = readDirectoryStartPage();
    for (final byte[] entry : allEntries) {
      final long entryHash = hashSerializedKey(entry);
      final int newDirIndex = directoryIndex(entryHash, globalDepth);
      final int targetBucketPage = readDirectoryEntry(directoryStartPage, newDirIndex);
      insertRawEntry(targetBucketPage, entry);
    }
  }

  /**
   * Doubles the directory, incrementing globalDepth.
   * <p>
   * The doubled directory is always written to a freshly allocated region at the end of the file (copy on write)
   * instead of being expanded in place. This is what makes a directory doubling atomic for a lookup that runs
   * without the file lock (#4743): the old region is never touched, and the switch to the new one is the publish
   * of a SINGLE page - page 0, which carries both the new global depth and the new directory start page. A reader
   * therefore always sees either (old depth, old region) or (new depth, new region), never a mix of the two. The
   * pages of the old region are left behind and reclaimed by the next index rebuild.
   */
  private void doubleDirectory() throws IOException {
    final BasePage metaPage = metaPage();
    final int oldDepth = metaPage.readInt(META_GLOBAL_DEPTH);
    final int oldDirectoryStartPage = metaPage.readInt(metaTailOffset);
    final int oldSize = 1 << oldDepth;
    final int newSize = oldSize * 2;

    // Read old directory entries
    final int[] oldEntries = new int[oldSize];
    for (int i = 0; i < oldSize; i++)
      oldEntries[i] = readDirectoryEntry(oldDirectoryStartPage, i);

    final int entriesPerPage = directoryEntriesPerPage();
    final int neededPages = (newSize + entriesPerPage - 1) / entriesPerPage;

    final int newDirectoryStartPage = getTotalPages();
    for (int i = 0; i < neededPages; i++) {
      database.getTransaction().addPage(new PageId(database, fileId, newDirectoryStartPage + i), pageSize);
      updatePageCount(newDirectoryStartPage + i + 1);
    }

    // Write doubled directory: each old entry is duplicated
    for (int i = 0; i < oldSize; i++) {
      writeDirectoryEntry(newDirectoryStartPage, 2 * i, oldEntries[i]);
      writeDirectoryEntry(newDirectoryStartPage, 2 * i + 1, oldEntries[i]);
    }

    // Update metadata: both fields live on page 0, so they become visible together
    writeDirectoryStartPage(newDirectoryStartPage);
    writeGlobalDepth(oldDepth + 1);
  }

  // ─── OVERFLOW PAGES ──────────────────────────────────────

  private void insertIntoOverflow(MutablePage currentPage, int currentPageNum,
      final byte[] serializedKey, final byte[] serializedRID) throws IOException {
    final int entryDataSize = unique ?
        serializedKey.length + serializedRID.length :
        serializedKey.length + varIntSize(1) + serializedRID.length;
    final int totalNeeded = entryDataSize + SLOT_SIZE;

    // Defensive cycle detection: a corrupted overflow chain that loops back to a previously-seen page would
    // otherwise spin forever. A valid chain visits distinct pages, so it cannot be longer than the file: the
    // bounded-step guard is equivalent to tracking the visited pages, without allocating on the insert path.
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;

    while (true) {
      int overflowPageNum = currentPage.readInt(BUCKET_OVERFLOW_PAGE);

      if (overflowPageNum == NO_OVERFLOW_PAGE) {
        final int localDepth = currentPage.readShort(BUCKET_LOCAL_DEPTH) & 0xFFFF;
        overflowPageNum = allocateOverflowPage(localDepth);
        currentPage.writeInt(BUCKET_OVERFLOW_PAGE, overflowPageNum);
      }

      if (++chainSteps > maxChainPages || !isValidBucketPage(overflowPageNum))
        throw corruptedOverflowChain(overflowPageNum);

      final MutablePage overflowPage = database.getTransaction()
          .getPageToModify(new PageId(database, fileId, overflowPageNum), pageSize, false);
      final int entryCount = overflowPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;

      if (totalNeeded <= freeSpace(overflowPage, entryCount)) {
        insertEntryInPage(overflowPage, entryCount, serializedKey, serializedRID);
        return;
      }

      if (entryCount == 0)
        throw entryTooLarge(totalNeeded, freeSpace(overflowPage, 0));

      // Chain to the next overflow page
      currentPage = overflowPage;
      currentPageNum = overflowPageNum;
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

    writeBucketCount(readBucketCount() + 1);

    return newPageNum;
  }

  private int allocateOverflowPage(final int localDepth) throws IOException {
    return allocateBucketPage(localDepth);
  }

  // ─── INITIALIZATION ──────────────────────────────────────

  private void initializeNewIndex() throws IOException {
    this.metaTailOffset = META_KEY_TYPES_START + declaredKeyTypes.length + 2 * Binary.BYTE_SERIALIZED_SIZE;

    // Page 0: metadata
    final MutablePage metaPage = database.getTransaction().addPage(new PageId(database, fileId, 0), pageSize);
    updatePageCount(1);

    int pos = META_GLOBAL_DEPTH;
    metaPage.writeInt(pos, 0);                           // globalDepth = 0
    pos += Binary.INT_SERIALIZED_SIZE;
    metaPage.writeInt(pos, 0);                           // totalEntries = 0
    pos += Binary.INT_SERIALIZED_SIZE;
    metaPage.writeByte(pos, (byte) declaredKeyTypes.length); // numberOfKeys
    pos += Binary.BYTE_SERIALIZED_SIZE;
    // Persist the SCHEMA type, not the storage type, so the metadata page keeps describing the index the same way
    // the schema does and the storage encoding stays an internal detail of this class.
    for (final byte declaredKeyType : declaredKeyTypes) {
      metaPage.writeByte(pos, declaredKeyType);
      pos += Binary.BYTE_SERIALIZED_SIZE;
    }
    metaPage.writeByte(pos, (byte) nullStrategy.ordinal());
    pos += Binary.BYTE_SERIALIZED_SIZE;
    metaPage.writeByte(pos, (byte) (unique ? 1 : 0));
    pos += Binary.BYTE_SERIALIZED_SIZE;

    metaPage.writeInt(pos, 1); // directoryStartPage = 1
    pos += Binary.INT_SERIALIZED_SIZE;

    metaPage.writeInt(pos, 2); // bucketsStartPage = 2
    pos += Binary.INT_SERIALIZED_SIZE;

    metaPage.writeInt(pos, 0); // bucketCount = 0

    // Page 1: directory (initially 1 entry pointing to bucket at page 2)
    final MutablePage dirPage = database.getTransaction().addPage(new PageId(database, fileId, 1), pageSize);
    updatePageCount(2);
    dirPage.writeInt(0, 2); // directory[0] → bucket page 2

    // Page 2: initial bucket (local depth 0, empty)
    final MutablePage bucketPage = database.getTransaction().addPage(new PageId(database, fileId, 2), pageSize);
    updatePageCount(3);
    bucketPage.writeShort(BUCKET_LOCAL_DEPTH, (short) 0);
    bucketPage.writeShort(BUCKET_ENTRY_COUNT, (short) 0);
    bucketPage.writeInt(BUCKET_OVERFLOW_PAGE, NO_OVERFLOW_PAGE);
    bucketPage.writeShort(BUCKET_DATA_END, (short) BUCKET_CONTENT_START);

    writeBucketCount(1);
  }

  /**
   * Loads the immutable part of the metadata (key types + null strategy) and computes the offset of the trailing
   * int triplet on the metadata page. The structural counters (global depth, directory start page, bucket count,
   * total entries) are intentionally NOT cached here: see the note on {@link #metaTailOffset}.
   */
  private void loadMetadata() throws IOException {
    final BasePage metaPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);

    final int rawNumKeys = metaPage.readByte(META_NUMBER_OF_KEYS) & 0xFF;

    // Sanity-guard the key count read from the (possibly corrupt) metadata page before using it to size
    // arrays and walk the page. Without this a garbage count byte would either blow up with an
    // IndexOutOfBounds deep in this loop, or - far worse - silently load an invalid key type that only
    // surfaces much later as the cryptic "Unsupported key type for hash index: -108" during a search
    // (issue #352). Clamping to 0 keeps the database openable so the corrupt index can be dropped/rebuilt.
    final int maxKeysForPage = (metaPage.getMaxContentSize() - META_KEY_TYPES_START) / Binary.BYTE_SERIALIZED_SIZE;
    final boolean numKeysCorrupt = rawNumKeys < 1 || rawNumKeys > MAX_SANE_KEY_COUNT || rawNumKeys > maxKeysForPage;
    final int numKeys = numKeysCorrupt ? 0 : rawNumKeys;

    boolean metadataCorrupt = numKeysCorrupt;

    int pos = META_KEY_TYPES_START;
    declaredKeyTypes = new byte[numKeys];
    binaryKeyTypes = new byte[numKeys];
    keyTypes = new Type[numKeys];
    for (int i = 0; i < numKeys; i++) {
      declaredKeyTypes[i] = metaPage.readByte(pos);
      binaryKeyTypes[i] = storageKeyType(declaredKeyTypes[i]);
      keyTypes[i] = Type.getByBinaryType(declaredKeyTypes[i]);
      if (!isSupportedKeyType(declaredKeyTypes[i]))
        metadataCorrupt = true;
      pos += Binary.BYTE_SERIALIZED_SIZE;
    }

    final int nullStrategyOrdinal = metaPage.readByte(pos) & 0xFF;
    final LSMTreeIndexAbstract.NULL_STRATEGY[] strategies = LSMTreeIndexAbstract.NULL_STRATEGY.values();
    if (nullStrategyOrdinal < strategies.length)
      nullStrategy = strategies[nullStrategyOrdinal];
    else {
      nullStrategy = LSMTreeIndexAbstract.NULL_STRATEGY.SKIP;
      metadataCorrupt = true;
    }
    pos += Binary.BYTE_SERIALIZED_SIZE;
    // unique is already set from constructor
    pos += Binary.BYTE_SERIALIZED_SIZE;

    // Offset of the trailing int triplet (dirStartPage, bucketsStartPage, bucketCount): stable for the lifetime
    // of the index because the number of key columns never changes.
    this.metaTailOffset = pos;

    if (metadataCorrupt)
      LogManager.instance().log(this, Level.SEVERE,
          "Corrupted metadata detected on hash index '%s' (fileId=%d): %s. The index must be rebuilt (DROP and "
              + "recreate it). Raw metadata page 0 dump (content bytes):%n%s",
          null, getName(), fileId, describeMetadata(metaPage, rawNumKeys), dumpMetadataPage(metaPage));

    // An index created before the creation-time check of #5713 can carry a page size outside the supported range on
    // disk. Report it rather than throw: throwing here would make the whole database unopenable, while the index is
    // still droppable and rebuildable - and CHECK DATABASE surfaces the same problem through checkMetadataIntegrity().
    //
    // WHY WRITES ARE STILL ACCEPTED afterwards, rather than the component being marked read-only: an OVERSIZED index is
    // already unusable in practice, not quietly degrading. Every offset on its bucket pages has wrapped, so the first
    // lookup - including the unique-constraint probe an insert performs - walks the overwritten overflow pointer and
    // raises corruptedOverflowChain(). The failure is loud on the read side, which is where it would matter, so a
    // write-side block would mostly convert one loud error into a different loud error while removing the operator's
    // ability to keep the type usable until the rebuild window. The rebuild itself is unaffected either way: it scans
    // the records and populates a NEW file, never writing through this bucket. An UNDERSIZED index is not damaged at
    // all (see describeUnsupportedPageSize), so there is nothing to protect it from.
    if (!isSupportedPageSize(pageSize))
      // Severity follows the finding, not the fact that something was found: only the oversized case has actually
      // damaged data. Logging SEVERE for a page size we simultaneously describe as probably still working would
      // contradict the message on every open.
      LogManager.instance().log(this, isPageSizeDamaging(pageSize) ? Level.SEVERE : Level.WARNING,
          "Hash index '%s' (fileId=%d) has an unsupported page size of %d bytes (allowed: %d..%d): %s. It should be "
              + "rebuilt (DROP and recreate it, or REBUILD INDEX) with a supported page size.",
          null, getName(), fileId, pageSize, MIN_PAGE_SIZE, MAX_PAGE_SIZE, describeUnsupportedPageSize(pageSize));
  }

  /**
   * Whether an out-of-range page size means the index has ALREADY been damaged, as opposed to merely being outside the
   * range this index type now accepts. Only the oversized direction wraps the 16-bit on-page offsets; see
   * {@link #describeUnsupportedPageSize}.
   */
  private static boolean isPageSizeDamaging(final int pageSize) {
    return pageSize > MAX_PAGE_SIZE;
  }

  /**
   * Explains what an out-of-range page size means for THIS index, because the two directions are not the same problem
   * and must not be reported as if they were.
   * <p>
   * Above {@link #MAX_PAGE_SIZE} the 16-bit on-page offsets have wrapped, so the index really is damaged. Below
   * {@link #MIN_PAGE_SIZE} nothing has wrapped: the old creation path accepted any {@code pageSize > 0}, so such an
   * index may well have been working, and telling its operator it is "corrupted" would send them hunting for damage
   * that is not there. What is true in that case is only that the page is below the floor the index now requires to
   * guarantee its metadata page fits.
   * <p>
   * This is the ONE place that wording lives: the load-time log and {@link #checkMetadataIntegrity} both report through
   * it, so the two cannot drift into describing the same file differently.
   */
  private static String describeUnsupportedPageSize(final int pageSize) {
    return isPageSizeDamaging(pageSize) ?
        "bucket pages address entries with 16-bit offsets, so this index is damaged" :
        "below the minimum required for the metadata page, though the index may still be working";
  }

  /**
   * Returns true if the given SCHEMA binary type is one this hash index can serialize/deserialize as a key
   * component. Used to refuse an unsupported type at creation ({@link #checkSupportedKeyTypes}) and to validate the
   * key types loaded from the metadata page, so corruption is detected up front instead of deep in a search.
   * <p>
   * The cases mirror those of {@link #getSerializedValueSize} after {@link #storageKeyType} has been applied, which
   * is why {@code TYPE_RID} is accepted here but absent there: it is stored as {@code TYPE_COMPRESSED_RID}.
   * <p>
   * Two of the accepted types have no {@link Type} constant that maps to them, so they cannot be declared through the
   * schema and never appear in {@link #supportedKeyTypeNames}: {@code TYPE_COMPRESSED_RID}, which only reaches this
   * method as a storage type, and {@code TYPE_UUID}. They stay accepted so a metadata page carrying one is not
   * mistaken for corruption.
   */
  static boolean isSupportedKeyType(final byte type) {
    switch (type) {
    case BinaryTypes.TYPE_BOOLEAN:
    case BinaryTypes.TYPE_BYTE:
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
    case BinaryTypes.TYPE_STRING:
    case BinaryTypes.TYPE_BINARY:
    case BinaryTypes.TYPE_COMPRESSED_RID:
    case BinaryTypes.TYPE_RID:
    case BinaryTypes.TYPE_DECIMAL:
    case BinaryTypes.TYPE_UUID:
      return true;
    default:
      return false;
    }
  }

  /**
   * Maps a schema binary type to the binary type this index actually writes on the page.
   * <p>
   * The only remapping is {@link BinaryTypes#TYPE_RID} (the encoding of {@link Type#LINK}, and therefore of an edge
   * type's {@code @out}/{@code @in} endpoints) to {@link BinaryTypes#TYPE_COMPRESSED_RID}: both encodings are
   * deterministic and injective, so hashing and byte comparison are unaffected, but the fixed 4+8 byte form costs 12
   * bytes per column against the 2-7 of the varint form the bucket already uses for entry values. On the composite
   * {@code (@out,@in)} key that is the point of an endpoint-keyed unique index, that is roughly half the key bytes -
   * and hence half the pages - for free. See issue #5677.
   */
  static byte storageKeyType(final byte declaredBinaryType) {
    return declaredBinaryType == BinaryTypes.TYPE_RID ? BinaryTypes.TYPE_COMPRESSED_RID : declaredBinaryType;
  }

  /**
   * Validates the key types a hash index is about to be created with and returns them unchanged, so they can be
   * assigned directly in the creation constructor. An unsupported one is refused up front with a message naming it,
   * instead of surfacing as an "unsupported key type" deep inside the first insert (#5677).
   * <p>
   * The creation constructor of this class calls it, and that is what makes the refusal unbypassable: it is the only
   * path that writes the metadata page, so no caller can declare a key type the bucket cannot encode and leave
   * {@link #loadMetadata} to report it as corruption on the next open. The load constructor deliberately does NOT
   * call it - an index already on disk must keep opening, and {@code loadMetadata} already validates what it reads.
   * <p>
   * {@code HashIndexFactoryHandler.create()} calls it as well, and that call is not redundant. Unlike
   * {@link #checkSupportedPageSize}, which is evaluated in the {@code super()} argument list and therefore before the
   * file exists, this one can only run after {@code super()} has already created and registered it. Refusing in the
   * handler is what keeps the ordinary creation path from leaving an empty file behind.
   */
  static Type[] checkSupportedKeyTypes(final String indexName, final Type[] keyTypes) {
    if (keyTypes == null)
      return null;
    for (final Type keyType : keyTypes)
      if (keyType == null || !isSupportedKeyType(keyType.getBinaryType()))
        throw new IndexException(
            "Cannot create index '" + indexName + "' of type HASH because "
                // name(), not toString(): the supported set below is built from name(), and an implicit dependency on
                // Type not overriding toString() would silently report the two in different spellings.
                + (keyType == null ? "a key column has no type" : "the key type " + keyType.name() + " cannot be used")
                + " as a HASH index key. Supported key types are: " + SUPPORTED_KEY_TYPE_NAMES
                + ". Create the index as LSM_TREE instead");
    return keyTypes;
  }

  /**
   * Returns true if a bucket page of the given size can be addressed by the 16-bit on-page fields (and is large enough
   * to hold the metadata page). See {@link #MAX_PAGE_SIZE} and {@link #MIN_PAGE_SIZE}.
   */
  static boolean isSupportedPageSize(final int pageSize) {
    return pageSize >= MIN_PAGE_SIZE && pageSize <= MAX_PAGE_SIZE;
  }

  /**
   * Validates the page size a hash index is about to be created with and returns it unchanged, so it can be used
   * directly as a {@code super()} argument (issue #5713).
   * <p>
   * Refusing here rather than letting the insert path wrap makes the failure name the configuration instead of
   * reporting the index as corrupted after the fact: an oversized page silently truncates every data offset to 16
   * bits, and the first symptom is an overflow chain that has been overwritten into a cycle.
   */
  static int checkSupportedPageSize(final String indexName, final int pageSize) {
    if (!isSupportedPageSize(pageSize))
      throw new IndexException(
          "Cannot create index '" + indexName + "' of type HASH with a page size of " + pageSize
              + " bytes: a hash bucket page addresses its entries with 16-bit offsets, so the page size must be between "
              + MIN_PAGE_SIZE + " and " + MAX_PAGE_SIZE + " bytes. Use LSM_TREE if a bigger page is required");
    return pageSize;
  }

  /**
   * Comma-separated list of the schema types usable as a hash index key, for the creation-time error message. The set
   * is fixed at class-load time, so it is built once rather than on every refusal.
   */
  private static String supportedKeyTypeNames() {
    final StringBuilder buffer = new StringBuilder(128);
    for (final Type type : Type.values())
      if (isSupportedKeyType(type.getBinaryType())) {
        if (!buffer.isEmpty())
          buffer.append(", ");
        buffer.append(type.name());
      }
    return buffer.toString();
  }

  /**
   * Human-readable one-line summary of the parsed metadata fields, used in corruption diagnostics.
   */
  private String describeMetadata(final BasePage metaPage, final int rawNumKeys) {
    return "globalDepth=" + metaPage.readInt(META_GLOBAL_DEPTH) + ", totalEntries=" + metaPage.readInt(META_TOTAL_ENTRIES)
        + ", numberOfKeys=" + rawNumKeys + ", keyTypes=" + formatKeyTypes()
        + ", directoryStartPage=" + metaPage.readInt(metaTailOffset)
        + ", bucketsStartPage=" + metaPage.readInt(metaTailOffset + Binary.INT_SERIALIZED_SIZE)
        + ", bucketCount=" + metaPage.readInt(metaTailOffset + 2 * Binary.INT_SERIALIZED_SIZE) + ", unique=" + unique;
  }

  /**
   * Formats the loaded key types as signed value + hex, flagging any that are not valid hash index key types.
   */
  private String formatKeyTypes() {
    final StringBuilder buffer = new StringBuilder(2 + declaredKeyTypes.length * 12);
    buffer.append('[');
    for (int i = 0; i < declaredKeyTypes.length; i++) {
      if (i > 0)
        buffer.append(", ");
      final byte type = declaredKeyTypes[i];
      buffer.append(type).append("(0x").append(String.format("%02X", type & 0xFF)).append(')');
      if (!isSupportedKeyType(type))
        buffer.append("=INVALID");
    }
    return buffer.append(']').toString();
  }

  /**
   * Hex dump of the leading content bytes of the metadata page, so the actual on-disk bytes are captured
   * in the log when corruption is detected.
   */
  private String dumpMetadataPage(final BasePage page) {
    final int len = Math.min(page.getMaxContentSize(), 48);
    final StringBuilder hex = new StringBuilder(len * 3);
    for (int i = 0; i < len; i++) {
      if (i > 0 && i % 16 == 0)
        hex.append('\n');
      hex.append(String.format("%02X ", page.readByte(i) & 0xFF));
    }
    return hex.toString();
  }

  /**
   * Builds a rich, actionable exception for a key column whose type this index cannot encode.
   * <p>
   * The offending type is the one loaded from the metadata page for that column, NOT a byte read out of the entry
   * being walked, so this is never evidence that entry's bytes are damaged. Since {@link #checkSupportedKeyTypes}
   * refuses an unsupported type at creation, reaching this point means the metadata page itself no longer describes
   * a valid index - either it is corrupted, or the index predates that check. Both are fixed by recreating the index,
   * so the message says so without claiming the stored records are damaged. The bare type value (e.g. -108) is
   * meaningless on its own; the index identity, the column, the loaded types and the parse position are added.
   */
  private IndexException unsupportedKeyType(final byte type, final int column, final int offset) {
    return new IndexException(
        "Key column " + column + " of hash index '" + getName() + "' (fileId=" + fileId + ") has type " + type + " (0x"
            + String.format("%02X", type & 0xFF) + "), which a hash index cannot encode (hit while parsing an entry at "
            + "content offset " + offset + "). Declared key types=" + formatKeyTypes()
            + ". No record data is lost: either the index metadata page is damaged, or the index was created on a "
            + "property type HASH does not support. Drop it and recreate it, as LSM_TREE if the key type is not in: "
            + SUPPORTED_KEY_TYPE_NAMES);
  }

  /**
   * Builds the exception thrown when an overflow-chain walk exceeds the number of pages in the file, which can only
   * happen if the chain is cyclic (a page's overflow pointer eventually loops back to an already-visited page).
   * The read/scan walkers ({@link #searchBucket}, non-unique {@code putInternal}, {@code removeFromBucket},
   * {@code canSplitHelp}, {@code collectEntriesFromPage}) use this bounded-step guard rather than the allocating
   * {@code visited} set used by {@link #insertIntoOverflow}/{@link #insertRawEntry}, because {@code searchBucket}
   * runs on the unique-constraint hot path of every insert and must not allocate per lookup. A valid chain visits
   * distinct pages, so it can never be longer than {@link #getTotalPages()}. See issue #4743.
   */
  private IndexException corruptedOverflowChain(final int page) {
    return new IndexException(
        "Detected cycle in hash index '" + getName() + "' (fileId=" + fileId + ") overflow chain at page " + page
            + " (totalPages=" + getTotalPages() + "). The index is corrupted, please rebuild it (DROP and recreate it).");
  }

  /**
   * A single entry (key + RID) never fits in an empty page: chaining another overflow page would not help, so fail
   * with an actionable message instead of allocating pages forever.
   */
  private IndexException entryTooLarge(final int entrySize, final int pageCapacity) {
    return new IndexException(
        "Entry of " + entrySize + " bytes does not fit in a page of hash index '" + getName() + "' (fileId=" + fileId
            + ", usable space per page=" + pageCapacity + " bytes). Use a smaller key or create the index with a bigger "
            + "page size.");
  }

  /**
   * Walks the directory and every overflow chain to verify that the index structure is sound: entries point to
   * existing bucket pages, chains are acyclic and no page belongs to two different chains. Used by CHECK DATABASE
   * to detect a corrupted index up front (issue #4743) instead of waiting for a query to hit the broken chain.
   */
  public List<String> checkStructuralIntegrity() {
    final List<String> problems = new ArrayList<>();
    try {
      final BasePage metaPage = metaPage();
      final int globalDepth = metaPage.readInt(META_GLOBAL_DEPTH);
      final int directoryStartPage = metaPage.readInt(metaTailOffset);
      final int totalPages = getTotalPages();

      if (globalDepth < 0 || globalDepth > 30) {
        problems.add("invalid globalDepth=" + globalDepth);
        return problems;
      }

      // chainOwner[p] = head page of the chain page p belongs to, plus 1 (0 = not visited yet)
      final int[] chainOwner = new int[totalPages];
      final int directorySize = 1 << globalDepth;
      int previousHead = -1;

      for (int i = 0; i < directorySize && problems.size() < MAX_REPORTED_PROBLEMS; i++) {
        final int head = readDirectoryEntry(directoryStartPage, i);
        if (!isValidBucketPage(head)) {
          problems.add("directory entry " + i + " points to the invalid page " + head + " (totalPages=" + totalPages + ")");
          continue;
        }

        // consecutive entries usually share the same bucket: skip the chains already walked
        if (head == previousHead || chainOwner[head] == head + 1)
          continue;
        previousHead = head;

        int current = head;
        while (current != NO_OVERFLOW_PAGE) {
          if (!isValidBucketPage(current)) {
            problems.add("chain of bucket " + head + " reaches the invalid page " + current + " (totalPages=" + totalPages + ")");
            break;
          }
          if (chainOwner[current] == head + 1) {
            problems.add("chain of bucket " + head + " is cyclic: page " + current + " is visited twice");
            break;
          }
          if (chainOwner[current] != 0) {
            problems.add("page " + current + " belongs to both the chain of bucket " + (chainOwner[current] - 1)
                + " and the chain of bucket " + head);
            break;
          }
          chainOwner[current] = head + 1;
          current = readPage(current).readInt(BUCKET_OVERFLOW_PAGE);
        }
      }
    } catch (final Exception e) {
      problems.add("error while walking the index structure: " + e.getMessage());
    }

    if (!problems.isEmpty())
      LogManager.instance().log(this, Level.SEVERE,
          "CHECK DATABASE found a corrupted structure on hash index '%s' (fileId=%d): %s. The index must be rebuilt "
              + "(DROP and recreate it).", null, getName(), fileId, problems);

    return problems;
  }

  /**
   * Validates the loaded metadata (key types, directory/bucket page pointers, counters) and returns a list of
   * human-readable problems; an empty list means healthy. Independent of record content, so CHECK DATABASE can
   * surface a corrupt metadata page (issue #352) proactively instead of waiting for a query to fail with a
   * cryptic "Unsupported key type for hash index: -108".
   */
  public List<String> checkMetadataIntegrity() {
    final List<String> problems = new ArrayList<>();

    // A page size outside the addressable range is a property of the file, not of page 0, but it belongs here: it makes
    // every offset on every bucket page unreliable, so reporting it first stops the structural walk below from
    // attributing the resulting garbage to a cyclic chain (#5713).
    if (!isSupportedPageSize(pageSize))
      problems.add("unsupported page size=" + pageSize + " (allowed: " + MIN_PAGE_SIZE + ".." + MAX_PAGE_SIZE + "): "
          + describeUnsupportedPageSize(pageSize));

    if (declaredKeyTypes == null || declaredKeyTypes.length == 0)
      problems.add("no key types loaded (the metadata page reports an invalid key count)");
    else
      for (int i = 0; i < declaredKeyTypes.length; i++)
        if (!isSupportedKeyType(declaredKeyTypes[i]))
          problems.add("invalid key type at column " + i + ": " + declaredKeyTypes[i] + " (0x"
              + String.format("%02X", declaredKeyTypes[i] & 0xFF) + ")");

    try {
      final int globalDepth = readGlobalDepth();
      final int directoryStartPage = readDirectoryStartPage();
      final int bucketsStartPage = readBucketsStartPage();
      final int bucketCount = readBucketCount();

      if (globalDepth < 0)
        problems.add("invalid globalDepth=" + globalDepth);
      if (directoryStartPage < 1)
        problems.add("invalid directoryStartPage=" + directoryStartPage);
      if (bucketsStartPage < 1)
        problems.add("invalid bucketsStartPage=" + bucketsStartPage);
      if (bucketCount < 1)
        problems.add("invalid bucketCount=" + bucketCount);
    } catch (final IOException e) {
      problems.add("cannot read the metadata page: " + e.getMessage());
    }

    if (!problems.isEmpty()) {
      // An undersized page size is the one finding here that does NOT mean the metadata is damaged, so when it is the
      // only thing found this must not announce corruption - the surrounding sentence would then contradict the very
      // problem it is wrapping, which reads as "may still be working" (#5713).
      final boolean corrupted = problems.size() > 1 || isSupportedPageSize(pageSize) || isPageSizeDamaging(pageSize);
      if (corrupted)
        LogManager.instance().log(this, Level.SEVERE,
            "CHECK DATABASE found corrupted metadata on hash index '%s' (fileId=%d): %s. The index must be rebuilt "
                + "(DROP and recreate it).", null, getName(), fileId, problems);
      else
        LogManager.instance().log(this, Level.WARNING,
            "CHECK DATABASE found an unsupported configuration on hash index '%s' (fileId=%d): %s. The index should be "
                + "rebuilt (DROP and recreate it, or REBUILD INDEX).", null, getName(), fileId, problems);

      // the structural walk relies on the metadata being sane: no point in running it on a corrupted page 0
      return problems;
    }

    return checkStructuralIntegrity();
  }

  // ─── DIRECTORY OPERATIONS ────────────────────────────────

  /**
   * Reads the bucket page number from the directory at the given index.
   */
  int readDirectoryEntry(final int index) throws IOException {
    return readDirectoryEntry(readDirectoryStartPage(), index);
  }

  /**
   * Reads the bucket page number from the directory at the given index, with the directory start page already
   * resolved by the caller (used by the loops that walk the whole directory, to read the metadata page once).
   */
  int readDirectoryEntry(final int directoryStartPage, final int index) throws IOException {
    final int entriesPerPage = directoryEntriesPerPage();
    final int dirPageOffset = index / entriesPerPage;
    final int entryOffset = (index % entriesPerPage) * Binary.INT_SERIALIZED_SIZE;

    return readPage(directoryStartPage + dirPageOffset).readInt(entryOffset);
  }

  private int directoryEntriesPerPage() {
    return (pageSize - BasePage.PAGE_HEADER_SIZE) / Binary.INT_SERIALIZED_SIZE;
  }

  /**
   * Writes a bucket page number to the directory at the given index.
   */
  private void writeDirectoryEntry(final int directoryStartPage, final int index, final int bucketPageNum)
      throws IOException {
    final int entriesPerPage = directoryEntriesPerPage();
    final int dirPageOffset = index / entriesPerPage;
    final int entryOffset = (index % entriesPerPage) * Binary.INT_SERIALIZED_SIZE;

    final MutablePage dirPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, directoryStartPage + dirPageOffset), pageSize, false);
    dirPage.writeInt(entryOffset, bucketPageNum);
  }

  private void updateDirectoryAfterSplit(final int oldBucketPage, final int newBucketPage,
      final int oldLocalDepth, final int newLocalDepth) throws IOException {
    final int globalDepth = readGlobalDepth();
    final int directorySize = 1 << globalDepth;
    // The split bit is the newLocalDepth-th bit from the MSB of the hash.
    // In the directory index (top globalDepth bits), this maps to bit (globalDepth - newLocalDepth) from LSB.
    final int splitBit = 1 << (globalDepth - newLocalDepth);
    final int directoryStartPage = readDirectoryStartPage();

    for (int i = 0; i < directorySize; i++) {
      final int bucketPageForEntry = readDirectoryEntry(directoryStartPage, i);
      if (bucketPageForEntry == oldBucketPage) {
        if ((i & splitBit) != 0)
          writeDirectoryEntry(directoryStartPage, i, newBucketPage);
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
        // Index keys must be deterministic: encryption with random IV would yield a different ciphertext
        // for the same plaintext on every call, breaking hash lookup and key comparison.
        serializer.serializeValue(database, buffer, binaryKeyTypes[i], keys[i], false);
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
      k1 ^= (long) data[tail] & 0xff;
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
    int currentPageNum = bucketPageNum;

    // Defensive, allocation-free cycle detection on the overflow chain (see insertIntoOverflow above).
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;

    while (true) {
      if (++chainSteps > maxChainPages || !isValidBucketPage(currentPageNum))
        throw corruptedOverflowChain(currentPageNum);

      final MutablePage page = database.getTransaction()
          .getPageToModify(new PageId(database, fileId, currentPageNum), pageSize, false);
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
        return;
      }

      if (entryCount == 0)
        throw entryTooLarge(totalNeeded, freeSpace(page, 0));

      // No space in this page - follow or create overflow chain (preserves raw entry format)
      int overflowPageNum = page.readInt(BUCKET_OVERFLOW_PAGE);
      if (overflowPageNum == NO_OVERFLOW_PAGE) {
        final int localDepth = page.readShort(BUCKET_LOCAL_DEPTH) & 0xFFFF;
        overflowPageNum = allocateOverflowPage(localDepth);
        page.writeInt(BUCKET_OVERFLOW_PAGE, overflowPageNum);
      }
      currentPageNum = overflowPageNum;
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
   * Compacts a bucket page by rebuilding the data area without dead space (holes).
   * Reads all live entries (referenced by slots), rewrites them contiguously starting
   * at BUCKET_CONTENT_START, and updates all slot pointers.
   *
   * @return the entry count after compaction (unchanged, but returned for convenience)
   */
  private int compactPage(final MutablePage page, final int entryCount) {
    if (entryCount == 0)
      return 0;

    // Collect all live entries with their slot positions
    final byte[][] entries = new byte[entryCount][];
    for (int i = 0; i < entryCount; i++) {
      final int offset = readSlot(page, i);
      final int size = getEntrySize(page, offset);
      entries[i] = new byte[size];
      page.readByteArray(offset, entries[i]);
    }

    // Rewrite entries contiguously starting at BUCKET_CONTENT_START
    int dataEnd = BUCKET_CONTENT_START;
    for (int i = 0; i < entryCount; i++) {
      page.writeByteArray(dataEnd, entries[i]);
      writeSlot(page, i, dataEnd);
      dataEnd += entries[i].length;
    }

    page.writeShort(BUCKET_DATA_END, (short) dataEnd);
    return entryCount;
  }

  /**
   * For non-unique index: adds a RID to an existing entry for the same key.
   */
  private void addRIDToExistingEntry(final int bucketPageNum, final MutablePage page, int entryCount,
      final int pos, final byte[] serializedKey, final byte[] serializedRID) throws IOException {
    int entryStart = readSlot(page, pos);
    final int oldEntrySize = getEntrySize(page, entryStart);

    // Skip the key
    final int keyLen = computeKeyLengthFromPage(page, entryStart);
    final int ridCountOffset = entryStart + keyLen;

    // Read current RID count
    final int currentRidCount = readVarIntFromPage(page, ridCountOffset);
    final int oldRidCountSize = varIntSize(currentRidCount);
    final int newRidCount = currentRidCount + 1;
    final int newRidCountSize = varIntSize(newRidCount);

    final int ridCountDiff = newRidCountSize - oldRidCountSize;

    // Read old entry data and build new entry with extra RID
    final byte[] oldEntryBytes = new byte[oldEntrySize];
    page.readByteArray(entryStart, oldEntryBytes);

    final int oldRidsStart = keyLen + oldRidCountSize;
    final int oldRidsLen = oldEntrySize - oldRidsStart;
    final byte[] newRidCountBytes = encodeVarInt(newRidCount);

    final int newEntrySize = keyLen + newRidCountBytes.length + oldRidsLen + serializedRID.length;
    final byte[] newEntry = new byte[newEntrySize];
    System.arraycopy(oldEntryBytes, 0, newEntry, 0, keyLen);
    System.arraycopy(newRidCountBytes, 0, newEntry, keyLen, newRidCountBytes.length);
    System.arraycopy(oldEntryBytes, oldRidsStart, newEntry, keyLen + newRidCountBytes.length, oldRidsLen);
    System.arraycopy(serializedRID, 0, newEntry, keyLen + newRidCountBytes.length + oldRidsLen, serializedRID.length);

    // Check if there's space for the new entry (the ENTIRE new entry is appended at dataEnd,
    // the old entry becomes dead space - so we need newEntrySize bytes of free space)
    int availableSpace = freeSpace(page, entryCount);
    if (newEntrySize > availableSpace) {
      // Try compaction to reclaim dead space from previous updates
      entryCount = compactPage(page, entryCount);
      availableSpace = freeSpace(page, entryCount);

      if (newEntrySize > availableSpace) {
        // Still not enough space even after compaction. Instead of trying to move the
        // oversized entry, keep the existing entry in place and insert a separate entry
        // for just the new RID (key + ridCount=1 + RID). The search handles multiple
        // entries for the same key correctly by scanning the entire overflow chain.
        insertIntoOverflow(page, bucketPageNum, serializedKey, serializedRID);
        return;
      }

      // After compaction, the slot positions may have changed - find the entry again
      final int newPos = findExactEntry(page, entryCount, serializedKey);
      if (newPos >= 0)
        entryStart = readSlot(page, newPos);
    }

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
  private int removeRIDFromEntry(final MutablePage page, int entryCount, final int pos,
      final RID targetRID) {
    final int entryStart = readSlot(page, pos);
    final int keyLen = computeKeyLengthFromPage(page, entryStart);
    final int offset = entryStart + keyLen;

    final int ridCount = readVarIntFromPage(page, offset);

    if (ridCount <= 1) {
      // Verify the single RID matches the target before removing the entire entry
      final int ridOffset = offset + varIntSize(ridCount);
      final RID rid = readCompressedRID(page, ridOffset);
      if (!rid.equals(targetRID))
        return 0;
      removeEntryFromPage(page, entryCount, pos);
      return 1;
    }

    // Read old entry, rebuild without the target RID
    final int oldEntrySize = getEntrySize(page, entryStart);
    final byte[] oldEntry = new byte[oldEntrySize];
    page.readByteArray(entryStart, oldEntry);

    final int ridCountSize = varIntSize(ridCount);
    int ridOffset = keyLen + ridCountSize;
    for (int r = 0; r < ridCount; r++) {
      final RID rid = readCompressedRID(page, entryStart + ridOffset);
      final int ridSize = compressedRIDSize(rid);
      if (rid.equals(targetRID)) {
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

        // Check if there's space to write at dataEnd
        int availableSpace = freeSpace(page, entryCount);
        if (newEntrySize > availableSpace) {
          entryCount = compactPage(page, entryCount);
        }

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
        offset += getSerializedValueSize(page, offset, i);
    }
    return offset - startOffset;
  }

  private int computeKeyLengthFromBytes(final byte[] data, final int startOffset) {
    int offset = startOffset;
    for (int i = 0; i < binaryKeyTypes.length; i++) {
      final byte nullMarker = data[offset];
      offset += 1;
      if (nullMarker != 0)
        offset += getSerializedValueSizeFromBytes(data, offset, i);
    }
    return offset - startOffset;
  }

  private int computeKeyLengthFromEntry(final byte[] data, final int startOffset) {
    return computeKeyLengthFromBytes(data, startOffset);
  }

  /**
   * Computes how many bytes the value of the given key column occupies in the page. The column is passed rather than
   * its type so the storage encoding is read from {@link #binaryKeyTypes} while a failure can still report the
   * schema type from {@link #declaredKeyTypes}.
   */
  private int getSerializedValueSize(final BasePage page, final int offset, final int column) {
    final byte type = binaryKeyTypes[column];
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
      throw unsupportedKeyType(declaredKeyTypes[column], column, offset);
    }
  }

  private int getSerializedValueSizeFromBytes(final byte[] data, final int offset, final int column) {
    final byte type = binaryKeyTypes[column];
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
      throw unsupportedKeyType(declaredKeyTypes[column], column, offset);
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

  private void collectEntriesFromPage(int currentPageNum, final List<byte[]> entries) throws IOException {
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;
    while (currentPageNum != NO_OVERFLOW_PAGE) {
      if (++chainSteps > maxChainPages || !isValidBucketPage(currentPageNum))
        throw corruptedOverflowChain(currentPageNum);
      final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, currentPageNum), pageSize);
      final int entryCount = page.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;

      for (int i = 0; i < entryCount; i++) {
        final int offset = readSlot(page, i);
        final int entrySize = getEntrySize(page, offset);
        final byte[] entry = new byte[entrySize];
        page.readByteArray(offset, entry);
        entries.add(entry);
      }

      currentPageNum = page.readInt(BUCKET_OVERFLOW_PAGE);
    }
  }

  // ─── RID READING ─────────────────────────────────────────

  private RID readCompressedRID(final BasePage page, final int offset) {
    // Compressed RID: bucketId (varInt) + position (varInt)
    final Binary view = page.getImmutableView(offset, 20); // max ~20 bytes for 2 varInts
    final long bucketId = view.getNumber();
    final long position = view.getNumber();
    return new RID((int) bucketId, position);
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
    final MutablePage metaPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    metaPage.writeInt(META_TOTAL_ENTRIES, metaPage.readInt(META_TOTAL_ENTRIES) + delta);
  }

  private void writeGlobalDepth(final int depth) throws IOException {
    final MutablePage metaPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    metaPage.writeInt(META_GLOBAL_DEPTH, depth);
  }

  private void writeBucketCount(final int count) throws IOException {
    final MutablePage metaPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    metaPage.writeInt(metaTailOffset + 2 * Binary.INT_SERIALIZED_SIZE, count);
  }

  private void writeDirectoryStartPage(final int startPage) throws IOException {
    final MutablePage metaPage = database.getTransaction()
        .getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    metaPage.writeInt(metaTailOffset, startPage);
  }
}
