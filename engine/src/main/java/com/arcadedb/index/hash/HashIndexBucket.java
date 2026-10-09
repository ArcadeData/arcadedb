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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

/**
 * Disk-backed extendible hash index using {@link PaginatedComponent} for page management.
 * <p>
 * File layout:
 * <pre>
 *   Page 0:   Metadata page (global depth, key types, bucket/directory start pages, etc.)
 *   Page 1+:  Directory pages (array of int bucket page numbers)
 *   Page D+:  Bucket pages (entries of compressed key + compressed RID) and RID list pages (version 3, non-unique only)
 * </pre>
 * <p>
 * Three layouts exist, told apart by the file version:
 * <ul>
 *   <li>{@link #CURRENT_VERSION} (3): the bucket pages of version 2, plus RID lists (issue #9228). The RIDs of a non-unique
 *   key are kept inline in its entry while they take up to a quarter of a page. Past that they move to pages of their own,
 *   chained from the entry, which keeps only the first and the last page of the chain. Adding a RID to such a key writes
 *   the last page and nothing else, where an inline entry is copied whole on every insert and, once wider than a page,
 *   spreads over the overflow chain of the bucket, where every other key of the bucket has to walk past it.</li>
 *   <li>{@link #INLINE_RIDS_VERSION} (2): entries are NOT ordered (issue #5712). A new entry is appended at the end of the
 *   data area and its slot at the end of the slot directory, so an insert dirties the entry, one slot and the page header
 *   instead of the whole run of slots after a sorted insertion point. Each slot carries a 1-byte tag (the low byte of the
 *   key hash): a lookup scans the slots comparing the tag, and compares full keys only on a tag hit. Every RID of a key
 *   stays inline.</li>
 *   <li>{@link #LEGACY_SORTED_VERSION} (1): the layout of the indexes created before #5712. Entries are kept sorted by
 *   serialized key and binary-searched.</li>
 * </ul>
 * An index of an older version keeps working as it is and moves to the current layout on REBUILD INDEX (or DROP and
 * recreate), which always creates the current version. Overflow pages are chained when a bucket is full and cannot split
 * (same hash prefix collision).
 */
public class HashIndexBucket extends PaginatedComponent {
  public static final String UNIQUE_INDEX_EXT    = "uhashidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nhashidx";

  // Page size used when the caller does not ask for one. 4 KB was the fastest of the sizes measured for inserts and for
  // lookups, for long, string and composite keys, sequential and random, unique and not (issue #5712): a lookup scans the
  // tags of the page, so a bigger page costs more per probe, and an insert of a bigger page dirties more of it. It is
  // a tuning choice, independent of MAX_PAGE_SIZE, which is a hard limit of the on-page addressing.
  public static final int DEF_PAGE_SIZE     = 4_096;
  // Default for a key with a variable-width column (STRING, BINARY, DECIMAL): the width of an entry is not known at creation,
  // and a key wider than a page is refused at insert (see entryTooLarge). It measured the same as DEF_PAGE_SIZE for short
  // strings and leaves four times the room for long ones.
  public static final int DEF_VARIABLE_KEY_PAGE_SIZE = 16_384;
  // The default of the indexes created before #5712. A rebuild of a sorted-layout index of this size moves it to DEF_PAGE_SIZE.
  static final int LEGACY_DEF_PAGE_SIZE     = 65_536;
  public static final int CURRENT_VERSION   = 3;
  // Version of the files created between #5712 and #9228: unordered tagged bucket pages, every RID of a key inline
  public static final int INLINE_RIDS_VERSION   = 2;
  // Version of the files created before #5712: sorted bucket pages, no slot tags
  public static final int LEGACY_SORTED_VERSION = 1;
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
   * META_KEY_TYPES_START + numberOfKeys + 2 + 4 * INT_SERIALIZED_SIZE} bytes - 99 at the {@link #MAX_SANE_KEY_COUNT}
   * ceiling - so a page size that cannot even hold page 0 is refused up front instead of writing metadata past the end
   * of it. A RID list page needs {@link #RID_PAGE_CONTENT_START} bytes of header plus one RID, far below it too.
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
  // After key types: nullStrategy(1), unique(1), dirStartPage(4), bucketsStartPage(4), bucketCount(4) and, from version 3,
  // the first page of the free RID list pages (4, NO_OVERFLOW_PAGE when there is none)

  // Upper bound on the number of key components used to sanity-check the (possibly corrupt) count read
  // from the metadata page before it is trusted to size arrays and walk the page. Composite indexes have
  // very few columns in practice; this is a generous ceiling that still catches a garbage byte.
  static final int MAX_SANE_KEY_COUNT = 64;

  // Number of times a lookup re-reads the metadata + directory before declaring the index corrupted (#4743).
  static final int MAX_LOOKUP_RETRIES = 3;

  // Number of times a lookup starts again when a RID list reaches a page of another key (#9228). Each retry follows a commit
  // that freed and reused the page under the lookup, so a hot key under heavy churn can need more than a torn directory read
  // does; a damaged list fails every attempt the same way, and CHECK DATABASE reports it without racing anything.
  static final int MAX_RID_LIST_LOOKUP_RETRIES = 64;

  // Upper bound on the problems reported by a single structural check, so a badly damaged index does not build a
  // huge list (the first problems are enough to know it must be rebuilt).
  static final int MAX_REPORTED_PROBLEMS = 20;

  // The schema types usable as a hash index key, listed in the creation-time refusal. Fixed for the life of the JVM.
  private static final String SUPPORTED_KEY_TYPE_NAMES = supportedKeyTypeNames();

  // Bucket page header offsets (relative to PAGE_HEADER_SIZE)
  static final int BUCKET_LOCAL_DEPTH     = 0;                       // short (2): depth in the low 15 bits, see NO_DEAD_SPACE_FLAG
  static final int BUCKET_ENTRY_COUNT     = 2;                       // short (2)
  static final int BUCKET_OVERFLOW_PAGE   = 4;                       // int (4)
  static final int BUCKET_DATA_END       = 8;                        // short (2): offset past last entry data
  static final int BUCKET_CONTENT_START   = 10;                      // entries start here

  // High bit of the local depth short (#9253): set when the page is known to hold no dead space, so the walk of an overflow
  // chain moves past a full page with one free-space check instead of decoding every entry on it. Clear means unknown: pages
  // written by older versions and freshly rebuilt ones start that way and are checked once. Whatever leaves a hole in the data
  // area (removing an entry or a RID, relocating a grown entry) clears it, so a new writer that does the same must too (a miss
  // costs space, never correctness: the page is just not compacted on this path); a compaction or a clean check sets it.
  // Files written with the flag set are not readable by a version that predates it (it reads the short as the depth), so
  // downgrading after this change is not supported.
  static final int NO_DEAD_SPACE_FLAG = 0x8000;
  static final int LOCAL_DEPTH_MASK   = 0x7FFF;

  // Slot directory: entry offsets stored at the END of the page, growing downward.
  // slot[i] is at pageOffset = (pageSize - PAGE_HEADER_SIZE) - (i + 1) * slotSize
  // Each slot stores the byte offset (relative to PAGE_HEADER_SIZE) of the entry's data (2 bytes). The current layout
  // adds a 1-byte hash tag right after the offset, the legacy sorted layout has none.
  static final int SLOT_SIZE        = 2;
  static final int TAGGED_SLOT_SIZE = 3;

  // RID list pages (version 3, issue #9228). The header shares two offsets with a bucket page: the first short tells the
  // two kinds apart (a bucket page keeps its local depth there, at most 30 plus NO_DEAD_SPACE_FLAG, never the marker), and
  // the next page sits where a bucket page keeps its overflow page. The owner is the hash of the key the list belongs to:
  // a freed page is reused by another key, so a lookup, which runs outside the commit lock, can follow a pointer read just
  // before the page changed hands, and the owner is how it finds out and starts again instead of returning the RIDs of
  // another key. Its limits: a page given back to the SAME key is not told apart (the RIDs read are that key's, from a
  // newer state, as a read across the pages of an overflow chain can mix two states), and neither is a page given to a
  // different key with the same 64-bit hash, which needs that collision and the race at once. Writers run under the commit
  // lock and never meet a torn list, so for them a wrong owner is corruption.
  static final int RID_LIST_PAGE_MARKER   = 0x4000;
  // A page on the free list. Every freed page is marked, not just the last one: a lookup that read the entry before the
  // commit that freed the list must stop at the first freed page it reaches and start again, instead of reading on into
  // the free list, whose pages can hold RIDs the key no longer has
  static final int RID_FREE_PAGE_MARKER   = 0x4001;
  static final int RID_PAGE_MARKER        = 0;                       // short (2)
  // The two 16-bit fields hold at most 65535: MAX_PAGE_SIZE (64 KB, enforced at creation) bounds DATA_END below that, and
  // COUNT to about 32K, since a compressed RID takes at least 2 bytes
  static final int RID_PAGE_COUNT         = 2;                       // short (2): RIDs on this page
  static final int RID_PAGE_NEXT          = 4;                       // int (4): next page of the list, or of the free list
  static final int RID_PAGE_DATA_END      = 8;                       // short (2): offset past the last RID
  static final int RID_PAGE_OWNER         = 10;                      // long (8): hash of the key
  static final int RID_PAGE_CONTENT_START = 18;                      // compressed RIDs, packed

  // Value of an entry whose RIDs live in a RID list: a header of 0, which an inline entry never has (its last RID going
  // away removes it), then the first and the last page of the list
  static final int RID_LIST_VALUE_SIZE = 1 + 2 * Binary.INT_SERIALIZED_SIZE;

  final HashIndex mainIndex;
  final BinarySerializer serializer;
  final BinaryComparator comparator;
  final boolean unique;
  // True for the current layout (unordered entries + slot tags), false for the legacy sorted one (#5712)
  final boolean tagged;
  final int     slotSize;
  // True when the RIDs of a key may move to a RID list (version 3, non-unique)
  final boolean ridLists;
  // Largest value (header + RIDs) an entry keeps inline when RID lists are on: a quarter of the data area of a page
  final int     inlineValueLimit;

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

  // First key column whose type the metadata page declares but a hash index cannot encode, or -1. Set at load only. With
  // unordered buckets a lookup parses just the entries whose tag matches the key, and a key encoded with a damaged type
  // matches nothing, so without this the damage would read as "no such key" instead of the actionable error (#352).
  private int unsupportedKeyColumn = -1;

  /**
   * Called at creation time.
   */
  HashIndexBucket(final HashIndex mainIndex, final DatabaseInternal database, final String name, final boolean unique,
      final String filePath, final ComponentFile.MODE mode, final Type[] keyTypes, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) throws IOException {
    this(mainIndex, database, name, unique, filePath, mode, keyTypes, pageSize, nullStrategy, CURRENT_VERSION);
  }

  /**
   * Called at creation time with an explicit layout version. Production code always creates the current one; the
   * legacy sorted layout can only be asked for to exercise the read path of the indexes created before #5712.
   */
  HashIndexBucket(final HashIndex mainIndex, final DatabaseInternal database, final String name, final boolean unique,
      final String filePath, final ComponentFile.MODE mode, final Type[] keyTypes, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final int layoutVersion) throws IOException {
    // The page size is validated inside the super() argument list on purpose: this constructor is the ONLY path that
    // creates the file, and super() creates it, so checking here - before super() runs - is what guarantees no hash
    // index file can exist with a page size the bucket cannot address (#5713).
    super(database, name, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode,
        checkSupportedPageSize(name, pageSize), layoutVersion);

    this.mainIndex = mainIndex;
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.tagged = isTaggedLayout(name, layoutVersion);
    this.slotSize = tagged ? TAGGED_SLOT_SIZE : SLOT_SIZE;
    this.ridLists = !unique && layoutVersion >= CURRENT_VERSION;
    this.inlineValueLimit = inlineValueLimit(pageSize);
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
    this.tagged = isTaggedLayout(name, version);
    this.slotSize = tagged ? TAGGED_SLOT_SIZE : SLOT_SIZE;
    this.ridLists = !unique && version >= CURRENT_VERSION;
    this.inlineValueLimit = inlineValueLimit(pageSize);

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
    // it in place, and a RID list page can be freed and given to another key while it is read, hence the bounded
    // retry before declaring the index corrupted (#4743, #9228).
    for (int attempt = 0; ; attempt++) {
      final BasePage metaPage = metaPage();
      final int dirIndex = directoryIndex(hash, metaPage.readInt(META_GLOBAL_DEPTH));
      final int bucketPageNum = readDirectoryEntry(metaPage.readInt(metaTailOffset), dirIndex);

      if (isValidBucketPage(bucketPageNum)) {
        final List<RID> result = searchBucket(bucketPageNum, serializedKey, hash, limit);
        if (result != null)
          return result;

        // Under REPEATABLE_READ the transaction keeps the pages it read: the retry must read them again, not the same stale
        // copies (its own changes stay, unpinFiles only drops the immutable pages)
        final TransactionContext tx = database.getTransactionIfExists();
        if (tx != null)
          tx.unpinFiles(List.of(fileId));

        if (attempt >= MAX_RID_LIST_LOOKUP_RETRIES)
          throw new IndexException("The RID list of a key of hash index '" + getName() + "' (fileId=" + fileId
              + ") kept reaching a page that does not belong to it after " + attempt + " attempts. Run CHECK DATABASE: if it "
              + "reports the index as corrupted, rebuild it (REBUILD INDEX, or DROP and recreate it); if not, the key was changing "
              + "too fast for the lookup and the lookup can be retried.");
        // let the commit that is reusing the pages finish before reading the list again: yield first, then back off
        if (attempt < 8)
          Thread.yield();
        else
          LockSupport.parkNanos(Math.min(attempt, 32) * 50_000L);

      } else if (attempt >= MAX_LOOKUP_RETRIES)
        throw new IndexException(
            "Invalid entry " + bucketPageNum + " at position " + dirIndex + " in the directory of hash index '" + getName()
                + "' (fileId=" + fileId + ", totalPages=" + getTotalPages()
                + "). The index is corrupted, please rebuild it (DROP and recreate it).");
    }
  }

  /**
   * Searches a bucket page (and its overflow chain) for entries matching the given keys. Returns null when a RID list
   * reaches a page that does not belong to the key, which the caller retries (see {@link #readRidList}).
   */
  private List<RID> searchBucket(final int bucketPageNum, final byte[] searchKey, final long hash,
      final int limit) throws IOException {
    final int tag = tagOf(hash);
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

      if (entryCount > 0 && !searchInPage(page, entryCount, searchKey, tag, hash, result, limit))
        return null;

      if (limit > 0 && result.size() >= limit)
        break;

      currentPage = overflowPage;
    }
    return result;
  }

  /**
   * Looks within a single bucket page for the entries matching the given key. Returns false when a RID list reaches a page
   * that does not belong to the key.
   */
  private boolean searchInPage(final BasePage page, final int entryCount, final byte[] searchKey, final int tag, final long hash,
      final List<RID> result, final int limit) throws IOException {
    int pos = findNextEntry(page, entryCount, searchKey, tag, 0);

    while (pos >= 0) {
      int offset = readSlot(page, pos);
      final int keyLen = computeKeyLengthFromPage(page, offset);

      offset += keyLen;

      if (unique) {
        result.add(readCompressedRID(page, offset));
        // a unique key holds one entry and callers pass limit 1 for it (HashIndex.getDiskResult), but an all-null key is exempt
        // from uniqueness (issue #9237): the caller asks for all of its entries
        if (limit > 0 && result.size() >= limit)
          return true;
      } else {
        final int header = readVarIntFromPage(page, offset);
        if (header == 0 && ridLists) {
          if (!readRidList(page.readInt(offset + 1), hash, result, limit))
            return false;
        } else {
          offset += varIntSize(header);
          // the header is the bytes the RIDs take from version 3, and their number up to version 2
          if (ridLists) {
            final int end = offset + header;
            while (offset < end) {
              result.add(readCompressedRID(page, offset));
              if (limit > 0 && result.size() >= limit)
                return true;
              offset += compressedRIDSizeFromPage(page, offset);
            }
          } else
            for (int r = 0; r < header; r++) {
              result.add(readCompressedRID(page, offset));
              if (limit > 0 && result.size() >= limit)
                return true;
              offset += compressedRIDSizeFromPage(page, offset);
            }
        }
        if (limit > 0 && result.size() >= limit)
          return true;
      }
      pos = findNextEntry(page, entryCount, searchKey, tag, pos + 1);
    }
    return true;
  }

  /**
   * Adds the RIDs of a RID list to the result. Returns false, having added nothing, when the list reaches a page that is not
   * a RID list page of this key: a lookup holds no lock, so a page it was pointed to can have been freed and given to another
   * key in between, and the caller starts again from the directory. A page of the same key read in a newer version is not
   * told apart, exactly as for the entries of an overflow chain, which are no more atomic across pages.
   */
  private boolean readRidList(final int headPage, final long hash, final List<RID> result, final int limit) throws IOException {
    final int sizeBefore = result.size();
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;
    for (int current = headPage; current != NO_OVERFLOW_PAGE; ) {
      if (++chainSteps > maxChainPages || !isValidBucketPage(current)) {
        result.subList(sizeBefore, result.size()).clear();
        return false;
      }
      final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, current), pageSize);
      if (!isRidListPageOf(page, hash)) {
        result.subList(sizeBefore, result.size()).clear();
        return false;
      }
      final int dataEnd = page.readShort(RID_PAGE_DATA_END) & 0xFFFF;
      for (int offset = RID_PAGE_CONTENT_START; offset < dataEnd; ) {
        result.add(readCompressedRID(page, offset));
        if (limit > 0 && result.size() >= limit)
          return true;
        offset += compressedRIDSizeFromPage(page, offset);
      }
      current = page.readInt(RID_PAGE_NEXT);
    }
    return true;
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

    final TransactionContext tx = database.getTransaction();
    final byte[] serializedRID = serializeCompressedRID(rid);

    // Page and slot of the entry of the key when it is there but cannot grow on its page
    int blockedPageNum = NO_OVERFLOW_PAGE;
    int blockedPos = -1;

    // For non-unique indexes, check if key already exists (on primary page or overflow chain) and append RID. The walk only
    // reads: the page holding the key is the one taken for modification, not every page walked past (#9228)
    if (!unique) {
      final int tag = tagOf(hash);
      int currentPageNum = bucketPageNum;
      final int maxChainPages = getTotalPages();
      int chainSteps = 0;
      while (currentPageNum != NO_OVERFLOW_PAGE) {
        if (++chainSteps > maxChainPages || !isValidBucketPage(currentPageNum))
          throw corruptedOverflowChain(currentPageNum);
        final BasePage currentPage = tx.getPage(new PageId(database, fileId, currentPageNum), pageSize);
        final int currentEntryCount = currentPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
        final int existingPos = findNextEntry(currentPage, currentEntryCount, serializedKey, tag, 0);
        if (existingPos >= 0) {
          if (addRIDToExistingEntry(currentPageNum, currentPage, currentEntryCount, existingPos, serializedRID, hash)) {
            updateTotalEntries(1);
            return;
          }
          blockedPageNum = currentPageNum;
          blockedPos = existingPos;
          break;
        }
        currentPageNum = currentPage.readInt(BUCKET_OVERFLOW_PAGE);
      }
    }

    // read only: the bucket page is taken for modification only by the paths that write it
    final PageId bucketPageId = new PageId(database, fileId, bucketPageNum);
    final BasePage bucketPage = tx.getPage(bucketPageId, pageSize);
    final int entryCount = bucketPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
    final int localDepth = bucketPage.readShort(BUCKET_LOCAL_DEPTH) & LOCAL_DEPTH_MASK;

    if (blockedPageNum == NO_OVERFLOW_PAGE) {
      // Calculate entry size (data + slot)
      final int entryDataSize = unique ?
          serializedKey.length + serializedRID.length :
          serializedKey.length + varIntSize(singleRidHeader(serializedRID)) + serializedRID.length;
      final int totalNeeded = entryDataSize + slotSize;

      // Try to insert into the bucket
      if (totalNeeded <= freeSpace(bucketPage, entryCount)) {
        insertEntryInSlottedPage(tx.getPageToModify(bucketPageId, pageSize, false), entryCount, serializedKey, serializedRID, hash);
        updateTotalEntries(1);
        return;
      }
    } else if (!ridLists) {
      // Layouts without RID lists: the RID goes to a separate entry of the same key further down the chain, as it always did
      final MutablePage blockedPage = tx.getPageToModify(new PageId(database, fileId, blockedPageNum), pageSize, false);
      insertIntoOverflow(blockedPage, blockedPageNum, serializedKey, serializedRID, hash);
      updateTotalEntries(1);
      return;
    }

    // Bucket is full (or, with RID lists, the entry of the key cannot grow on its page): split if it would help
    if ((localDepth < globalDepth || localDepth < 30) && canSplitHelp(bucketPageNum, entryCount, localDepth)) {
      splitBucket(bucketPageNum, localDepth, dirIndex, hash);
      putInternal(serializedKey, rid, hash);
      return;
    }

    if (blockedPageNum == NO_OVERFLOW_PAGE)
      insertIntoOverflow(tx.getPageToModify(bucketPageId, pageSize, false), bucketPageNum, serializedKey, serializedRID, hash);
    else {
      // A RID list takes the RIDs out of the page, instead of a second entry of the key that every later insert of it would
      // find blocked again
      final MutablePage blockedPage = tx.getPageToModify(new PageId(database, fileId, blockedPageNum), pageSize, false);
      moveRIDsToRidList(blockedPageNum, blockedPage, blockedPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF, blockedPos,
          serializedRID, hash);
    }
    updateTotalEntries(1);
  }

  // ─── REMOVE ──────────────────────────────────────────────

  /**
   * Removes all entries for the given key.
   */
  void remove(final Object[] keys) throws IOException {
    // removes every entry of the key, an all-null one included: the commit path removes one record's entry by RID, below
    final byte[] serializedKey = serializeKeys(keys);
    final long hash = murmurHash64(serializedKey);
    final BasePage metaPage = metaPage();
    final int dirIndex = directoryIndex(hash, metaPage.readInt(META_GLOBAL_DEPTH));
    final int bucketPageNum = readDirectoryEntry(metaPage.readInt(metaTailOffset), dirIndex);

    removeFromBucket(bucketPageNum, serializedKey, hash, null, LSMTreeIndexAbstract.isKeyNull(keys));
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

    removeFromBucket(bucketPageNum, serializedKey, hash, rid, LSMTreeIndexAbstract.isKeyNull(keys));
  }

  private void removeFromBucket(final int bucketPageNum, final byte[] serializedKey, final long hash, final RID specificRID,
      final boolean nullKey) throws IOException {
    final int tag = tagOf(hash);
    int currentPageNum = bucketPageNum;
    int totalRemoved = 0;
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;

    while (currentPageNum != NO_OVERFLOW_PAGE) {
      if (++chainSteps > maxChainPages || !isValidBucketPage(currentPageNum))
        throw corruptedOverflowChain(currentPageNum);
      final PageId pageId = new PageId(database, fileId, currentPageNum);
      // read first: only a page holding the key is taken for modification
      final BasePage readPage = database.getTransaction().getPage(pageId, pageSize);
      int entryCount = readPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
      final int overflowPage = readPage.readInt(BUCKET_OVERFLOW_PAGE);

      final int pos = findNextEntry(readPage, entryCount, serializedKey, tag, 0);
      if (pos >= 0) {
        final MutablePage page = database.getTransaction().getPageToModify(pageId, pageSize, false);
        if (specificRID != null && unique && nullKey) {
          // the entries of an all-null key belong to different records: only the one of this RID goes away
          for (int p = pos; p >= 0; p = findNextEntry(page, entryCount, serializedKey, tag, p + 1)) {
            final int offset = readSlot(page, p);
            if (specificRID.equals(readCompressedRID(page, offset + computeKeyLengthFromPage(page, offset)))) {
              updateTotalEntries(-removeEntryFromPage(page, entryCount, p));
              return;
            }
          }
          // RID not in any entry on this page - continue to overflow pages
        } else if (specificRID != null && !unique) {
          // Search all matching entries on this page (entries for the same key may be split). No re-scan is needed after a
          // removal: the loop returns on the first one.
          for (int p = pos; p >= 0; p = findNextEntry(page, entryCount, serializedKey, tag, p + 1)) {
            final int removed = removeRIDFromEntry(page, entryCount, p, specificRID, hash);
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
          while (p >= 0) {
            totalRemoved += removeEntryFromPage(page, entryCount, p);
            entryCount--;
            // another entry has moved into position p (the next one in the legacy layout, the last one otherwise):
            // look again from p
            p = findNextEntry(page, entryCount, serializedKey, tag, p);
          }
          // the one entry of a unique key is gone; an all-null key can have more of them on the overflow pages
          if (unique && !nullKey) {
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
      insertRawEntry(targetBucketPage, entry, entryHash);
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
      final byte[] serializedKey, final byte[] serializedRID, final long hash) throws IOException {
    final int entryDataSize = unique ?
        serializedKey.length + serializedRID.length :
        serializedKey.length + varIntSize(singleRidHeader(serializedRID)) + serializedRID.length;
    final int totalNeeded = entryDataSize + slotSize;

    // Defensive cycle detection: a corrupted overflow chain that loops back to a previously-seen page would
    // otherwise spin forever. A valid chain visits distinct pages, so it cannot be longer than the file: the
    // bounded-step guard is equivalent to tracking the visited pages, without allocating on the insert path.
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;

    while (true) {
      int overflowPageNum = currentPage.readInt(BUCKET_OVERFLOW_PAGE);

      if (overflowPageNum == NO_OVERFLOW_PAGE) {
        final int localDepth = currentPage.readShort(BUCKET_LOCAL_DEPTH) & LOCAL_DEPTH_MASK;
        overflowPageNum = allocateOverflowPage(localDepth);
        currentPage.writeInt(BUCKET_OVERFLOW_PAGE, overflowPageNum);
      }

      if (++chainSteps > maxChainPages || !isValidBucketPage(overflowPageNum))
        throw corruptedOverflowChain(overflowPageNum);

      final MutablePage overflowPage = database.getTransaction()
          .getPageToModify(new PageId(database, fileId, overflowPageNum), pageSize, false);
      final int entryCount = overflowPage.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;

      if (totalNeeded <= freeSpace(overflowPage, entryCount)) {
        insertEntryInSlottedPage(overflowPage, entryCount, serializedKey, serializedRID, hash);
        return;
      }

      // The dead space of removed and grown entries is only reclaimed when an entry does not fit, as everywhere else
      if (!hasNoDeadSpace(overflowPage)) {
        if (hasDeadSpace(overflowPage, entryCount)) {
          compactPage(overflowPage, entryCount);
          if (totalNeeded <= freeSpace(overflowPage, entryCount)) {
            insertEntryInSlottedPage(overflowPage, entryCount, serializedKey, serializedRID, hash);
            return;
          }
        } else
          // Nothing to reclaim: remember it, so the next insert walking past this page does not read its entries again.
          // The page is already in the transaction's modified set (getPageToModify above), and the bit is written once per
          // page, so this adds no page to the commit and no extra conflict footprint
          setNoDeadSpace(overflowPage, true);
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
    pos += Binary.INT_SERIALIZED_SIZE;

    if (version >= CURRENT_VERSION)
      metaPage.writeInt(pos, NO_OVERFLOW_PAGE); // no free RID list page

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
    unsupportedKeyColumn = -1;

    boolean metadataCorrupt = numKeysCorrupt;

    int pos = META_KEY_TYPES_START;
    declaredKeyTypes = new byte[numKeys];
    binaryKeyTypes = new byte[numKeys];
    keyTypes = new Type[numKeys];
    for (int i = 0; i < numKeys; i++) {
      declaredKeyTypes[i] = metaPage.readByte(pos);
      binaryKeyTypes[i] = storageKeyType(declaredKeyTypes[i]);
      keyTypes[i] = Type.getByBinaryType(declaredKeyTypes[i]);
      if (!isSupportedKeyType(declaredKeyTypes[i])) {
        metadataCorrupt = true;
        if (unsupportedKeyColumn < 0)
          unsupportedKeyColumn = i;
      }
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
   * Whether a file version uses the tagged layout. Only the two layouts this class knows are accepted: reading a file of a
   * later version as the current one would misread its pages without any sign of it.
   */
  static boolean isTaggedLayout(final String indexName, final int version) {
    if (version == CURRENT_VERSION || version == INLINE_RIDS_VERSION)
      return true;
    if (version == LEGACY_SORTED_VERSION)
      return false;
    throw new IndexException("Hash index '" + indexName + "' has the page layout version " + version + ", which this server does not "
        + "support (it knows " + LEGACY_SORTED_VERSION + " to " + CURRENT_VERSION + "). It was created by a newer version: open "
        + "the database with that version, or drop and recreate the index.");
  }

  /**
   * Largest value (header + RIDs) an entry keeps inline when RID lists are on: a quarter of the data area of a bucket page.
   * Below it a key with few RIDs costs no page of its own, above it the copy an inline entry takes on every insert stops
   * growing. The RIDs of an entry at the limit always fit in one RID list page, whose data area is larger.
   */
  static int inlineValueLimit(final int pageSize) {
    return (pageSize - BasePage.PAGE_HEADER_SIZE - BUCKET_CONTENT_START) / 4;
  }

  /**
   * The page size a new index gets when the caller does not ask for one: {@link #DEF_PAGE_SIZE}, or
   * {@link #DEF_VARIABLE_KEY_PAGE_SIZE} when any key column has no fixed width.
   */
  static int defaultPageSize(final Type[] keyTypes) {
    for (final Type keyType : keyTypes)
      if (keyType == Type.STRING || keyType == Type.BINARY || keyType == Type.DECIMAL)
        return DEF_VARIABLE_KEY_PAGE_SIZE;
    return DEF_PAGE_SIZE;
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
    case BinaryTypes.TYPE_OFFSET_TIME:
    case BinaryTypes.TYPE_LOCAL_TIME:
    case BinaryTypes.TYPE_ZONED_DATETIME:
    case BinaryTypes.TYPE_DURATION:
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
    return BinaryTypes.getIndexStorageType(declaredBinaryType);
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

      if (ridLists && problems.size() < MAX_REPORTED_PROBLEMS)
        checkRidLists(chainOwner, problems);
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
   * Walks the RID list of every entry that has one, then the free list, after {@link #checkStructuralIntegrity} has marked the
   * pages of the bucket chains in {@code chainOwner}. A page must belong to one structure only, a list must be acyclic, end at
   * the page its entry names as the last one and hold, on every page, the RIDs its header counts, and its pages must be RID
   * list pages of the key.
   */
  private void checkRidLists(final int[] chainOwner, final List<String> problems) throws IOException {
    final int totalPages = chainOwner.length;
    final int ridListMark = -1;
    final int freeMark = -2;

    for (int bucketPageNum = 0; bucketPageNum < totalPages && problems.size() < MAX_REPORTED_PROBLEMS; bucketPageNum++) {
      if (chainOwner[bucketPageNum] <= 0)
        continue;
      final BasePage page = readPage(bucketPageNum);
      final int entryCount = page.readShort(BUCKET_ENTRY_COUNT) & 0xFFFF;
      for (int i = 0; i < entryCount && problems.size() < MAX_REPORTED_PROBLEMS; i++) {
        final int entryOffset = readSlot(page, i);
        final int keyLen = computeKeyLengthFromPage(page, entryOffset);
        final int valueOffset = entryOffset + keyLen;
        if (readVarIntFromPage(page, valueOffset) != 0)
          continue;

        final long hash = hashKeyAt(page, entryOffset, keyLen);
        final int tailPage = page.readInt(valueOffset + 1 + Binary.INT_SERIALIZED_SIZE);
        final String owner = "the RID list of entry " + i + " of page " + bucketPageNum;
        int last = NO_OVERFLOW_PAGE;
        for (int current = page.readInt(valueOffset + 1); current != NO_OVERFLOW_PAGE; ) {
          if (!isValidBucketPage(current)) {
            problems.add(owner + " reaches the invalid page " + current + " (totalPages=" + totalPages + ")");
            break;
          }
          if (chainOwner[current] != 0) {
            problems.add(owner + " reaches page " + current + ", which is " + (chainOwner[current] == ridListMark ?
                "already part of a RID list" : "part of the chain of bucket " + (chainOwner[current] - 1)));
            break;
          }
          chainOwner[current] = ridListMark;
          final BasePage listPage = readPage(current);
          if (!isRidListPageOf(listPage, hash)) {
            problems.add(owner + " reaches page " + current + ", which is not a RID list page of its key");
            break;
          }
          final String pageProblem = checkRidListPageContent(listPage);
          if (pageProblem != null) {
            problems.add(owner + ": page " + current + " " + pageProblem);
            break;
          }
          last = current;
          current = listPage.readInt(RID_PAGE_NEXT);
        }
        if (last != NO_OVERFLOW_PAGE && last != tailPage)
          problems.add(owner + " ends at page " + last + " but the entry names page " + tailPage + " as its last one");
      }
    }

    for (int current = readFreeRidListPage(); current != NO_OVERFLOW_PAGE && problems.size() < MAX_REPORTED_PROBLEMS; ) {
      if (!isValidBucketPage(current)) {
        problems.add("the free list of RID list pages reaches the invalid page " + current + " (totalPages=" + totalPages + ")");
        break;
      }
      if (chainOwner[current] != 0) {
        problems.add("the free list of RID list pages reaches page " + current + ", which is " + (chainOwner[current] == freeMark ?
            "already in the free list (cycle)" : "in use"));
        break;
      }
      chainOwner[current] = freeMark;
      final BasePage freePage = readPage(current);
      if (!isFreeRidListPage(freePage)) {
        problems.add("the free list of RID list pages reaches page " + current + ", which is not a free RID list page");
        break;
      }
      current = freePage.readInt(RID_PAGE_NEXT);
    }
  }

  /** Returns what is wrong with the RIDs of a RID list page, or null: they must fill the data area and match the count. */
  private String checkRidListPageContent(final BasePage page) {
    final int dataEnd = page.readShort(RID_PAGE_DATA_END) & 0xFFFF;
    final int count = page.readShort(RID_PAGE_COUNT) & 0xFFFF;
    if (count == 0)
      return "holds no RID";
    int offset = RID_PAGE_CONTENT_START;
    for (int r = 0; r < count; r++) {
      if (offset >= dataEnd)
        return "counts " + count + " RIDs but holds " + r;
      offset += compressedRIDSizeFromPage(page, offset);
    }
    return offset == dataEnd ? null : "has RID bytes that do not match its count of " + count;
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
    if (unsupportedKeyColumn >= 0)
      throw unsupportedKeyType(declaredKeyTypes[unsupportedKeyColumn], unsupportedKeyColumn, -1);

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
   * Inserts a raw entry (already serialized) into a bucket page, finding the correct position.
   */
  private void insertRawEntry(final int bucketPageNum, final byte[] rawEntry, final long hash) throws IOException {
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
      final int totalNeeded = rawEntry.length + slotSize;

      if (totalNeeded <= freeSpace(page, entryCount)) {
        appendEntry(page, entryCount, rawEntry, hash);
        return;
      }

      if (entryCount == 0)
        throw entryTooLarge(totalNeeded, freeSpace(page, 0));

      // No space in this page - follow or create overflow chain (preserves raw entry format)
      int overflowPageNum = page.readInt(BUCKET_OVERFLOW_PAGE);
      if (overflowPageNum == NO_OVERFLOW_PAGE) {
        final int localDepth = page.readShort(BUCKET_LOCAL_DEPTH) & LOCAL_DEPTH_MASK;
        overflowPageNum = allocateOverflowPage(localDepth);
        page.writeInt(BUCKET_OVERFLOW_PAGE, overflowPageNum);
      }
      currentPageNum = overflowPageNum;
    }
  }

  /**
   * Removes an entire entry at the given position. Returns the number of RIDs removed.
   */
  private int removeEntryFromPage(final MutablePage page, final int entryCount, final int pos) throws IOException {
    final int entryOffset = readSlot(page, pos);

    // Count RIDs being removed
    int removedCount;
    if (unique) {
      removedCount = 1;
    } else {
      final int valueOffset = entryOffset + computeKeyLengthFromPage(page, entryOffset);
      final int header = readVarIntFromPage(page, valueOffset);
      if (!ridLists)
        removedCount = header;
      else if (header == 0)
        removedCount = releaseRidList(page.readInt(valueOffset + 1), page.readInt(valueOffset + 1 + Binary.INT_SERIALIZED_SIZE),
            hashKeyAt(page, entryOffset, valueOffset - entryOffset));
      else
        removedCount = countRids(page, valueOffset + varIntSize(header), header);
    }

    dropSlot(page, entryCount, pos);
    return removedCount;
  }

  /**
   * Removes the slot of an entry, whose data becomes dead space.
   */
  private void dropSlot(final MutablePage page, final int entryCount, final int pos) {
    // For slotted pages, we don't need to shift entry data (it becomes a hole): only the slot goes away. The hole is
    // reclaimed on the next page rebuild (during split/compaction).
    if (tagged) {
      // Unordered layout: the last slot (offset and tag) takes the place of the removed one, nothing else moves
      final int last = entryCount - 1;
      if (pos != last) {
        writeSlot(page, pos, readSlot(page, last));
        writeTag(page, pos, readTag(page, last));
      }
    } else
      // Sorted layout: the order must be preserved, shift the following slots left
      for (int i = pos; i < entryCount - 1; i++)
        writeSlot(page, i, readSlot(page, i + 1));

    // Note: dataEnd stays the same (dead space). We'll recover it during splits.
    setNoDeadSpace(page, false);
    page.writeShort(BUCKET_ENTRY_COUNT, (short) (entryCount - 1));
  }

  private boolean hasNoDeadSpace(final BasePage page) {
    return (page.readShort(BUCKET_LOCAL_DEPTH) & NO_DEAD_SPACE_FLAG) != 0;
  }

  private void setNoDeadSpace(final MutablePage page, final boolean noDeadSpace) {
    final int depthAndFlag = page.readShort(BUCKET_LOCAL_DEPTH) & 0xFFFF;
    if (((depthAndFlag & NO_DEAD_SPACE_FLAG) != 0) == noDeadSpace)
      return;
    final int updated = noDeadSpace ? depthAndFlag | NO_DEAD_SPACE_FLAG : depthAndFlag & LOCAL_DEPTH_MASK;
    page.writeShort(BUCKET_LOCAL_DEPTH, (short) updated);
  }

  /** True when the data area holds more bytes than the live entries need: removed and grown entries leave holes. */
  private boolean hasDeadSpace(final BasePage page, final int entryCount) {
    int live = 0;
    for (int i = 0; i < entryCount; i++)
      live += getEntrySize(page, readSlot(page, i));
    return (page.readShort(BUCKET_DATA_END) & 0xFFFF) - BUCKET_CONTENT_START > live;
  }

  /**
   * Compacts a bucket page by rebuilding the data area without dead space (holes): the live entries (referenced by slots)
   * slide down to BUCKET_CONTENT_START in the order they sit on the page, which never overwrites an entry not moved yet, and
   * the slots are updated. The slot order does not change. Nothing is copied out of the page (#9228).
   *
   * @return true if any dead space was reclaimed
   */
  private boolean compactPage(final MutablePage page, final int entryCount) {
    final int oldDataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
    if (entryCount == 0) {
      // Nothing live, so nothing to move, but the data area still ends where the last removed or grown entry left it: a page
      // that emptied that way offered almost no space and could not take any entry (issue #9034 follow-up)
      page.writeShort(BUCKET_DATA_END, (short) BUCKET_CONTENT_START);
      setNoDeadSpace(page, true);
      return oldDataEnd > BUCKET_CONTENT_START;
    }

    // Slots sorted by the offset of their entry: offset in the high bits, slot in the low 16 (at most 65535 slots per page)
    final long[] byOffset = new long[entryCount];
    for (int i = 0; i < entryCount; i++)
      byOffset[i] = ((long) readSlot(page, i) << 16) | i;
    Arrays.sort(byOffset);

    int dataEnd = BUCKET_CONTENT_START;
    for (final long offsetAndSlot : byOffset) {
      final int offset = (int) (offsetAndSlot >>> 16);
      final int size = getEntrySize(page, offset);
      if (offset != dataEnd) {
        page.move(offset, dataEnd, size);
        writeSlot(page, (int) (offsetAndSlot & 0xFFFF), dataEnd);
      }
      dataEnd += size;
    }

    page.writeShort(BUCKET_DATA_END, (short) dataEnd);
    setNoDeadSpace(page, true);
    return dataEnd < oldDataEnd;
  }

  /**
   * For non-unique index: adds a RID to the existing entry of the key, at slot {@code pos} of the page {@code pageNum}, read
   * but not yet taken for modification. Returns false, having changed nothing, when an inline entry cannot grow on its page:
   * the caller then splits the bucket or, with RID lists, moves the RIDs to one (see {@link #moveRIDsToRidList}).
   * <p>
   * The cost of an insert does not depend on the RIDs the key already has (#9228): a RID list takes it at its last page, an
   * inline entry that ends the data area grows where it is, and only an inline entry followed by others is moved to the end
   * of the data area, which RID lists bound to a quarter of a page.
   */
  private boolean addRIDToExistingEntry(final int pageNum, final BasePage readPage, final int entryCount, final int pos,
      final byte[] serializedRID, final long hash) throws IOException {
    final int entryStart = readSlot(readPage, pos);
    final int keyLen = computeKeyLengthFromPage(readPage, entryStart);
    final int headerOffset = entryStart + keyLen;
    final int header = readVarIntFromPage(readPage, headerOffset);

    if (header == 0 && ridLists) {
      appendToRidList(pageNum, readPage, headerOffset, serializedRID, hash);
      return true;
    }

    final int oldHeaderSize = varIntSize(header);
    final int ridsLength = inlineRidsLength(readPage, headerOffset + oldHeaderSize, header);
    final int newHeader = ridLists ? header + serializedRID.length : header + 1;
    final int newHeaderSize = varIntSize(newHeader);
    final int oldEntrySize = keyLen + oldHeaderSize + ridsLength;
    final int newEntrySize = keyLen + newHeaderSize + ridsLength + serializedRID.length;

    final MutablePage page = database.getTransaction().getPageToModify(new PageId(database, fileId, pageNum), pageSize, false);

    if (ridLists && newEntrySize - keyLen > inlineValueLimit) {
      moveRIDsToRidList(pageNum, page, entryCount, pos, serializedRID, hash);
      return true;
    }

    final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
    final int growth = newEntrySize - oldEntrySize;
    if (entryStart + oldEntrySize == dataEnd && growth <= freeSpace(page, entryCount)) {
      // The entry ends the data area: it grows where it is, nothing is copied but the RIDs a longer header pushes by a byte
      final int ridsStart = headerOffset + newHeaderSize;
      if (newHeaderSize != oldHeaderSize)
        page.move(headerOffset + oldHeaderSize, ridsStart, ridsLength);
      page.writeNumber(headerOffset, newHeader);
      page.writeByteArray(ridsStart + ridsLength, serializedRID);
      page.writeShort(BUCKET_DATA_END, (short) (dataEnd + growth));
      return true;
    }

    int oldStart = entryStart;
    if (newEntrySize > freeSpace(page, entryCount)) {
      // Reclaim the dead space of removed and moved entries, if there is any: the flag spares the work on a page known to have none
      if (hasNoDeadSpace(page) || !compactPage(page, entryCount) || newEntrySize > freeSpace(page, entryCount))
        return false;
      // the compaction keeps the slots where they are, only the data moves
      oldStart = readSlot(page, pos);
    }

    // Move the entry to the end of the data area, one RID longer: the old bytes become dead space
    final int newStart = page.readShort(BUCKET_DATA_END) & 0xFFFF;
    page.move(oldStart, newStart, keyLen);
    page.writeNumber(newStart + keyLen, newHeader);
    page.move(oldStart + keyLen + oldHeaderSize, newStart + keyLen + newHeaderSize, ridsLength);
    page.writeByteArray(newStart + keyLen + newHeaderSize + ridsLength, serializedRID);
    page.writeShort(BUCKET_DATA_END, (short) (newStart + newEntrySize));
    writeSlot(page, pos, newStart);
    setNoDeadSpace(page, false);
    return true;
  }

  /**
   * For non-unique index: removes a specific RID from an entry. Returns 1 if removed, 0 otherwise.
   * If the entry has only one RID left, removes the entire entry.
   */
  private int removeRIDFromEntry(final MutablePage page, final int entryCount, final int pos, final RID targetRID, final long hash)
      throws IOException {
    final int entryStart = readSlot(page, pos);
    final int keyLen = computeKeyLengthFromPage(page, entryStart);
    final int headerOffset = entryStart + keyLen;

    final int header = readVarIntFromPage(page, headerOffset);
    final byte[] target = serializeCompressedRID(targetRID);

    if (header == 0 && ridLists)
      return removeRIDFromRidList(page, entryCount, pos, headerOffset, target, hash);

    final int headerSize = varIntSize(header);
    final int ridsStart = headerOffset + headerSize;
    final int ridsLength = inlineRidsLength(page, ridsStart, header);
    final int entryEnd = ridsStart + ridsLength;
    for (int ridOffset = ridsStart; ridOffset < entryEnd; ) {
      final int ridSize = compressedRIDSizeFromPage(page, ridOffset);
      if (!sameBytes(page, ridOffset, target, ridSize)) {
        ridOffset += ridSize;
        continue;
      }

      if (ridSize == ridsLength) {
        // the last RID of the entry
        dropSlot(page, entryCount, pos);
        return 1;
      }

      // Rewritten in place, shorter: key, the header, the RIDs before the removed one and the ones after it
      final int newHeader = ridLists ? header - ridSize : header - 1;
      final int newHeaderSize = varIntSize(newHeader);
      final int beforeLength = ridOffset - ridsStart;
      if (newHeaderSize != headerSize)
        page.move(ridsStart, headerOffset + newHeaderSize, beforeLength);
      page.move(ridOffset + ridSize, headerOffset + newHeaderSize + beforeLength, entryEnd - ridOffset - ridSize);
      page.writeNumber(headerOffset, newHeader);

      final int removedBytes = ridSize + headerSize - newHeaderSize;
      final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
      if (entryEnd == dataEnd)
        // the entry ends the data area: the bytes it no longer uses go back to the free space
        page.writeShort(BUCKET_DATA_END, (short) (dataEnd - removedBytes));
      else
        // the tail is dead space reclaimed by the next compaction
        setNoDeadSpace(page, false);
      return 1;
    }
    return 0;
  }

  /**
   * The value of an inline entry of a non-unique index starts with a header: the number of its RIDs up to version 2, the
   * bytes they take from version 3, so the size of an entry is known without decoding its RIDs (#9228). It is never 0 (the
   * last RID going away removes the entry): from version 3, 0 marks the entry whose RIDs live in a RID list.
   */
  private int singleRidHeader(final byte[] serializedRID) {
    return ridLists ? serializedRID.length : 1;
  }

  /** Bytes taken by the inline RIDs starting at {@code offset}, whose entry has the given header. */
  private int inlineRidsLength(final BasePage page, final int offset, final int header) {
    return ridLists ? header : ridsLength(page, offset, header);
  }

  /** Number of compressed RIDs in the {@code length} bytes from {@code offset}. */
  private static int countRids(final BasePage page, final int offset, final int length) {
    final int end = offset + length;
    int count = 0;
    for (int current = offset; current < end; count++)
      current += compressedRIDSizeFromPage(page, current);
    return count;
  }

  /** Bytes taken by {@code ridCount} compressed RIDs starting at {@code offset}. */
  private int ridsLength(final BasePage page, final int offset, final int ridCount) {
    int end = offset;
    for (int r = 0; r < ridCount; r++)
      end += compressedRIDSizeFromPage(page, end);
    return end - offset;
  }

  private static byte[] readBytes(final BasePage page, final int offset, final int length) {
    final byte[] bytes = new byte[length];
    page.readByteArray(offset, bytes);
    return bytes;
  }

  private static boolean sameBytes(final BasePage page, final int offset, final byte[] value, final int length) {
    if (length != value.length)
      return false;
    for (int i = 0; i < length; i++)
      if (page.readByte(offset + i) != value[i])
        return false;
    return true;
  }

  // ─── RID LISTS (VERSION 3, ISSUE #9228) ──────────────────

  /**
   * Moves the RIDs of the inline entry at {@code pos}, plus a new one, to a new RID list, and rewrites the entry as the
   * pointer to it.
   */
  private void moveRIDsToRidList(final int pageNum, final MutablePage page, final int entryCount, final int pos,
      final byte[] serializedRID, final long hash) throws IOException {
    final int entryStart = readSlot(page, pos);
    final int keyLen = computeKeyLengthFromPage(page, entryStart);
    final int headerOffset = entryStart + keyLen;
    final int header = readVarIntFromPage(page, headerOffset);
    final int headerSize = varIntSize(header);
    final int ridsStart = headerOffset + headerSize;
    final int ridsLength = inlineRidsLength(page, ridsStart, header);

    // The inline RIDs are at most a quarter of a bucket page plus the RID that did not fit, always less than a RID list page
    final MutablePage listPage = allocateRidListPage(hash);
    listPage.writeByteArray(RID_PAGE_CONTENT_START, readBytes(page, ridsStart, ridsLength));
    listPage.writeByteArray(RID_PAGE_CONTENT_START + ridsLength, serializedRID);
    listPage.writeShort(RID_PAGE_DATA_END, (short) (RID_PAGE_CONTENT_START + ridsLength + serializedRID.length));
    listPage.writeShort(RID_PAGE_COUNT, (short) (countRids(page, ridsStart, ridsLength) + 1));
    final int listPageNum = listPage.getPageId().getPageNumber();

    if (headerSize + ridsLength >= RID_LIST_VALUE_SIZE) {
      // The pointer replaces the value in place: what is left of the old value is dead space
      writeRidListValue(page, headerOffset, listPageNum, listPageNum);
      if (headerSize + ridsLength > RID_LIST_VALUE_SIZE)
        setNoDeadSpace(page, false);
      return;
    }

    // A value shorter than the pointer (a page too full to grow an entry of few RIDs): the entry is written again elsewhere
    final byte[] entry = new byte[keyLen + RID_LIST_VALUE_SIZE];
    page.readByteArray(entryStart, entry, 0, keyLen);
    entry[keyLen] = 0;
    final Binary pointers = new Binary(entry);
    pointers.putInt(keyLen + 1, listPageNum);
    pointers.putInt(keyLen + 1 + Binary.INT_SERIALIZED_SIZE, listPageNum);
    dropSlot(page, entryCount, pos);
    insertRawEntry(pageNum, entry, hash);
  }

  private void writeRidListValue(final MutablePage page, final int valueOffset, final int headPage, final int tailPage) {
    page.writeNumber(valueOffset, 0);
    page.writeInt(valueOffset + 1, headPage);
    page.writeInt(valueOffset + 1 + Binary.INT_SERIALIZED_SIZE, tailPage);
  }

  /**
   * Appends a RID to the RID list of the entry whose value starts at {@code valueOffset} of the page {@code pageNum}: the last
   * page of the list is written, and the entry only when a new last page is chained.
   */
  private void appendToRidList(final int pageNum, final BasePage entryPage, final int valueOffset, final byte[] serializedRID,
      final long hash) throws IOException {
    final int tailPageNum = entryPage.readInt(valueOffset + 1 + Binary.INT_SERIALIZED_SIZE);
    final MutablePage tail = ridListPageToModify(tailPageNum, hash);
    final int dataEnd = tail.readShort(RID_PAGE_DATA_END) & 0xFFFF;

    if (dataEnd + serializedRID.length <= ridListPageEnd()) {
      tail.writeByteArray(dataEnd, serializedRID);
      tail.writeShort(RID_PAGE_DATA_END, (short) (dataEnd + serializedRID.length));
      tail.writeShort(RID_PAGE_COUNT, (short) ((tail.readShort(RID_PAGE_COUNT) & 0xFFFF) + 1));
      return;
    }

    final MutablePage newTail = allocateRidListPage(hash);
    newTail.writeByteArray(RID_PAGE_CONTENT_START, serializedRID);
    newTail.writeShort(RID_PAGE_DATA_END, (short) (RID_PAGE_CONTENT_START + serializedRID.length));
    newTail.writeShort(RID_PAGE_COUNT, (short) 1);
    final int newTailNum = newTail.getPageId().getPageNumber();
    tail.writeInt(RID_PAGE_NEXT, newTailNum);
    database.getTransaction().getPageToModify(new PageId(database, fileId, pageNum), pageSize, false)
        .writeInt(valueOffset + 1 + Binary.INT_SERIALIZED_SIZE, newTailNum);
  }

  /**
   * Removes a RID from the RID list of the entry at {@code pos}. The list stays dense: the bytes after the RID on its page
   * close the gap, a page left empty is unlinked and freed, and a page whose next one now fits in it takes its RIDs and frees
   * it. The entry goes away with the last RID of the list. Returns 1 if the RID was found.
   */
  private int removeRIDFromRidList(final MutablePage entryPage, final int entryCount, final int pos, final int valueOffset,
      final byte[] target, final long hash) throws IOException {
    final TransactionContext tx = database.getTransaction();
    final int tailPageNum = entryPage.readInt(valueOffset + 1 + Binary.INT_SERIALIZED_SIZE);
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;
    int previous = NO_OVERFLOW_PAGE;

    for (int current = entryPage.readInt(valueOffset + 1); current != NO_OVERFLOW_PAGE; ) {
      if (++chainSteps > maxChainPages)
        throw corruptedRidList(current, "the list is cyclic");
      final BasePage readPage = ridListPage(current, hash);
      final int dataEnd = readPage.readShort(RID_PAGE_DATA_END) & 0xFFFF;
      final int next = readPage.readInt(RID_PAGE_NEXT);

      for (int offset = RID_PAGE_CONTENT_START; offset < dataEnd; ) {
        final int ridSize = compressedRIDSizeFromPage(readPage, offset);
        if (!sameBytes(readPage, offset, target, ridSize)) {
          offset += ridSize;
          continue;
        }

        // Everything this removal may touch is checked before the first write: the previous page was walked already, the next one
        // is validated here, so a damaged list fails without leaving half a change in the transaction
        final BasePage nextPage = next != NO_OVERFLOW_PAGE ? ridListPage(next, hash) : null;

        final MutablePage page = tx.getPageToModify(new PageId(database, fileId, current), pageSize, false);
        page.move(offset + ridSize, offset, dataEnd - offset - ridSize);
        final int newDataEnd = dataEnd - ridSize;
        final int newCount = (page.readShort(RID_PAGE_COUNT) & 0xFFFF) - 1;
        page.writeShort(RID_PAGE_DATA_END, (short) newDataEnd);
        page.writeShort(RID_PAGE_COUNT, (short) newCount);

        if (newCount == 0) {
          // Unlink the empty page
          if (previous == NO_OVERFLOW_PAGE && next == NO_OVERFLOW_PAGE)
            // it was the only one: the key has no RID left
            dropSlot(entryPage, entryCount, pos);
          else if (previous == NO_OVERFLOW_PAGE)
            entryPage.writeInt(valueOffset + 1, next);
          else {
            ridListPageToModify(previous, hash).writeInt(RID_PAGE_NEXT, next);
            if (current == tailPageNum)
              entryPage.writeInt(valueOffset + 1 + Binary.INT_SERIALIZED_SIZE, previous);
          }
          freeRidListPage(page);
        } else if (nextPage != null) {
          // Merge the next page in this one when its RIDs fit, so deletions do not leave a chain of half empty pages
          final int nextLength = (nextPage.readShort(RID_PAGE_DATA_END) & 0xFFFF) - RID_PAGE_CONTENT_START;
          if (nextLength <= ridListPageEnd() - newDataEnd) {
            final byte[] rids = new byte[nextLength];
            nextPage.readByteArray(RID_PAGE_CONTENT_START, rids);
            page.writeByteArray(newDataEnd, rids);
            page.writeShort(RID_PAGE_DATA_END, (short) (newDataEnd + nextLength));
            page.writeShort(RID_PAGE_COUNT, (short) (newCount + (nextPage.readShort(RID_PAGE_COUNT) & 0xFFFF)));
            page.writeInt(RID_PAGE_NEXT, nextPage.readInt(RID_PAGE_NEXT));
            if (next == tailPageNum)
              entryPage.writeInt(valueOffset + 1 + Binary.INT_SERIALIZED_SIZE, current);
            freeRidListPage(tx.getPageToModify(new PageId(database, fileId, next), pageSize, false));
          }
        }
        return 1;
      }

      previous = current;
      current = next;
    }
    return 0;
  }

  /**
   * Frees the whole RID list of a key whose entry is removed and returns the number of RIDs it held.
   */
  private int releaseRidList(final int headPage, final int tailPage, final long hash) throws IOException {
    int ridCount = 0;
    final int maxChainPages = getTotalPages();
    int chainSteps = 0;
    int last = NO_OVERFLOW_PAGE;
    // read-only first: a damaged list fails before any page is changed
    for (int current = headPage; current != NO_OVERFLOW_PAGE; ) {
      if (++chainSteps > maxChainPages)
        throw corruptedRidList(current, "the list is cyclic");
      final BasePage page = ridListPage(current, hash);
      ridCount += page.readShort(RID_PAGE_COUNT) & 0xFFFF;
      last = current;
      current = page.readInt(RID_PAGE_NEXT);
    }
    if (last != tailPage)
      // freeing from the head to a wrong last page would put on the free list pages still in use, or lose some
      throw corruptedRidList(tailPage, "the entry names it as the last page of the list, which ends at page " + last);

    MutablePage lastPage = null;
    for (int current = headPage; current != NO_OVERFLOW_PAGE; ) {
      lastPage = database.getTransaction().getPageToModify(new PageId(database, fileId, current), pageSize, false);
      lastPage.writeShort(RID_PAGE_MARKER, (short) RID_FREE_PAGE_MARKER);
      current = lastPage.readInt(RID_PAGE_NEXT);
    }
    // the pages of the list are already chained: the whole list goes on top of the free list at once
    pushOnFreeList(headPage, lastPage);
    return ridCount;
  }

  /** Marks one RID list page (already taken for modification) as free and puts it on top of the free list. */
  private void freeRidListPage(final MutablePage page) throws IOException {
    page.writeShort(RID_PAGE_MARKER, (short) RID_FREE_PAGE_MARKER);
    pushOnFreeList(page.getPageId().getPageNumber(), page);
  }

  /**
   * Puts the chain of free RID list pages from {@code firstPage} to {@code lastPage} (already taken for modification and
   * marked free) on top of the free list.
   */
  private void pushOnFreeList(final int firstPage, final MutablePage lastPage) throws IOException {
    lastPage.writeInt(RID_PAGE_NEXT, readFreeRidListPage());
    writeFreeRidListPage(firstPage);
  }

  /**
   * Returns an empty RID list page owned by the key of the given hash: the first free one if any, a new one otherwise.
   */
  private MutablePage allocateRidListPage(final long hash) throws IOException {
    final TransactionContext tx = database.getTransaction();
    final int free = readFreeRidListPage();
    final MutablePage page;
    if (free != NO_OVERFLOW_PAGE) {
      if (!isValidBucketPage(free))
        throw corruptedRidList(free, "the free list reaches an invalid page");
      page = tx.getPageToModify(new PageId(database, fileId, free), pageSize, false);
      if (!isFreeRidListPage(page))
        throw corruptedRidList(free, "the free list reaches a page that is not a free RID list page");
      writeFreeRidListPage(page.readInt(RID_PAGE_NEXT));
    } else {
      final int newPageNum = getTotalPages();
      page = tx.addPage(new PageId(database, fileId, newPageNum), pageSize);
      updatePageCount(newPageNum + 1);
    }
    page.writeShort(RID_PAGE_MARKER, (short) RID_LIST_PAGE_MARKER);
    page.writeShort(RID_PAGE_COUNT, (short) 0);
    page.writeInt(RID_PAGE_NEXT, NO_OVERFLOW_PAGE);
    page.writeShort(RID_PAGE_DATA_END, (short) RID_PAGE_CONTENT_START);
    page.writeLong(RID_PAGE_OWNER, hash);
    return page;
  }

  /** A page of a RID list of the key of the given hash, read for a change: anything else means the index is corrupted. */
  private BasePage ridListPage(final int pageNum, final long hash) throws IOException {
    if (!isValidBucketPage(pageNum))
      throw corruptedRidList(pageNum, "the page does not exist");
    final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, pageNum), pageSize);
    if (!isRidListPageOf(page, hash))
      throw corruptedRidList(pageNum, "the page is not a RID list page of the key");
    return page;
  }

  private MutablePage ridListPageToModify(final int pageNum, final long hash) throws IOException {
    ridListPage(pageNum, hash);
    return database.getTransaction().getPageToModify(new PageId(database, fileId, pageNum), pageSize, false);
  }

  /** True for a RID list page, in use or free. */
  static boolean isRidListPage(final BasePage page) {
    final int marker = page.readShort(RID_PAGE_MARKER) & 0xFFFF;
    return marker == RID_LIST_PAGE_MARKER || marker == RID_FREE_PAGE_MARKER;
  }

  static boolean isFreeRidListPage(final BasePage page) {
    return (page.readShort(RID_PAGE_MARKER) & 0xFFFF) == RID_FREE_PAGE_MARKER;
  }

  /** True for a page in use by the RID list of the key of the given hash, whose data area is sane. */
  private boolean isRidListPageOf(final BasePage page, final long hash) {
    if ((page.readShort(RID_PAGE_MARKER) & 0xFFFF) != RID_LIST_PAGE_MARKER || page.readLong(RID_PAGE_OWNER) != hash)
      return false;
    final int dataEnd = page.readShort(RID_PAGE_DATA_END) & 0xFFFF;
    return dataEnd >= RID_PAGE_CONTENT_START && dataEnd <= ridListPageEnd();
  }

  /** Offset past the last byte a RID list page can hold. */
  private int ridListPageEnd() {
    return pageSize - BasePage.PAGE_HEADER_SIZE;
  }

  private int readFreeRidListPage() throws IOException {
    return metaPage().readInt(metaTailOffset + 3 * Binary.INT_SERIALIZED_SIZE);
  }

  private void writeFreeRidListPage(final int pageNum) throws IOException {
    database.getTransaction().getPageToModify(new PageId(database, fileId, 0), pageSize, false)
        .writeInt(metaTailOffset + 3 * Binary.INT_SERIALIZED_SIZE, pageNum);
  }

  private IndexException corruptedRidList(final int page, final String reason) {
    return new IndexException(
        "Invalid RID list page " + page + " in hash index '" + getName() + "' (fileId=" + fileId + ", totalPages=" + getTotalPages()
            + "): " + reason + ". The index is corrupted, please rebuild it (DROP and recreate it).");
  }

  // ─── SLOTTED PAGE ACCESS ────────────────────────────────

  /**
   * Returns the page-relative offset where slot[i] is stored.
   * Slots grow from the end of the usable page area downward.
   */
  private int slotPosition(final int index) {
    return (pageSize - BasePage.PAGE_HEADER_SIZE) - (index + 1) * slotSize;
  }

  /**
   * Reads the data offset stored in slot[index].
   */
  private int readSlot(final BasePage page, final int index) {
    return page.readShort(slotPosition(index)) & 0xFFFF;
  }

  /**
   * Writes a data offset into slot[index]. The tag of the slot (current layout) is left untouched.
   */
  private void writeSlot(final MutablePage page, final int index, final int dataOffset) {
    page.writeShort(slotPosition(index), (short) dataOffset);
  }

  /**
   * Reads the hash tag of slot[index]. Only the current layout has one.
   */
  private int readTag(final BasePage page, final int index) {
    return page.readByte(slotPosition(index) + SLOT_SIZE) & 0xFF;
  }

  private void writeTag(final MutablePage page, final int index, final int tag) {
    page.writeByte(slotPosition(index) + SLOT_SIZE, (byte) tag);
  }

  /**
   * The tag a key is filed under: the low byte of its hash. The directory takes the TOP bits of the hash, so every key
   * of a bucket shares them and only the low ones can tell the keys of a bucket apart.
   */
  static int tagOf(final long hash) {
    return (int) hash & 0xFF;
  }

  /**
   * Returns the available free space in a bucket page.
   * Free space = gap between data end and the start of the slot directory.
   */
  private int freeSpace(final BasePage page, final int entryCount) {
    final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
    final int slotStart = (pageSize - BasePage.PAGE_HEADER_SIZE) - entryCount * slotSize;
    return slotStart - dataEnd;
  }

  /**
   * Returns the position of the first slot at or after {@code from} whose entry has the given key, or -1.
   * <p>
   * Current layout: the slots are scanned comparing the 1-byte tag, and the key bytes only on a tag hit. Legacy sorted
   * layout: the entries of a key are adjacent, so the first one is found by binary search and the following ones are
   * the slots right after it.
   */
  private int findNextEntry(final BasePage page, final int entryCount, final byte[] searchKey, final int tag, final int from) {
    if (tagged) {
      for (int i = from; i < entryCount; i++)
        if (readTag(page, i) == tag && keysMatch(page, readSlot(page, i), searchKey))
          return i;
      return -1;
    }

    if (from == 0)
      return findFirstEntry(page, entryCount, searchKey);
    return from < entryCount && keysMatch(page, readSlot(page, from), searchKey) ? from : -1;
  }

  /**
   * Binary search using slot directory (legacy sorted layout). Returns position of first match or -1.
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

  /**
   * Finds insertion point using slot directory (legacy sorted layout). Returns position for new entry.
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
   * Inserts entry into page using slotted page layout (see {@link #appendEntry}).
   */
  private void insertEntryInSlottedPage(final MutablePage page, final int entryCount,
      final byte[] serializedKey, final byte[] serializedRID, final long hash) {
    final byte[] entryBytes;
    if (unique) {
      entryBytes = new byte[serializedKey.length + serializedRID.length];
      System.arraycopy(serializedKey, 0, entryBytes, 0, serializedKey.length);
      System.arraycopy(serializedRID, 0, entryBytes, serializedKey.length, serializedRID.length);
    } else {
      final byte[] ridCountBytes = encodeVarInt(singleRidHeader(serializedRID));
      entryBytes = new byte[serializedKey.length + ridCountBytes.length + serializedRID.length];
      System.arraycopy(serializedKey, 0, entryBytes, 0, serializedKey.length);
      System.arraycopy(ridCountBytes, 0, entryBytes, serializedKey.length, ridCountBytes.length);
      System.arraycopy(serializedRID, 0, entryBytes, serializedKey.length + ridCountBytes.length, serializedRID.length);
    }

    appendEntry(page, entryCount, entryBytes, hash);
  }

  /**
   * Appends an already serialized entry (key + value) at the end of the data area of a page that has room for it.
   * <p>
   * Current layout: the slot is appended after the last one, so the insert writes the entry, one slot and the page
   * header. Legacy sorted layout: the slot goes to its sorted position, shifting the slots after it.
   */
  private void appendEntry(final MutablePage page, final int entryCount, final byte[] entryBytes, final long hash) {
    final int dataEnd = page.readShort(BUCKET_DATA_END) & 0xFFFF;
    page.writeByteArray(dataEnd, entryBytes);
    page.writeShort(BUCKET_DATA_END, (short) (dataEnd + entryBytes.length));

    if (tagged) {
      writeSlot(page, entryCount, dataEnd);
      writeTag(page, entryCount, tagOf(hash));
    } else {
      final int keyLen = computeKeyLengthFromEntry(entryBytes, 0);
      final byte[] serializedKey = new byte[keyLen];
      System.arraycopy(entryBytes, 0, serializedKey, 0, keyLen);
      final int insertPos = findInsertionPoint(page, entryCount, serializedKey);

      for (int i = entryCount; i > insertPos; i--)
        writeSlot(page, i, readSlot(page, i - 1));
      writeSlot(page, insertPos, dataEnd);
    }

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
      final int header = readVarIntFromPage(page, offset + keyLen);
      final int headerSize = varIntSize(header);
      if (ridLists)
        // O(1): the header is the byte length of the RIDs, or 0 for the pointer to a RID list
        return total + (header == 0 ? RID_LIST_VALUE_SIZE : headerSize + header);
      total += headerSize + ridsLength(page, offset + keyLen + headerSize, header);
    }
    return total;
  }

  /** Hash of the key of the entry at {@code offset}, the same {@link #serializeKeys} + {@link #murmurHash64} give. */
  private static long hashKeyAt(final BasePage page, final int offset, final int keyLen) {
    final byte[] key = new byte[keyLen];
    page.readByteArray(offset, key);
    return murmurHash64(key);
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
    case BinaryTypes.TYPE_BINARY:
      // Length-prefixed: read the varint length, then the bytes
      return lengthPrefixedSize(page, offset);
    case BinaryTypes.TYPE_COMPRESSED_RID:
      return compressedRIDSizeFromPage(page, offset);
    case BinaryTypes.TYPE_DECIMAL: {
      // scale (varInt) + unscaledValue bytes (length-prefixed)
      final int scaleSize = getVarNumberSize(page, offset);
      return scaleSize + lengthPrefixedSize(page, offset + scaleSize);
    }
    case BinaryTypes.TYPE_UUID:
      return 16; // Two longs
    case BinaryTypes.TYPE_LOCAL_TIME:
      return getVarNumberSize(page, offset);
    case BinaryTypes.TYPE_OFFSET_TIME:
      return getVarNumbersSize(page, offset, 2);
    case BinaryTypes.TYPE_DURATION:
      return getVarNumbersSize(page, offset, 4);
    case BinaryTypes.TYPE_ZONED_DATETIME: {
      // epochSecond, nano, then a flag: 0 = offset seconds (varnumber), 1 = zone id (length-prefixed)
      final int head = getVarNumbersSize(page, offset, 2);
      if (page.readByte(offset + head) == 0)
        return head + 1 + getVarNumberSize(page, offset + head + 1);
      return head + 1 + lengthPrefixedSize(page, offset + head + 1);
    }
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
    case BinaryTypes.TYPE_LOCAL_TIME:
      return getVarNumberSizeFromBytes(data, offset);
    case BinaryTypes.TYPE_OFFSET_TIME:
      return getVarNumbersSizeFromBytes(data, offset, 2);
    case BinaryTypes.TYPE_DURATION:
      return getVarNumbersSizeFromBytes(data, offset, 4);
    case BinaryTypes.TYPE_ZONED_DATETIME: {
      final int head = getVarNumbersSizeFromBytes(data, offset, 2);
      if (data[offset + head] == 0)
        return head + 1 + getVarNumberSizeFromBytes(data, offset + head + 1);
      final int[] lenAndSize = readVarIntAndSizeFromBytes(data, offset + head + 1);
      return head + 1 + lenAndSize[1] + lenAndSize[0];
    }
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
    return sameBytes(page, offset, searchKey, computeKeyLengthFromPage(page, offset));
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

  /** A varInt takes up to 10 bytes. */
  private static final int MAX_VARINT_SIZE = 10;

  private RID readCompressedRID(final BasePage page, final int offset) {
    // Compressed RID: bucketId (varInt) + position (varInt)
    final long bucketId = readVarLong(page, offset);
    final long position = readVarLong(page, offset + varIntLength(page, offset));
    return new RID((int) bucketId, position);
  }

  private static int compressedRIDSizeFromPage(final BasePage page, final int offset) {
    final int bucketIdLength = varIntLength(page, offset);
    return bucketIdLength + varIntLength(page, offset + bucketIdLength);
  }

  private int compressedRIDSizeFromBytes(final byte[] data, final int offset) {
    final Binary view = new Binary(data);
    view.position(offset);
    view.getNumber(); // bucketId
    view.getNumber(); // position
    return view.position() - offset;
  }

  // ─── VARINT HELPERS ──────────────────────────────────────
  // The page is read in place, one byte at a time and never past the last byte of the number: a copied window of a fixed
  // size overran the page buffer at its end (#9034), and its allocation was most of the cost of an insert into a key with
  // many RIDs, whose size is the sum of the sizes of its RIDs (#9228).

  /** Bytes taken by the varInt at {@code offset}. */
  private static int varIntLength(final BasePage page, final int offset) {
    int length = 1;
    while ((page.readByte(offset + length - 1) & 0x80) != 0)
      if (++length > MAX_VARINT_SIZE)
        throw new IndexException("Invalid variable length number at offset " + offset + " of page " + page.getPageId());
    return length;
  }

  /** Reads the unsigned varInt at {@code offset}, the encoding of {@link Binary#getUnsignedNumber()}. */
  private static long readUnsignedVarLong(final BasePage page, final int offset) {
    long value = 0;
    for (int i = offset, shift = 0; ; i++, shift += 7) {
      if (shift > 63)
        throw new IndexException("Invalid variable length number at offset " + offset + " of page " + page.getPageId());
      final byte b = page.readByte(i);
      value |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0)
        return value;
    }
  }

  /** Reads the signed (zigzag) varInt at {@code offset}, the encoding of {@link Binary#getNumber()}. */
  private static long readVarLong(final BasePage page, final int offset) {
    final long raw = readUnsignedVarLong(page, offset);
    return (raw >>> 1) ^ -(raw & 1);
  }

  private int readVarIntFromPage(final BasePage page, final int offset) {
    return (int) readVarLong(page, offset);
  }

  private int readVarIntFromBytes(final byte[] data, final int offset) {
    final Binary view = new Binary(data);
    view.position(offset);
    return (int) view.getNumber();
  }

  /** Bytes taken by a length-prefixed value (STRING, BINARY): the unsigned varInt length, then the bytes. */
  private static int lengthPrefixedSize(final BasePage page, final int offset) {
    return varIntLength(page, offset) + (int) readUnsignedVarLong(page, offset);
  }

  private int[] readVarIntAndSizeFromBytes(final byte[] data, final int offset) {
    final Binary view = new Binary(data);
    view.position(offset);
    final int startPos = view.position();
    final long value = view.getUnsignedNumber();
    return new int[] { (int) value, view.position() - startPos };
  }

  private static int getVarNumberSize(final BasePage page, final int offset) {
    return varIntLength(page, offset);
  }

  private static int getVarNumbersSize(final BasePage page, final int offset, final int count) {
    int end = offset;
    for (int i = 0; i < count; i++)
      end += varIntLength(page, end);
    return end - offset;
  }

  private int getVarNumbersSizeFromBytes(final byte[] data, final int offset, final int count) {
    final Binary view = new Binary(data);
    view.position(offset);
    final int startPos = view.position();
    for (int i = 0; i < count; i++)
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
