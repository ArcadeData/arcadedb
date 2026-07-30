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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.BufferBloomFilter;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MurmurHash;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.log.LogManager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * One bloom filter per compacted series of an {@link LSMTreeIndexCompacted}, so a point lookup can skip a series that
 * cannot hold the key without reading any of its pages (issue #5517).
 * <p>
 * A lookup walks every published series newest to oldest. The series' root page already rules out a key outside the
 * series' key RANGE, but says nothing about a key inside the range that the series does not hold - and that is the
 * common case of a bulk load, where each series spans nearly the whole key space and the unique-index duplicate check
 * therefore reads one data page per series, per record, only to find nothing.
 * <p>
 * <b>Why a {@link PaginatedComponent} and not a side file.</b> A plain file next to the index would not be replicated
 * over HA, would not be transactional, would not be in a backup and would not follow the index through DROP INDEX. As
 * a component its pages flow through the PageManager exactly like the compacted index pages they describe, over the
 * very same code path, so the filter can never travel differently from the data.
 * <p>
 * <b>Layout: strictly APPEND-ONLY.</b> Publishing a series appends its bit pages and then one directory page listing
 * every filter published so far, so the live directory is always the LAST page of the file. Nothing is ever rewritten
 * in place, which buys two things that in-place updates could not: a crash mid-publish leaves the previous directory
 * untouched and the file readable, and HA ships a compaction by shipping the page range each file GREW by - a rewritten
 * page 0 would simply not travel, leaving followers with a directory that disagrees with their own bits.
 * <p>
 * A directory page is {@code [magic][formatVersion][entryCount][checksum]} followed by {@value #DIRECTORY_ENTRY_SIZE}-byte
 * entries {@code [seriesRootPage][seriesPages][seriesFingerprint][firstFilterPage][filterPages][slotsPerBlock][probes][keys]}. A series'
 * bits are split into {@code filterPages} independent BLOCKS of one page each and a key is routed to exactly one block,
 * so a probe reads ONE {@value #PAGE_SIZE}-byte page however large the series is - see {@link #PAGE_SIZE} for why the
 * filter does not inherit the index's much larger page.
 * <p>
 * <b>Identity, and what a DOWNGRADE does.</b> An entry names its series by position, so it also carries the series'
 * page count and a fingerprint of its last data page; both are re-checked live on every probe, and either one
 * disagreeing means "search the series". That matters because a build that predates this component does not know the
 * {@code .bfidx} extension at all: it opens the database, compacts, and never touches the filter file. Its incremental
 * rounds only APPEND series, so the entries it leaves behind still describe the series they were built from - but a
 * round it rolls back can free a position that a later round reuses, and then only the shape and the fingerprint stand
 * between a stale entry and a series it knows nothing about. Downgrading across this feature and back is therefore not
 * free, and {@code arcadedb.indexBloomFilterRate=0} on the upgraded build (or a full compaction, which writes a new
 * file the old entries cannot name) is the way to make it so.
 * <p>
 * <b>The rule that must not break.</b> A false positive costs the lookup that would have happened anyway; a false
 * NEGATIVE hides data - a lookup skipped, a record reported missing, a unique-index duplicate check letting a duplicate
 * through - silently, with the index file intact. Every decision here is biased that way: a series with no directory
 * entry, an entry whose shape does not match the series the reader is looking at, a partial or null key, an unreadable
 * page - all answer "search this series". And before a filter is published, EVERY key just written is probed back
 * exhaustively ({@link #verifyNoFalseNegatives}); a single miss abandons the filter instead of publishing it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMTreeIndexBloomFilter extends PaginatedComponent {
  public static final String FILE_EXT        = "bfidx";
  public static final int    CURRENT_VERSION = 1;

  /** Suffix appended to the compacted index component name; the pair is resolved by name at schema-load time. */
  static final String NAME_SUFFIX = "_bf";

  /**
   * Page size of the filter file, deliberately NOT the index's.
   * <p>
   * A probe reads exactly one page, so the page size IS the I/O cost of a probe - and an LSM index defaults to 256 KB
   * pages, which would have the filter read as much as the data page it exists to avoid. It also decides the waste:
   * every series rounds its bits up to a page and spends one more on its directory, so at 256 KB a small series cost
   * 512 KB to describe with 30 KB of bits.
   * <p>
   * 8 KB is small enough that both of those disappear and large enough that a block still holds ~6.8k keys at 1%, so
   * the occupancy skew of the blocked layout stays under 2% - and one directory page still lists 291 series.
   */
  static final int PAGE_SIZE = 8_192;

  /** Seed of the key hash. Changing it invalidates every filter on disk, so it travels with {@link #FORMAT_VERSION}. */
  static final int HASH_SEED = 0x5bf1e995;

  private static final int MAGIC                 = 0x424C4F4D; // "BLOM"
  private static final int FORMAT_VERSION        = 2;
  private static final int DIRECTORY_HEADER_SIZE = 4 * Binary.INT_SERIALIZED_SIZE;
  private static final int DIRECTORY_ENTRY_SIZE  = 8 * Binary.INT_SERIALIZED_SIZE;

  /**
   * Pages to look back through for the newest directory when the last page of the file is not one, i.e. after a crash
   * between a publish's bit pages and its directory page. It only has to cover the pages a single publish appends;
   * beyond it the file is simply read as having no filters.
   */
  private static final int MAX_DIRECTORY_LOOKBACK = 4_096;

  /**
   * Past ~12 probes the false-positive rate barely improves while the cost of a lookup keeps growing linearly. The
   * bound matters for a SMALL series, where a whole page of bits for a handful of keys would otherwise ask for
   * thousands of probes.
   */
  private static final int MAX_PROBES = 12;

  /**
   * Directory of the filters, keyed by the series' root page. Rebuilt whole and swapped in one go, so a reader either
   * sees the directory before a publish or after it, never halfway - {@code volatile} is the safe-publication edge
   * {@link BufferBloomFilter} requires for its lock-free reads.
   */
  private volatile Map<Integer, Entry> directory = Map.of();

  /** One published filter. Immutable once built. */
  record Entry(int seriesRootPage, int seriesPages, int seriesFingerprint, int firstFilterPage, int filterPages,
               int slotsPerBlock, int probes, int keys) {
  }

  /** Constructor used when a compaction creates the filter file. */
  LSMTreeIndexBloomFilter(final DatabaseInternal database, final String name, final String filePath, final int pageSize)
      throws IOException {
    super(database, name, filePath, FILE_EXT, ComponentFile.MODE.READ_WRITE, pageSize, CURRENT_VERSION);
  }

  /** Constructor used at load time. */
  protected LSMTreeIndexBloomFilter(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
  }

  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new LSMTreeIndexBloomFilter(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /** The compacted index component name this file belongs to, derived from its own name. */
  public String getOwnerName() {
    return componentName.endsWith(NAME_SUFFIX) ?
        componentName.substring(0, componentName.length() - NAME_SUFFIX.length()) :
        null;
  }

  @Override
  public Object getMainComponent() {
    return this;
  }

  /**
   * Reads the newest directory into RAM, i.e. the last page of the file - or, when a crash cut a publish between its
   * bit pages and its directory page, the last complete directory before it. Never throws: a filter file that cannot
   * be read simply filters nothing, which is exactly the behaviour of an index without one.
   */
  public void loadDirectory() {
    try {
      final int totalPages = getTotalPages();
      final int floor = Math.max(0, totalPages - MAX_DIRECTORY_LOOKBACK);

      for (int pageNumber = totalPages - 1; pageNumber >= floor; --pageNumber) {
        final Map<Integer, Entry> loaded = readDirectoryPage(pageNumber);
        if (loaded != null) {
          directory = loaded;
          return;
        }
      }

      directory = Map.of();

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot read the bloom filter directory of '%s', lookups will read every series (error=%s)", null, componentName,
          e.toString());
      directory = Map.of();
    }
  }

  /**
   * The directory a page holds, or null when the page is not one. Bit pages are indistinguishable from anything by
   * design, so the four header words plus a checksum over the entries are what tells them apart - a random page
   * mistaken for a directory would answer for pages it does not own, which is the one way this file could hide a row.
   */
  Map<Integer, Entry> readDirectoryPage(final int pageNumber) throws IOException {
    final BasePage page = database.getPageManager()
        .getImmutablePage(new PageId(database, getFileId(), pageNumber), pageSize, false, true);
    if (page == null)
      return null;

    if (page.readInt(0) != MAGIC || page.readInt(Binary.INT_SERIALIZED_SIZE) != FORMAT_VERSION)
      return null;

    final int count = page.readInt(2 * Binary.INT_SERIALIZED_SIZE);
    if (count < 0 || count > maxDirectoryEntries())
      return null;

    final List<Entry> entries = new ArrayList<>(count);
    for (int i = 0; i < count; ++i) {
      final Entry entry = readEntry(page, i);
      if (entry == null)
        return null;
      entries.add(entry);
    }

    if (page.readInt(3 * Binary.INT_SERIALIZED_SIZE) != checksumOf(entries))
      return null;

    return mapOf(entries);
  }

  /** Hash of the entries, so a bit page cannot pass for a directory and a truncated one cannot pass for a whole one. */
  private static int checksumOf(final List<Entry> entries) {
    int hash = MAGIC ^ entries.size();
    for (final Entry entry : entries) {
      hash = hash * 31 + entry.seriesRootPage();
      hash = hash * 31 + entry.seriesPages();
      hash = hash * 31 + entry.seriesFingerprint();
      hash = hash * 31 + entry.firstFilterPage();
      hash = hash * 31 + entry.filterPages();
      hash = hash * 31 + entry.slotsPerBlock();
      hash = hash * 31 + entry.probes();
      hash = hash * 31 + entry.keys();
    }
    return hash;
  }

  /**
   * TRUE when the series MAY hold the key and must be searched. The answer is TRUE for everything this file does not
   * know about, so the caller can call it unconditionally.
   *
   * @param seriesRootPage    root page of the series, its position within the compacted file
   * @param seriesPages       data pages of the series
   * @param seriesFingerprint {@code LSMTreeIndexCompacted.seriesFingerprint} of the series' last data page - a page
   *                          the reader has already read to get {@code seriesPages}, so this costs no I/O
   * @param keyHash           the key hashed by {@link #hashKey}
   */
  boolean mightContain(final int seriesRootPage, final int seriesPages, final int seriesFingerprint, final long keyHash) {
    // A directory entry names a series by WHERE it sits, and a position can be reused: an aborted compaction round
    // leaves its pages unreachable and the next round writes a different series over them. The shape and the
    // fingerprint together are what make an entry answer only for the series it was built from - a stale one would
    // filter a series by another's keys, which hides rows.
    final Entry entry = directory.get(seriesRootPage);
    if (entry == null || entry.seriesPages() != seriesPages || entry.seriesFingerprint() != seriesFingerprint)
      return true;

    try {
      final int block = blockOf(keyHash, entry.filterPages());
      // Read OUTSIDE the transaction, and allocation-free. Outside, because nothing ever modifies a filter page inside
      // a transaction - they are written only by a compaction, which runs outside one - so pulling them into the
      // caller's tracked page set would grow every write transaction's working set for nothing. Allocation-free,
      // because this runs once per series per lookup and a bulk load performs both millions of times.
      final BasePage page = database.getPageManager()
          .getImmutablePage(new PageId(database, getFileId(), entry.firstFilterPage() + block), pageSize, false, false);
      if (page == null)
        return true;

      return BufferBloomFilter.mightContainHash(page, entry.slotsPerBlock(), entry.probes(), keyHash);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Cannot probe the bloom filter of series %d in '%s' (error=%s)", null,
          seriesRootPage, componentName, e.toString());
      return true;
    }
  }

  /**
   * Writes the filter of a freshly written series and publishes it in the directory.
   * <p>
   * Never propagates a failure: the filter is an optimisation and a compaction that produced correct index pages must
   * not be failed by it. What it does NOT do on failure is publish a half-written filter.
   *
   * @param hashes the {@link #hashKey} of every key of the series, in any order, duplicates allowed
   * @param count  how many of {@code hashes} are populated
   */
  void publish(final int seriesRootPage, final int seriesPages, final int seriesFingerprint, final long[] hashes,
      final int count, final double falsePositiveRate) {
    if (count < 1)
      return;

    try {
      if (directory.size() >= maxDirectoryEntries() && !directory.containsKey(seriesRootPage)) {
        LogManager.instance().log(this, Level.INFO,
            "Bloom filter directory of '%s' is full (%d series): the new series is published without a filter", null,
            componentName, directory.size());
        return;
      }

      final int blockBits = blockBits();
      final int totalSlots = BufferBloomFilter.slotsFor(count, falsePositiveRate);
      final int filterPages = Math.max(1, (totalSlots + blockBits - 1) / blockBits);
      final int slotsPerBlock = Math.min(blockBits, roundUpToEight((totalSlots + filterPages - 1) / filterPages));
      final int probes = Math.min(MAX_PROBES,
          BufferBloomFilter.probesFor(slotsPerBlock, (count + filterPages - 1) / filterPages));

      final int firstFilterPage = getTotalPages();

      final List<MutablePage> blocks = new ArrayList<>(filterPages);
      final BufferBloomFilter[] filters = new BufferBloomFilter[filterPages];
      for (int i = 0; i < filterPages; ++i) {
        final MutablePage block = new MutablePage(new PageId(database, getFileId(), firstFilterPage + i), pageSize);
        blocks.add(block);
        filters[i] = new BufferBloomFilter(block.getTrackable(), slotsPerBlock, HASH_SEED, probes);
      }

      for (int i = 0; i < count; ++i)
        filters[blockOf(hashes[i], filterPages)].addHash(hashes[i]);

      if (!verifyNoFalseNegatives(filters, hashes, count, filterPages, seriesRootPage))
        return;

      final Entry entry = new Entry(seriesRootPage, seriesPages, seriesFingerprint, firstFilterPage, filterPages,
          slotsPerBlock, probes, count);

      for (final MutablePage block : blocks)
        database.getPageManager().updatePageVersion(block, true);
      updatePageCount(firstFilterPage + filterPages);
      database.getPageManager().writePages(blocks, false);

      // The directory goes last, and only once the bits it points at are on disk: a crash in between leaves the file
      // ending in pages no directory names, which the next load walks straight past.
      appendDirectory(withEntry(entry));

      LogManager.instance().log(this, Level.FINE,
          "Published the bloom filter of series %d in '%s' (keys=%d pages=%d probes=%d expectedFalsePositives=%.4f)", null,
          seriesRootPage, componentName, count, filterPages, probes,
          filters[0].expectedFalsePositiveRate((count + filterPages - 1) / filterPages));

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot write the bloom filter of series %d in '%s', lookups will read the series (error=%s)", null, seriesRootPage,
          componentName, e.toString());
    }
  }

  /**
   * Drops every filter of a series at or after {@code seriesRootPageFloor}.
   * <p>
   * Called when a compaction round is rolled back (#4946): its series pages stay on disk but become unreachable and
   * the NEXT round writes its own series over the same page numbers. A directory entry left behind would then describe
   * a series that no longer exists at that root page - and the new series living there would be filtered by the OLD
   * series' keys, which is a false negative on everything the new series added.
   * <p>
   * The dropped filters' bit pages stay where they are and simply stop being named by any directory. The file only
   * ever grows, which is what lets a crash - and an HA follower, which receives a compaction as the page range each
   * file grew by - always see a directory that agrees with the bits around it.
   */
  void rollbackFrom(final int seriesRootPageFloor) {
    final List<Entry> survivors = new ArrayList<>(directory.size());
    for (final Entry entry : directory.values())
      if (entry.seriesRootPage() < seriesRootPageFloor)
        survivors.add(entry);

    if (survivors.size() == directory.size())
      return;

    // Stop using the dropped filters BEFORE trying to persist the fact. Readers consult the in-RAM directory, and if
    // the page write below fails, having lost a filter costs a page read while having kept a stale one costs rows.
    directory = mapOf(survivors);

    try {
      appendDirectory(survivors);
    } catch (final Exception e) {
      // The stale entry survives on disk until the next publish appends a directory built from the (already corrected)
      // in-RAM one. Until then a reopened database would see it again - and the entry is only ever consulted for a
      // series that starts at the same root page AND spans the same number of pages, so it takes that coincidence on
      // top of two consecutive write failures and a restart in between to matter.
      LogManager.instance().log(this, Level.WARNING,
          "Cannot persist the rolled-back bloom filter directory of '%s'; the filters are already out of use in this "
              + "process, and the next compaction rewrites the directory (error=%s)", null, componentName, e.toString());
    }
  }

  private static Map<Integer, Entry> mapOf(final List<Entry> entries) {
    final Map<Integer, Entry> map = new HashMap<>(Math.max(1, entries.size() * 2));
    for (final Entry entry : entries)
      map.put(entry.seriesRootPage(), entry);
    return Map.copyOf(map);
  }

  /** Number of series that currently carry a filter. */
  public int getPublishedFilters() {
    return directory.size();
  }

  /**
   * Hash of a serialized index key. The bytes MUST be the ones {@code writeKeys} produces, on both the compaction and
   * the lookup path, or the two sides agree on nothing.
   */
  static long hashKey(final Binary serializedKey) {
    // The hash reads the backing array from index 0, so a buffer that is a VIEW into a larger one would hash the wrong
    // bytes - and the compaction and the lookup would then disagree about where a key lives. Scratch buffers are always
    // standalone; the check keeps that from being an assumption nobody can see.
    if (serializedKey.getContentBeginOffset() != 0)
      throw new IllegalArgumentException("A bloom key must be serialized into a standalone buffer");

    return MurmurHash.hash64(serializedKey.getContent(), serializedKey.size(), HASH_SEED);
  }

  /**
   * Probes back every key just added. A bloom filter that answers "no" to a key it holds is worse than no filter at
   * all, and a bug that produces one - a routing/probing mismatch, a slot count the buffer cannot address, a block
   * index computed from bits the probes also use - would otherwise surface as rows silently missing from lookups. The
   * check is O(keys) over data already in RAM and runs once per series, against a compaction that just read and
   * rewrote every one of those keys.
   */
  private boolean verifyNoFalseNegatives(final BufferBloomFilter[] filters, final long[] hashes, final int count,
      final int filterPages, final int seriesRootPage) {
    for (int i = 0; i < count; ++i)
      if (!filters[blockOf(hashes[i], filterPages)].mightContainHash(hashes[i])) {
        LogManager.instance().log(this, Level.SEVERE,
            "Bloom filter of series %d in '%s' does not report back a key it was given: discarding it (this is a bug, "
                + "please report it - the index itself is intact and lookups are unaffected)", null, seriesRootPage,
            componentName);
        return false;
      }
    return true;
  }

  /** The directory as it would be with {@code entry} added, replacing any entry of the same series. */
  private List<Entry> withEntry(final Entry entry) {
    final List<Entry> entries = new ArrayList<>(directory.size() + 1);
    for (final Entry existing : directory.values())
      if (existing.seriesRootPage() != entry.seriesRootPage())
        entries.add(existing);
    entries.add(entry);
    return entries;
  }

  /**
   * Appends a new directory page listing {@code entries} and swaps the in-RAM directory. Readers consult the in-RAM
   * copy, which is replaced only after the page is on disk, so a reader either sees the previous directory or the new
   * one - and a reopened database finds the same thing on the last page of the file.
   */
  private void appendDirectory(final List<Entry> entries) throws IOException, InterruptedException {
    final int pageNumber = getTotalPages();
    final MutablePage page = new MutablePage(new PageId(database, getFileId(), pageNumber), pageSize);

    page.writeInt(0, MAGIC);
    page.writeInt(Binary.INT_SERIALIZED_SIZE, FORMAT_VERSION);
    page.writeInt(2 * Binary.INT_SERIALIZED_SIZE, entries.size());
    page.writeInt(3 * Binary.INT_SERIALIZED_SIZE, checksumOf(entries));

    for (int i = 0; i < entries.size(); ++i) {
      final Entry entry = entries.get(i);
      int pos = DIRECTORY_HEADER_SIZE + i * DIRECTORY_ENTRY_SIZE;
      pos += page.writeInt(pos, entry.seriesRootPage());
      pos += page.writeInt(pos, entry.seriesPages());
      pos += page.writeInt(pos, entry.seriesFingerprint());
      pos += page.writeInt(pos, entry.firstFilterPage());
      pos += page.writeInt(pos, entry.filterPages());
      pos += page.writeInt(pos, entry.slotsPerBlock());
      pos += page.writeInt(pos, entry.probes());
      page.writeInt(pos, entry.keys());
    }

    database.getPageManager().updatePageVersion(page, true);
    updatePageCount(pageNumber + 1);
    database.getPageManager().writePages(List.of(page), false);

    directory = mapOf(entries);
  }

  private Entry readEntry(final BasePage page, final int index) {
    int pos = DIRECTORY_HEADER_SIZE + index * DIRECTORY_ENTRY_SIZE;
    final int seriesRootPage = page.readInt(pos);
    final int seriesPages = page.readInt(pos += Binary.INT_SERIALIZED_SIZE);
    final int seriesFingerprint = page.readInt(pos += Binary.INT_SERIALIZED_SIZE);
    final int firstFilterPage = page.readInt(pos += Binary.INT_SERIALIZED_SIZE);
    final int filterPages = page.readInt(pos += Binary.INT_SERIALIZED_SIZE);
    final int slotsPerBlock = page.readInt(pos += Binary.INT_SERIALIZED_SIZE);
    final int probes = page.readInt(pos += Binary.INT_SERIALIZED_SIZE);
    final int keys = page.readInt(pos + Binary.INT_SERIALIZED_SIZE);

    // A malformed entry must not become a filter: it would answer for pages it does not own.
    if (seriesRootPage < 0 || seriesPages < 1 || firstFilterPage < 0 || filterPages < 1 || probes < 1
        || slotsPerBlock < 8 || slotsPerBlock % 8 != 0 || slotsPerBlock > blockBits()
        || firstFilterPage + filterPages > getTotalPages())
      return null;

    return new Entry(seriesRootPage, seriesPages, seriesFingerprint, firstFilterPage, filterPages, slotsPerBlock,
        probes, keys);
  }

  /**
   * Block a key belongs to. {@link BufferBloomFilter} derives its probes from BOTH halves of the hash, so there are no
   * spare bits to route with: passing the hash through the splitmix64 finaliser - which mixes every input bit into
   * every output bit - gives a value independent of the two halves the probes use, for the cost of three multiplies.
   * Routing on raw bits of the same hash would tie which block a key lands in to where it probes inside that block,
   * and a filter is only as good as the independence of the bits it stands on.
   */
  private static int blockOf(final long hash, final int filterPages) {
    if (filterPages == 1)
      return 0;

    long mixed = hash;
    mixed ^= mixed >>> 30;
    mixed *= 0xbf58476d1ce4e5b9L;
    mixed ^= mixed >>> 27;
    mixed *= 0x94d049bb133111ebL;
    mixed ^= mixed >>> 31;

    return (int) Long.remainderUnsigned(mixed, filterPages);
  }

  /** Bits a full page can hold, i.e. the size of one block. */
  private int blockBits() {
    return (pageSize - BasePage.PAGE_HEADER_SIZE) * 8;
  }

  private int maxDirectoryEntries() {
    return (pageSize - BasePage.PAGE_HEADER_SIZE - DIRECTORY_HEADER_SIZE) / DIRECTORY_ENTRY_SIZE;
  }

  private static int roundUpToEight(final int slots) {
    return Math.max(8, (slots + 7) / 8 * 8);
  }

  /**
   * Creates the filter file of a compacted index, or opens the one it already has.
   */
  static LSMTreeIndexBloomFilter createOrLoad(final LSMTreeIndexCompacted compacted) throws IOException {
    return createOrLoad(compacted.getDatabase(), compacted.getName() + NAME_SUFFIX, PAGE_SIZE);
  }

  static LSMTreeIndexBloomFilter createOrLoad(final DatabaseInternal database, final String name, final int pageSize)
      throws IOException {
    final LSMTreeIndexBloomFilter filter = new LSMTreeIndexBloomFilter(database, name,
        database.getDatabasePath() + File.separator + name, pageSize);
    database.getSchema().getEmbedded().registerFile(filter);
    filter.loadDirectory();
    return filter;
  }

  @Override
  public String toString() {
    return componentName;
  }

  /** Drops the file, quietly: losing a filter costs performance, never correctness. */
  public void dropQuietly() {
    try {
      if (database.isOpen()) {
        database.getPageManager().deleteFile(database, getFileId());
        database.getFileManager().dropFile(getFileId());
        database.getSchema().getEmbedded().removeFile(getFileId());
      } else if (!getOSFile().delete())
        LogManager.instance().log(this, Level.FINE, "Cannot delete the bloom filter file '%s'", null, getOSFile());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on dropping the bloom filter file '%s' (error=%s)", null,
          componentName, e.toString());
    }
  }
}
