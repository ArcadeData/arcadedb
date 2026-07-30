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
package com.arcadedb.engine.timeseries;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * Append-only string dictionary shared by every shard of one TimeSeries type, so a TAG column
 * occupies a fixed 4-byte id in the mutable row instead of a reserved
 * {@code 2 + MAX_STRING_BYTES} slot (issue #5519).
 * <p>
 * The reservation was the whole defect: the writer stored a tag packed but the stride assumed the
 * maximum, so a ten-tag row cost 2612 bytes of stride to carry ~110 bytes of payload, fitting 25
 * rows in a 64 KB page and amplifying page and WAL traffic 24x. A tag is low-cardinality by
 * definition, which is exactly what a dictionary exploits.
 * <p>
 * <b>Scope is the type, not the shard nor the database.</b> Per type is what makes the id space
 * meaningful: every shard sees the same declared tag set, dropping the type drops the dictionary
 * (which is the whole reclamation story), and cardinality is bounded per type rather than shared
 * with every other type in the database. The database-wide {@code com.arcadedb.engine.Dictionary}
 * cannot serve this role: it is a single 320 KB page holding every property and type name, it grows
 * a {@code CopyOnWriteArrayList} one O(n) copy at a time, and it commits a nested transaction per
 * new value under a global monitor.
 * <p>
 * <b>Ids.</b> Id 0 is reserved and virtual: it denotes {@code null}/{@code ""}, consumes no stored
 * entry, and preserves the pre-existing round-trip where a null tag reads back as {@code ""}. Stored
 * entries take ids 1..N in insertion order. Ids are 4 bytes, so there is no overflow case to handle
 * and no fallback path; the 2-byte id a per-block sealed dictionary uses would have bought ~1250
 * rows per page instead of ~900, which is not worth an overflow story.
 * <p>
 * <b>Transactions.</b> New values are interned in one nested transaction per append batch, taken
 * before the data transaction begins and serialized across shards by {@link #internLock}. Committing
 * separately means the data pages never conflict with the dictionary tail page, and the in-RAM
 * mapping is published only after that commit succeeds, so a failed commit leaves RAM and disk
 * agreeing. The converse - a committed id whose data transaction then rolls back - leaves an unused
 * entry, which is harmless because the dictionary is append-only and ids are never reused.
 * <p>
 * <b>Steady state is free.</b> After warm-up every value is already known, so an append does a
 * lookup pass and no transaction at all.
 * <p>
 * Header page (page 0) layout (offsets from PAGE_HEADER_SIZE) - 13 bytes:
 * - [0..3]   magic "TSTD" (4 bytes)
 * - [4]      formatVersion (1 byte)
 * - [5..8]   stored entry count (int)
 * - [9..12]  data page count (int)
 * <p>
 * Data page layout (offsets from PAGE_HEADER_SIZE):
 * - [0..3]   entry count in page (int)
 * - [4..7]   bytes used after the page header (int)
 * - [8..]    entries: 2-byte length prefix + UTF-8 payload, never straddling a page boundary
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesTagDictionary extends PaginatedComponent {

  public static final String DICT_EXT        = "tstd";
  public static final int    CURRENT_VERSION = 0;

  /**
   * Id of {@code null} and of the empty string. Virtual: it is never stored.
   */
  public static final int EMPTY_ID = 0;

  /**
   * Returned by {@link #getId(String)} for a value this dictionary has never interned.
   */
  public static final int NO_ID = -1;

  /**
   * Suffix appended to the TimeSeries type name to name its dictionary component.
   */
  public static final String NAME_SUFFIX = "_tags";

  private static final int MAGIC_VALUE = 0x54535444; // "TSTD"

  // Header page offsets (from PAGE_HEADER_SIZE)
  private static final int HEADER_MAGIC_OFFSET          = 0;
  private static final int HEADER_FORMAT_VERSION_OFFSET = 4;
  private static final int HEADER_ENTRY_COUNT_OFFSET    = 5;
  private static final int HEADER_DATA_PAGE_COUNT       = 9;

  // Data page offsets (from PAGE_HEADER_SIZE)
  private static final int DATA_ENTRY_COUNT_OFFSET = 0;
  private static final int DATA_USED_BYTES_OFFSET  = 4;
  private static final int DATA_ENTRIES_OFFSET     = 8;

  // Id -> value. Replaced wholesale on growth and volatile-published after the entries are written,
  // so a reader that resolves an id read off a committed data page always sees the value for it.
  private volatile String[] byId = { "" };
  // Value -> id. Concurrent because lookups run on every ingest thread while interning holds the lock,
  // and volatile because a reload replaces it wholesale instead of clearing it under those lookups.
  private volatile ConcurrentHashMap<String, Integer> idByValue = new ConcurrentHashMap<>();

  private volatile int     entryCount;
  private volatile int     dataPageCount;
  private          int     maxSize;
  // Whether the in-RAM mapping has been rebuilt from the pages. A dictionary created in this session
  // starts loaded; one the component factory rebuilt at cold open does not.
  private volatile boolean loaded;

  // Serializes id assignment across the shards of this type. Contended only while a batch carries
  // values never seen before, which is the warm-up phase and nothing after it.
  private final Lock internLock = new ReentrantLock();

  /**
   * Factory handler for loading an existing .tstd file during schema load. The in-RAM mapping is
   * rebuilt later by {@link #load()}, once the component is registered and the file id resolves.
   */
  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new TimeSeriesTagDictionary(database, name, filePath, id);
    }
  }

  /**
   * Creates a new tag dictionary. As with {@link TimeSeriesBucket}, the header page is NOT written
   * here: {@link #initHeaderPage()} must be called after the component is registered with the
   * schema, so the commit can resolve the file by its id.
   */
  public TimeSeriesTagDictionary(final DatabaseInternal database, final String name, final String filePath)
      throws IOException {
    super(database, name, filePath, DICT_EXT, ComponentFile.MODE.READ_WRITE,
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE), CURRENT_VERSION);
    this.maxSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TIMESERIES_TAG_DICTIONARY_MAX_SIZE);
  }

  /**
   * Opens an existing tag dictionary.
   */
  public TimeSeriesTagDictionary(final DatabaseInternal database, final String name, final String filePath, final int id)
      throws IOException {
    super(database, name, filePath, id, ComponentFile.MODE.READ_WRITE,
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE), CURRENT_VERSION);
    this.maxSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TIMESERIES_TAG_DICTIONARY_MAX_SIZE);
  }

  /**
   * Returns the dictionary shared by every shard of a TimeSeries type, creating and registering it on
   * first use. The single instance comes from the schema, which is what makes it per-type rather than
   * per-shard: the first shard creates it, the rest find it, and a cold open finds the one the
   * component factory rebuilt.
   *
   * @param mutableFormatVersion the type's stored mutable row format version
   *
   * @return the dictionary, or {@code null} when the type has no STRING TAG column to encode or was
   * written by a build that stored tags inline
   */
  public static TimeSeriesTagDictionary openOrCreate(final DatabaseInternal database, final String typeName,
      final List<ColumnDefinition> columns, final int mutableFormatVersion) throws IOException {
    if (mutableFormatVersion < TimeSeriesBucket.VERSION_DICTIONARY_TAGS || !hasDictionaryColumns(columns))
      return null;

    final String name = typeName + NAME_SUFFIX;
    final LocalSchema schema = (LocalSchema) database.getSchema();

    final Component existing = schema.getFileByName(name);
    if (existing instanceof TimeSeriesTagDictionary dictionary) {
      dictionary.ensureLoaded();
      return dictionary;
    }

    final TimeSeriesTagDictionary dictionary =
        new TimeSeriesTagDictionary(database, name, database.getDatabasePath() + "/" + name);
    schema.registerFile(dictionary);

    if (dictionary.getTotalPages() > 0)
      // The component was absent from the schema but its file is on disk. Adopt what is there:
      // writing a fresh header would silently reset the id space and orphan every id already stored
      // in a data page.
      dictionary.load();
    else {
      dictionary.initHeaderPage();
      dictionary.loaded = true;
    }
    return dictionary;
  }

  /**
   * Whether any column of the type would be dictionary-encoded. A type with no STRING TAG has nothing
   * to intern, so it gets no dictionary file at all.
   */
  public static boolean hasDictionaryColumns(final List<ColumnDefinition> columns) {
    for (final ColumnDefinition col : columns)
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG && col.getDataType() == Type.STRING)
        return true;
    return false;
  }

  /**
   * Writes the header page in a self-contained nested transaction, mirroring
   * {@link TimeSeriesBucket#initHeaderPage()}. Routed through the wrapped database so that, under
   * HA, the page is shipped to followers together with the file-creation entry of the enclosing DDL.
   */
  public void initHeaderPage() throws IOException {
    final DatabaseInternal db = database.getWrappedDatabaseInstance();
    db.begin();
    // Only this method's own nested transaction may be rolled back below: if begin() had failed there
    // would be no nested transaction, and rolling back "the active one" would discard the caller's.
    boolean ownTransaction = true;
    try {
      final MutablePage headerPage = db.getTransaction().addPage(new PageId(database, fileId, 0), pageSize);
      headerPage.writeInt(HEADER_MAGIC_OFFSET, MAGIC_VALUE);
      headerPage.writeByte(HEADER_FORMAT_VERSION_OFFSET, (byte) CURRENT_VERSION);
      headerPage.writeInt(HEADER_ENTRY_COUNT_OFFSET, 0);
      headerPage.writeInt(HEADER_DATA_PAGE_COUNT, 0);
      pageCount.set(1);
      db.commit();
      ownTransaction = false;
    } catch (final Exception e) {
      throw e instanceof IOException io ? io : new IOException("Failed to initialise TimeSeries tag dictionary header", e);
    } finally {
      if (ownTransaction && db.isTransactionActive())
        db.rollback();
    }
  }

  /**
   * Rebuilds the in-RAM mapping from the pages, in a self-contained read transaction. Called once
   * per open, after the component is registered with the schema.
   */
  public synchronized void ensureLoaded() throws IOException {
    if (loaded)
      return;
    load();
  }

  public synchronized void load() throws IOException {
    if (getTotalPages() == 0) {
      loaded = true;
      return;
    }

    // A read transaction of our own, so this can be called from inside a scan. It must be the only one
    // rolled back at the end: load() is reachable from resolveMiss() mid-query, and discarding the
    // caller's transaction there would abort the very scan that asked for the value.
    database.begin();
    try {
      final TransactionContext tx = database.getTransaction();
      final BasePage headerPage = tx.getPage(new PageId(database, fileId, 0), pageSize);
      final int storedEntries = headerPage.readInt(HEADER_ENTRY_COUNT_OFFSET);
      final int pages = headerPage.readInt(HEADER_DATA_PAGE_COUNT);

      final String[] values = new String[storedEntries + 1];
      values[EMPTY_ID] = "";

      // Built aside and swapped in below rather than rebuilt in place: a lookup runs lock-free and
      // concurrently with this, and clearing the live map would make it report a stored value as
      // absent - which on the ingest path means interning a second id for a value that already has one.
      final ConcurrentHashMap<String, Integer> rebuilt = new ConcurrentHashMap<>();
      int id = 1;
      for (int pageNum = 1; pageNum <= pages; pageNum++) {
        final BasePage page = tx.getPage(new PageId(database, fileId, pageNum), pageSize);
        final int pageEntries = page.readInt(DATA_ENTRY_COUNT_OFFSET);
        int offset = DATA_ENTRIES_OFFSET;
        for (int i = 0; i < pageEntries; i++) {
          final int len = page.readShort(offset) & 0xFFFF;
          final byte[] bytes = new byte[len];
          if (len > 0)
            page.readByteArray(offset + 2, bytes);
          final String value = new String(bytes, StandardCharsets.UTF_8);
          values[id] = value;
          rebuilt.putIfAbsent(value, id);
          id++;
          offset += 2 + len;
        }
      }

      // Published only now, complete: same order as publish(), reverse array before forward map, so a
      // thread that finds a value in the map can always resolve the id it got back. A load that throws
      // before this point leaves the previous mapping untouched, and `loaded` false so it is retried.
      this.byId = values;
      this.idByValue = rebuilt;
      this.entryCount = storedEntries;
      this.dataPageCount = pages;
      this.loaded = true;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  /**
   * Returns the id of an already-interned value, {@link #EMPTY_ID} for {@code null} or {@code ""},
   * or {@link #NO_ID} when the value has never been interned. Allocation-free and lock-free.
   */
  public int getId(final String value) {
    if (value == null || value.isEmpty())
      return EMPTY_ID;
    final Integer id = idByValue.get(value);
    return id != null ? id : NO_ID;
  }

  /**
   * Resolves an id back to its value. Returns the shared {@link String} instance, so a scan over
   * millions of rows no longer allocates one string per tag per row as the inline encoding did.
   */
  public String getById(final int id) {
    final String[] values = byId;
    if (id >= 0 && id < values.length && values[id] != null)
      return values[id];
    return resolveMiss(id);
  }

  /**
   * Rebuilds from the pages when an id is read that this instance has not interned itself.
   * <p>
   * The case that matters is an HA follower: it receives the dictionary pages through the Raft WAL and
   * applies them to storage, but {@link #internAll} - the only thing that populates the in-RAM mapping
   * - runs on the leader. Without this the follower would hand back {@code null} for every tag written
   * since it opened. Reloading on the miss makes the map self-healing wherever pages can arrive from
   * outside this instance.
   */
  private synchronized String resolveMiss(final int id) {
    // Another thread may have reloaded while this one waited on the monitor.
    String[] values = byId;
    if (id >= 0 && id < values.length && values[id] != null)
      return values[id];

    // An id inside what is already loaded is not a staleness miss, and reloading cannot conjure it.
    if (id < 0 || id <= entryCount)
      return null;

    // Reload only when the pages really do hold the id, which is what tells a stale map apart from a
    // corrupt id: without this a scan carrying a corrupt id would re-read every page on every row.
    // The test is against the count stored in the header - one already-cached page read - and not
    // against the last reload, because a leader interns for as long as it ingests: keying on the
    // reload would self-heal for the first wave of ids and then never again.
    if (id > peekStoredEntryCount())
      return null;

    try {
      load();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error reloading TimeSeries tag dictionary '%s' while resolving id %d: %s", e, getName(), id, e.getMessage());
      return null;
    }

    values = byId;
    return id < values.length ? values[id] : null;
  }

  /**
   * Entry count as stored in the header page, which is what an id arriving from outside this instance
   * has to be measured against. Read in a transaction of its own, like {@link #load()}, since this is
   * reachable from inside a scan. A header that cannot be read degrades to the in-RAM count, so the
   * caller declines to reload rather than reloading once per row.
   */
  private int peekStoredEntryCount() {
    if (getTotalPages() == 0)
      return entryCount;

    database.begin();
    try {
      return database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize)
          .readInt(HEADER_ENTRY_COUNT_OFFSET);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error reading the header of TimeSeries tag dictionary '%s': %s", e, getName(), e.getMessage());
      return entryCount;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  /**
   * Interns a single value and returns its id, committing if the value is new.
   */
  public int intern(final String value) throws IOException {
    final int existing = getId(value);
    if (existing != NO_ID)
      return existing;
    internAll(Collections.singletonList(value));
    return getId(value);
  }

  /**
   * Assigns an id to every value in {@code values} that does not already have one, in a single
   * nested transaction. Values already known, {@code null} and {@code ""} are skipped, so calling
   * this on an entire batch costs nothing once the dictionary is warm.
   */
  public void internAll(final Collection<String> values) throws IOException {
    if (values.isEmpty())
      return;

    internLock.lock();
    try {
      // Re-check under the lock: another shard may have interned these while we were queued.
      final List<String> toAdd = new ArrayList<>();
      final Set<String> pending = new HashSet<>();
      for (final String value : values) {
        if (value == null || value.isEmpty() || idByValue.containsKey(value))
          continue;
        if (pending.add(value))
          toAdd.add(value);
      }
      if (toAdd.isEmpty())
        return;

      if (entryCount + toAdd.size() > maxSize)
        throw new IllegalStateException(
            "TimeSeries tag dictionary '" + getName() + "' would exceed the maximum of " + maxSize
                + " distinct values (current=" + entryCount + ", adding=" + toAdd.size()
                + "). A TAG column is meant to be low-cardinality; move a high-cardinality value to a FIELD, or raise "
                + GlobalConfiguration.TIMESERIES_TAG_DICTIONARY_MAX_SIZE.getKey());

      // Encoded once here and carried to the pages: a value is validated and written in the same bytes.
      final List<byte[]> encoded = new ArrayList<>(toAdd.size());
      for (final String value : toAdd) {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > TimeSeriesBucket.MAX_STRING_BYTES)
          throw new IllegalArgumentException(
              "Tag value exceeds max length of " + TimeSeriesBucket.MAX_STRING_BYTES + " bytes for dictionary '"
                  + getName() + "'");
        encoded.add(bytes);
      }

      // Route through the wrapped database so the pages are shipped to followers under HA.
      final DatabaseInternal db = database.getWrappedDatabaseInstance();
      db.begin();
      // See initHeaderPage(): roll back only our own nested transaction, never a caller's. This one
      // usually IS nested - the ingest path calls it from inside the append transaction.
      boolean ownTransaction = true;
      final int[] appended;
      try {
        appended = appendEntries(db.getTransaction(), encoded);
        db.commit();
        ownTransaction = false;
      } catch (final Exception e) {
        throw e instanceof IOException io ? io :
            new IOException("Failed to append to TimeSeries tag dictionary '" + getName() + "'", e);
      } finally {
        if (ownTransaction && db.isTransactionActive())
          db.rollback();
      }

      // Publish to RAM only now: before the commit, disk is the single source of truth, so a failed
      // commit leaves nothing to undo.
      publish(appended[0], appended[1], toAdd);
    } finally {
      internLock.unlock();
    }
  }

  /**
   * Number of stored entries, excluding the virtual empty-string id.
   */
  public int size() {
    return entryCount;
  }

  /**
   * Number of allocated data pages, excluding the header page.
   */
  public int getDataPageCount() {
    return dataPageCount;
  }

  /**
   * Overrides the configured cardinality cap for this dictionary.
   */
  public void setMaxSize(final int maxSize) {
    this.maxSize = maxSize;
  }

  // --- Private helpers ---

  /**
   * Appends the already UTF-8 encoded values to the tail data page, spilling onto new pages so an
   * entry is never split. Returns {@code { id assigned to the first value, resulting data page count }}.
   */
  private int[] appendEntries(final TransactionContext tx, final List<byte[]> toAdd) throws IOException {
    final MutablePage headerPage = tx.getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    int storedEntries = headerPage.readInt(HEADER_ENTRY_COUNT_OFFSET);
    int pages = headerPage.readInt(HEADER_DATA_PAGE_COUNT);
    final int firstId = storedEntries + 1;

    final int capacity = pageSize - BasePage.PAGE_HEADER_SIZE - DATA_ENTRIES_OFFSET;

    MutablePage page = null;
    int pageEntries = 0;
    int pageUsed = 0;
    if (pages > 0) {
      page = tx.getPageToModify(new PageId(database, fileId, pages), pageSize, false);
      pageEntries = page.readInt(DATA_ENTRY_COUNT_OFFSET);
      pageUsed = page.readInt(DATA_USED_BYTES_OFFSET);
    }

    for (final byte[] bytes : toAdd) {
      final int needed = 2 + bytes.length;

      // MAX_STRING_BYTES caps an entry at 258 bytes, so this can only fire on a configured page size
      // far too small to host one. Checked rather than assumed: without it the write below would run
      // off the end of a freshly allocated page.
      if (needed > capacity)
        throw new IllegalArgumentException(
            "A tag value of " + bytes.length + " bytes does not fit a dictionary page of " + pageSize
                + " bytes (capacity " + capacity + ") for '" + getName() + "'. Raise "
                + GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getKey());

      if (page == null || pageUsed + needed > capacity) {
        if (page != null)
          flushDataPageHeader(page, pageEntries, pageUsed);
        page = allocateDataPage(tx, ++pages);
        pageEntries = 0;
        pageUsed = 0;
      }

      page.writeShort(DATA_ENTRIES_OFFSET + pageUsed, (short) bytes.length);
      if (bytes.length > 0)
        page.writeByteArray(DATA_ENTRIES_OFFSET + pageUsed + 2, bytes);
      pageUsed += needed;
      pageEntries++;
      storedEntries++;
    }

    flushDataPageHeader(page, pageEntries, pageUsed);
    headerPage.writeInt(HEADER_ENTRY_COUNT_OFFSET, storedEntries);
    headerPage.writeInt(HEADER_DATA_PAGE_COUNT, pages);
    return new int[] { firstId, pages };
  }

  private MutablePage allocateDataPage(final TransactionContext tx, final int pageNum) throws IOException {
    final MutablePage page;
    if (pageNum < getTotalPages())
      page = tx.getPageToModify(new PageId(database, fileId, pageNum), pageSize, false);
    else {
      page = tx.addPage(new PageId(database, fileId, pageNum), pageSize);
      pageCount.incrementAndGet();
    }
    page.writeInt(DATA_ENTRY_COUNT_OFFSET, 0);
    page.writeInt(DATA_USED_BYTES_OFFSET, 0);
    return page;
  }

  private static void flushDataPageHeader(final MutablePage page, final int entries, final int used) {
    page.writeInt(DATA_ENTRY_COUNT_OFFSET, entries);
    page.writeInt(DATA_USED_BYTES_OFFSET, used);
  }

  /**
   * Publishes the committed entries to the in-RAM mapping.
   * <p>
   * The reverse array is grown by doubling and the new reference is volatile-published <em>before</em>
   * the forward map is populated, so a thread that finds a value in the map can always resolve the id
   * it got back.
   * <p>
   * Synchronized on the same monitor as {@link #load()} so a reload cannot swap in a mapping rebuilt
   * from the pages while this is publishing into the one it replaces, which would drop these entries
   * from RAM and let the next batch intern a second id for a value that already has one. Reached only
   * when a batch carries a value never seen before, so the extra ordering costs nothing in steady state.
   * Callers hold {@code internLock} first, and nothing takes that lock while holding this monitor.
   */
  private synchronized void publish(final int firstId, final int pages, final List<String> toAdd) {
    final int required = firstId + toAdd.size();
    String[] values = byId;
    if (required > values.length) {
      int newLength = Math.max(values.length * 2, 16);
      while (newLength < required)
        newLength *= 2;
      final String[] grown = new String[newLength];
      System.arraycopy(values, 0, grown, 0, values.length);
      values = grown;
    }

    int id = firstId;
    for (final String value : toAdd)
      values[id++] = value;

    this.byId = values;
    this.entryCount = firstId - 1 + toAdd.size();

    id = firstId;
    for (final String value : toAdd)
      idByValue.putIfAbsent(value, id++);

    this.dataPageCount = pages;
  }
}
