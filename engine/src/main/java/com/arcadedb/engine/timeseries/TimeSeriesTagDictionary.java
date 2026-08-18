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
import com.arcadedb.engine.PaginatedComponentFile;
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
   * <p>
   * The page size, version and mode {@code ComponentFactory} recovered from the file name are passed straight
   * through (issue #6314); see {@link TimeSeriesBucket.PaginatedComponentFactoryHandler} for why re-deriving the
   * page size from the live configuration instead is a silent misread rather than an exception.
   */
  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new TimeSeriesTagDictionary(database, name, filePath, id, mode, pageSize, version);
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
   * Opens an existing tag dictionary on the id, page size and version its own file name carries (issue #6314).
   */
  public TimeSeriesTagDictionary(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    this.maxSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TIMESERIES_TAG_DICTIONARY_MAX_SIZE);
  }

  /**
   * Opens a second view over a file that is ALREADY registered with the file manager, taking the id, the page size,
   * the version AND the mode from that file rather than re-deriving any of them (issues #6314 and #6340). Every one
   * of the four is a property of the file and of nothing else, so this is the only form a caller in that position
   * should need - and the mode was the last one still guessed here, hard-coded to {@code READ_WRITE} in the middle
   * of three values that were read off the file, because {@code ComponentFile} had no accessor for it.
   */
  public TimeSeriesTagDictionary(final DatabaseInternal database, final String name, final PaginatedComponentFile file)
      throws IOException {
    this(database, name, file.getFilePath(), file.getFileId(), file.getMode(), file.getPageSize(), file.getVersion());
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

    // A file already registered under this component name is what this dictionary must be BUILT ON, id, page size
    // and version and all. The id-allocating constructor would take a fresh file id from the manager and then be
    // handed this very file anyway - getOrCreateFile() is keyed by the component name - leaving the component
    // addressing pages of an id that is not the file it holds, which is unusable in either direction. The page
    // size has to come from the same place for the same reason (issue #6314): the live bucketDefaultPageSize is
    // whatever this run was configured with, not what the file on disk was written at.
    final ComponentFile existingFile = database.getFileManager().getFileByComponentName(name);

    // "Registered but not paginated" is unreachable today - PaginatedComponentFile is the only ComponentFile the
    // engine ever constructs - and the branch below is written so it STAYS that way loudly. Falling through to the
    // create-fresh arm on a registered name would be the silent outcome: it takes a new file id, gets handed this
    // very file back by name, and dies on the #6283 id guard several frames later with a message about ids that
    // says nothing about the real cause. This whole change is about turning that class of mismatch into a
    // statement, so the assumption is one here too rather than an instanceof quietly deciding it.
    if (existingFile != null && !(existingFile instanceof PaginatedComponentFile))
      throw new IllegalStateException(
          "The file registered under component name '" + name + "' is a " + existingFile.getClass().getSimpleName()
              + " ('" + existingFile.getFilePath() + "'), but a tag dictionary can only be built on a paginated file");

    final TimeSeriesTagDictionary dictionary = existingFile != null ?
        new TimeSeriesTagDictionary(database, name, (PaginatedComponentFile) existingFile) :
        new TimeSeriesTagDictionary(database, name, database.getDatabasePath() + "/" + name);

    // Registered BEFORE it is initialised, and it has to be: initHeaderPage() commits, and a commit
    // resolves the component to bump its page count through schema.getFileById(), which throws on an id
    // the schema does not know. The half-built component is not exposed by that: openOrCreate() runs from
    // the TimeSeriesEngine constructor, under the schema DDL that creates or opens the type, before a
    // single shard of it exists - so nothing can hold this type's dictionary id yet, and the only other
    // caller is a test-only TimeSeriesShard constructor.
    schema.registerFile(dictionary);

    final StoredHeader header = dictionary.readStoredHeader();
    if (header != null)
      // The component was absent from the schema but its file already carries an initialised header.
      // Adopt what is there: writing a fresh header would silently reset the id space and orphan every
      // id already stored in a data page. The test is on the header itself and not on getTotalPages(),
      // which under-reports for as long as the pages sit in the flush queue (issue #6198).
      dictionary.load(header);
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
    load(readStoredHeader());
  }

  /**
   * Rebuilds from a header already read, so a caller that had to read it to decide whether to reload -
   * {@link #resolveMiss} - does not pay for a second lookup. Passing the checked header also ties the
   * rebuild to it: the walk is guaranteed to cover the id that header was measured against.
   *
   * @param header the header page contents, or {@code null} when the file carries none
   */
  private synchronized void load(final StoredHeader header) throws IOException {
    if (header == null) {
      // No initialised header, so nothing has ever been stored in this file
      loaded = true;
      return;
    }

    // Header first (see readStoredHeader()), pages after, in two steps rather than one. What makes that
    // safe is that the format is strictly append-only: the counts are a lower bound that an append
    // landing in between can only raise, and the bytes of the entries they cover never change. So the
    // walk below rebuilds a valid prefix, and the entries it missed are picked up by the next reload.
    final int storedEntries = header.entryCount();
    final int pages = header.dataPageCount();

    // A read transaction of our own, so this can be called from inside a scan. It must be the only one
    // rolled back at the end: load() is reachable from resolveMiss() mid-query, and discarding the
    // caller's transaction there would abort the very scan that asked for the value.
    database.begin();
    try {
      final TransactionContext tx = database.getTransaction();

      final String[] values = new String[storedEntries + 1];
      values[EMPTY_ID] = "";

      // Built aside and swapped in below rather than rebuilt in place: a lookup runs lock-free and
      // concurrently with this, and clearing the live map would make it report a stored value as
      // absent - which on the ingest path means interning a second id for a value that already has one.
      final ConcurrentHashMap<String, Integer> rebuilt = new ConcurrentHashMap<>();
      int id = 1;
      for (int pageNum = 1; pageNum <= pages && id <= storedEntries; pageNum++) {
        final BasePage page = tx.getPage(new PageId(database, fileId, pageNum), pageSize);
        final int pageEntries = page.readInt(DATA_ENTRY_COUNT_OFFSET);
        int offset = DATA_ENTRIES_OFFSET;
        // Bounded by the declared entry count and not by the page's own counter alone: that is what keeps
        // an append committing between the two steps from overrunning the array sized for the first one.
        for (int i = 0; i < pageEntries && id <= storedEntries; i++) {
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

    // The file demonstrably holds these pages, whatever this instance's counter was seeded with. This
    // matters beyond reporting: appendEntries() allocates a data page as new when the counter says the
    // page number is past the end, which over an under-reported counter would overwrite a populated page.
    // updatePageCount() only ever raises the counter, so this cannot pull back an instance already ahead.
    updatePageCount(pages + 1);
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

    try {
      // Reload only when the pages really do hold the id, which is what tells a stale map apart from a
      // corrupt id: without this a scan carrying a corrupt id would re-read every page on every row.
      // The test is against the count stored in the header - one already-cached page read - and not
      // against the last reload, because a leader interns for as long as it ingests: keying on the
      // reload would self-heal for the first wave of ids and then never again.
      final StoredHeader header = readStoredHeader();
      if (header == null || id > header.entryCount())
        return null;

      load(header);
    } catch (final IOException e) {
      // A read path declines the value rather than failing the scan around it.
      LogManager.instance().log(this, Level.WARNING,
          "Error reloading TimeSeries tag dictionary '%s' while resolving id %d: %s", e, getName(), id, e.getMessage());
      return null;
    }

    values = byId;
    return id < values.length ? values[id] : null;
  }

  /**
   * Reads the header page and returns {@code { stored entry count, data page count }}, or {@code null}
   * when this file carries no initialised header - which is the only thing that means "nothing has ever
   * been stored here".
   * <p>
   * <b>The header page is the authority, not {@link #getTotalPages()}</b> (issue #6198). That counter is
   * per-instance: it is seeded from the physical file size at construction and afterwards only the
   * component <em>registered with the schema</em> gets it bumped at commit. So an instance built over a
   * file whose committed pages are still in the flush queue reads zero pages for a file that holds
   * several, and gating on it conflated "this instance has loaded nothing" with "the file holds
   * nothing" - disabling the self-heal in {@link #resolveMiss} exactly in the state it exists for, and
   * letting {@link #openOrCreate} write a fresh header over a populated file. Reading page 0 has neither
   * blind spot: the read cache and the flush queue sit in front of the disk, so a committed page is
   * visible whether or not it has reached the file, and the magic tells an initialised header from a
   * page that is not there at all.
   * <p>
   * Read straight through the page manager rather than in a transaction of its own: this is reachable
   * from inside a scan, where opening one is both a cost and a hazard, and the page is never fabricated
   * ({@code createIfNotExists} false), so probing a file that has no page 0 leaves no phantom behind.
   * <p>
   * A failure to read a header that is there is not "there is no header": it is thrown, so a caller that
   * cannot cope with an empty dictionary ({@link #load()}, {@link #openOrCreate}) fails loudly instead of
   * publishing an empty mapping over the real one. {@link #resolveMiss} is the caller that does cope, and
   * it catches.
   */
  private StoredHeader readStoredHeader() throws IOException {
    final BasePage headerPage = database.getPageManager()
        .getImmutablePage(new PageId(database, fileId, 0), pageSize, true, false);
    if (headerPage == null || headerPage.readInt(HEADER_MAGIC_OFFSET) != MAGIC_VALUE)
      return null;
    return new StoredHeader(headerPage.readInt(HEADER_ENTRY_COUNT_OFFSET), headerPage.readInt(HEADER_DATA_PAGE_COUNT));
  }

  /**
   * What the header page says the file holds: the two counters every decision in this class is made
   * against, named rather than carried as a pair of positional ints.
   */
  private record StoredHeader(int entryCount, int dataPageCount) {
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

  /**
   * Validates everything this format asserts about itself and returns one line per problem, empty when there is
   * none - the shape {@code IndexInternal.checkIntegrity()} uses, so {@code DatabaseChecker} folds the answers of
   * every storage kind the same way (issue #6340).
   * <p>
   * What it can find that nothing else does: a dictionary is the only thing that can turn a 4-byte id in a
   * mutable row back into the tag it stands for, so a truncated or unwalkable one does not fail a query - it makes
   * every tag written since the damage read back as {@code null}, silently, on every row. The entry count in page
   * 0 and the per-page counters are reconcilable against the entries themselves, which is what lets that be said
   * before a reader trips over it.
   * <p>
   * The walk is the same one {@link #load(StoredHeader)} does and costs the same: one page read per data page,
   * with the entries decoded only far enough to step over them.
   * <p>
   * Held under {@link #internLock} for the same reason the shard check holds its append lock: the counters are
   * raised by a live writer, so reading page 0 and then walking the pages without it would report a healthy
   * dictionary as short by exactly the entries an intern committed in between. Interning is contended only during
   * warm-up, so this blocks nothing in steady state.
   */
  public List<String> checkIntegrity() throws IOException {
    internLock.lock();
    try {
      return checkIntegrityUnderLock();
    } finally {
      internLock.unlock();
    }
  }

  private List<String> checkIntegrityUnderLock() throws IOException {
    final List<String> problems = new ArrayList<>();

    final long fileSize = getComponentFile().getSize();
    if (fileSize % pageSize != 0)
      problems.add("the file is " + fileSize + " bytes, which is not a whole number of " + pageSize
          + "-byte pages: it was written at a different page size, or its tail was truncated");

    final BasePage headerPage = database.getPageManager()
        .getImmutablePage(new PageId(database, fileId, 0), pageSize, true, false);
    if (headerPage == null || headerPage.readInt(HEADER_MAGIC_OFFSET) != MAGIC_VALUE) {
      if (fileSize > 0)
        problems.add("the file holds " + fileSize + " bytes but page 0 does not carry the 'TSTD' magic: the header "
            + "page is missing or was overwritten, so every tag id in this type's rows is unresolvable");
      return problems;
    }

    final int formatVersion = headerPage.readByte(HEADER_FORMAT_VERSION_OFFSET) & 0xFF;
    if (formatVersion != version)
      problems.add("page 0 declares format version " + formatVersion + " but the file name says version " + version);

    final int declaredEntries = headerPage.readInt(HEADER_ENTRY_COUNT_OFFSET);
    final int declaredPages = headerPage.readInt(HEADER_DATA_PAGE_COUNT);
    if (declaredEntries < 0 || declaredPages < 0) {
      problems.add("page 0 declares " + declaredEntries + " entries over " + declaredPages
          + " data page(s), and a count cannot be negative");
      return problems;
    }

    final int capacity = pageSize - BasePage.PAGE_HEADER_SIZE - DATA_ENTRIES_OFFSET;
    int entries = 0;
    // Whether every page page 0 announces was walked to its end. The total below is a statement about ALL of them,
    // so a walk that stopped partway cannot make it: what it counted is a prefix, and reporting a prefix as the
    // whole restates one finding as two.
    boolean walkedEveryEntry = true;

    for (int pageNumber = 1; pageNumber <= declaredPages; pageNumber++) {
      final BasePage page = database.getPageManager()
          .getImmutablePage(new PageId(database, fileId, pageNumber), pageSize, true, false);
      if (page == null) {
        problems.add("page 0 declares " + declaredPages + " data page(s) but page " + pageNumber
            + " is not in the file: every id stored on it is unresolvable");
        walkedEveryEntry = false;
        break;
      }

      final int pageEntries = page.readInt(DATA_ENTRY_COUNT_OFFSET);
      final int pageUsed = page.readInt(DATA_USED_BYTES_OFFSET);
      if (pageEntries < 0 || pageUsed < 0 || pageUsed > capacity) {
        problems.add("data page " + pageNumber + " declares " + pageEntries + " entries over " + pageUsed
            + " byte(s), which a page of capacity " + capacity + " cannot hold");
        walkedEveryEntry = false;
        continue;
      }

      // Walked rather than trusted: the counters and the bytes have to agree, and it is the bytes a reader
      // actually steps through - an entry whose length prefix overruns the used region makes every entry after
      // it on the page resolve to something else.
      int offset = 0;
      int walked = 0;
      while (walked < pageEntries) {
        if (offset + 2 > pageUsed) {
          problems.add("data page " + pageNumber + " declares " + pageEntries + " entries but only " + walked
              + " fit in the " + pageUsed + " byte(s) it declares as used");
          walkedEveryEntry = false;
          break;
        }
        final int length = page.readShort(DATA_ENTRIES_OFFSET + offset) & 0xFFFF;
        if (length > TimeSeriesBucket.MAX_STRING_BYTES || offset + 2 + length > pageUsed) {
          problems.add("entry " + walked + " of data page " + pageNumber + " declares a length of " + length
              + " byte(s), which runs past the " + pageUsed + " byte(s) the page declares as used");
          walkedEveryEntry = false;
          break;
        }
        offset += 2 + length;
        walked++;
      }
      entries += walked;
    }

    if (walkedEveryEntry && entries != declaredEntries)
      problems.add("page 0 declares " + declaredEntries + " entries but its " + declaredPages
          + " data page(s) hold " + entries + ": the ids above " + entries + " resolve to nothing");

    return problems;
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
