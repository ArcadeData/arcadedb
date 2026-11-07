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
package com.arcadedb.index.columnar;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordFactory;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.IntHashSet;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * One apply pass of one projection: takes the dirty pages out of its trackers, re-reads them at their newest committed version and
 * brings the overlay up to date with them (spec 6.2, 6.3). Runs on {@link ColumnarApplyPool} (or a closing thread) holding
 * {@link ColumnarProjection#applyLock()} for the whole pass (R13).
 * <ol>
 * <li>The trackers are drained (primary, and external when the bucket has one, R7). The owners of the drained pages in the content owner
 * maps (records behind a placeholder pointer or a chunk chain whose bytes live on a drained primary page, records whose EXTERNAL value
 * lives on a drained external page) are re-read too, unless their own page is drained: that page's visit handles them (an owner on a
 * page drained by record joins its records).</li>
 * <li>Each drained primary page is visited ascending through {@link LocalBucket#visitCommittedPage}, with no transaction; a page a local
 * commit marked by record (Task 15 of #10662) only for its marked records, through {@link LocalBucket#visitCommittedSlots}, its block
 * keeping the version of its last whole visit. A RECORD is extracted column by column with the projection's own writer dictionaries
 * and compared with {@link ColumnarProjection#liveValueLookup}: equal is a no-op (spec 6.3, the vertex head pointer churn of an edge
 * insert), different is staged in the page's NEW {@link OverlayPageBlock} (seeded first from the page's current block: a block is the
 * page's WHOLE overlay content). A changed value whose old one lives in a segment marks that row dead (V-M18), one in the frozen overlay
 * of a running flush supersedes it there (V-B2).
 * A live position that is no record any more (deleted, a freed slot, content of another record, past the page's record count) dies the
 * same way, and so does a chunked record deleted while the pass walked its chain (reported {@code EMPTY}, Task 14 of #10662). Each
 * owner is re-read alone through {@link LocalBucket#visitCommittedRecord}.</li>
 * <li>The owner maps follow every readable record: off-page pages set, a plain or deleted record removed. The changes are collected
 * during the read and written after the publish, under each map's write lock, so a pass that fails leaves the maps as they were.</li>
 * <li>Kill point {@value #BEFORE_PUBLISH}, then dead rows, new blocks, superseded frozen rows and the overlay are published, the owner
 * map changes written, and only then the pages are completed in the trackers (Task 7 ordering) and recorded as applied since the freeze
 * (V-B2).</li>
 * </ol>
 * A RECORD whose bytes cannot be read (a broken chain or pointer, a record in motion, a value that fails to deserialize) keeps its
 * previous value, is counted in {@code apply.unreadable} and never touches the owner maps. A permanently corrupt slot is never re-marked
 * by a commit, so its page is re-marked here with an exponential backoff; after {@value #UNREADABLE_ATTEMPTS_BEFORE_REBUILD} failed
 * retries a {@code corrupt} rebuild is requested, the previous valid state serving meanwhile. The first failure of a page is logged at
 * FINE (most likely a commit still publishing the record), the failed retries at WARNING once per page per minute (with the exception
 * when deserialization threw), and the rebuild request once. A committed page that cannot be read at
 * all is kept whole and retried the same way. So is a page past the bucket's committed page count (a commit's first mark drained before
 * its pages are published), never applied as an empty page, but neither counted, logged nor rebuilt: it leaves the retries once visible,
 * or after the last attempt (see {@link UnreadablePages#notVisible}).
 * <p>
 * Conversion failures are counted once per staged record version and failing value, after the publish: a re-read of an unchanged
 * record with an unconvertible value counts nothing, and a failed pass counts nothing either.
 * <p>
 * Allocation: the buffers are kept by the projection between passes; a pass allocates per record only what deserialization needs,
 * and per page only the live rows of the segments covering it. Nothing is boxed on the per-slot path.
 */
final class ColumnarApplier {
  /** Reached by every pass that drained pages, after the pages are read and before anything of them is published. */
  static final String BEFORE_PUBLISH                    = "apply.beforePublish";
  /** Reached after each drained primary page, or the owners of a clean page, were read and before the block is stamped (Task 15b). */
  static final String AFTER_PAGE_READ                   = "apply.afterPageRead";
  /** Failed retries of a page with an unreadable record, spaced by the backoff, after which the projection is marked corrupt. */
  static final int    UNREADABLE_ATTEMPTS_BEFORE_REBUILD = 5;

  private static final int[] NO_PAGES = new int[0];

  private ColumnarApplier() {
  }

  /**
   * Runs one pass on the calling thread (an apply pool thread, or a closing one) and returns the number of pages drained. A database
   * context is set up for the pass when the thread has none (external values are read through the database) and removed after it.
   * Any implicit transaction those reads opened is ended, whether the context was created here or not; a transaction the thread had
   * active before the pass is left as it is.
   */
  static int runPass(final ColumnarIndexManager manager, final ColumnarProjection projection) {
    final LocalDatabase database = manager.getDatabase();
    return inDatabaseContext(database, () -> {
      projection.applyLock().lock();
      try {
        final ColumnDefinition[] columns = projection.columns();
        final DocumentType type = database.getSchema().getTypeByBucketId(projection.bucketId());
        final String typeName = type == null ? projection.typeName() : type.getName();
        Pass pass = projection.applyScratch();
        if (pass == null || !Arrays.equals(pass.columns, columns) || !pass.typeName.equals(typeName)) {
          pass = new Pass(projection, columns, typeName);
          projection.setApplyScratch(pass);
        }
        return pass.run(manager);
      } finally {
        projection.applyLock().unlock();
      }
    });
  }

  /**
   * Runs {@code body} on the calling thread with a database context, set up when the thread has none (records and their external
   * values are read through the database) and removed after. Any implicit transaction the reads opened is ended, whether the context
   * was created here or not; a transaction the thread had active before is left as it is. Shared by the apply pass and the rebuild.
   */
  static <T> T inDatabaseContext(final LocalDatabase database, final Supplier<T> body) {
    final String path = database.getDatabasePath();
    final boolean contextCreated = DatabaseContext.INSTANCE.getContextIfExists(path) == null;
    if (contextCreated)
      DatabaseContext.INSTANCE.init(database);
    final boolean transactionWasActive = !contextCreated && database.isTransactionActive();
    // THE PROJECTION IS NOT A READER OF THE USER: A BEFORE READ LISTENER MUST NOT FIRE FROM A BACKGROUND JOB (#11351). IT COULD VETO
    // AN EXTERNAL VALUE, DIVERGING THE PROJECTION FROM THE STORED RECORD, OR READ THE DATABASE AND TAKE ITS LOCK, WHICH STALLS A DROP
    // FENCING THIS JOB. THE GUARD THAT KEEPS A LISTENER'S OWN READS FROM FIRING LISTENERS DOES EXACTLY THAT FOR THE WHOLE BODY
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(path);
    final boolean wasFiringReadEvents = context.isFiringReadEvents();
    context.setFiringReadEvents(true);
    try {
      return body.get();
    } finally {
      context.setFiringReadEvents(wasFiringReadEvents);
      try {
        if (!transactionWasActive && database.isTransactionActive())
          database.rollback();
      } finally {
        if (contextCreated)
          DatabaseContext.INSTANCE.removeContext(path);
      }
    }
  }

  /** The drained pages of the tracker, ascending; the records of the pages drained by record go to {@code records} (null: none). */
  private static int[] drain(final DirtyPageTracker tracker, final PositionBitmap records) {
    final IntHashSet pages = new IntHashSet();
    if (tracker.drainInto(pages, records) == 0)
      return NO_PAGES;
    final int[] sorted = pages.toArray();
    Arrays.sort(sorted);
    return sorted;
  }

  /**
   * The pages of one projection whose last visit found an unreadable record, with their retry schedule. Owned by the apply thread
   * (under the projection's apply lock); only {@link #isDue} is read by the tick, lock-free. A visit before the retry is due (a commit
   * re-marked the page) neither counts as an attempt nor moves the schedule, so busy writers cannot rush a page to a rebuild.
   */
  static final class UnreadablePages {
    /** Shortest delay between two logs of the unreadable records of one page. */
    static final  long    LOG_INTERVAL_MS = 60_000L;
    private static final long NEVER       = Long.MIN_VALUE;

    private int[]         pages    = NO_PAGES;
    private int[]         attempts = NO_PAGES;
    private long[]        dueMs    = new long[0];
    private boolean[]     remarked = new boolean[0];
    private long[]        loggedMs = new long[0];
    private int           size;
    private volatile long nextDueMs = Long.MAX_VALUE;
    /** {@link #size} for readers off the apply lock. */
    private volatile int  retrying;

    /** Some page awaits a retry (final review A-I2: such a page's records may be missing from the projection); lock free. */
    boolean hasRetries() {
      return retrying > 0;
    }

    /** A page is due for its retry at the clock time; lock free. */
    boolean isDue(final long nowMs) {
      return nowMs >= nextDueMs;
    }

    /** Marks into the tracker every page whose retry is due, once per attempt. */
    void remarkDue(final long nowMs, final DirtyPageTracker tracker) {
      if (nowMs < nextDueMs)
        return;
      final int[] due = new int[size];
      int count = 0;
      for (int i = 0; i < size; i++)
        if (!remarked[i] && nowMs >= dueMs[i]) {
          remarked[i] = true;
          due[count++] = pages[i];
        }
      tracker.mark(due, count);
      refreshNextDue();
    }

    /**
     * A visit of the page found an unreadable record. The first failure schedules the first retry; the visit of a re-marked page is a
     * failed attempt and schedules the next retry, the delay doubling per attempt.
     *
     * @return the failed attempts of the page so far, 1 for a first failure
     */
    int failed(final int page, final long nowMs) {
      final int i = indexOf(page);
      if (i < 0) {
        add(page, 1, nowMs);
        return 1;
      }
      if (remarked[i]) {
        attempts[i]++;
        dueMs[i] = nowMs + backoffMs(attempts[i]);
        remarked[i] = false;
        refreshNextDue();
      }
      return attempts[i];
    }

    /**
     * A visit gave up reading a record of the page because publications stayed open through every attempt (#11364): nothing is wrong
     * with the record. The page is retried at the first backoff, whatever its failed attempts so far, and the visit is not a failed
     * attempt: it never brings the page closer to a {@value ColumnarApplier#UNREADABLE_ATTEMPTS_BEFORE_REBUILD}-attempt rebuild.
     */
    void inMotion(final int page, final long nowMs) {
      final int i = indexOf(page);
      if (i < 0) {
        add(page, 0, nowMs);
        return;
      }
      if (remarked[i]) {
        dueMs[i] = nowMs + backoffMs(1);
        remarked[i] = false;
        refreshNextDue();
      }
    }

    private void add(final int page, final int failedAttempts, final long nowMs) {
      if (size == pages.length) {
        final int capacity = Math.max(4, size * 2);
        pages = Arrays.copyOf(pages, capacity);
        attempts = Arrays.copyOf(attempts, capacity);
        dueMs = Arrays.copyOf(dueMs, capacity);
        remarked = Arrays.copyOf(remarked, capacity);
        loggedMs = Arrays.copyOf(loggedMs, capacity);
      }
      pages[size] = page;
      attempts[size] = failedAttempts;
      dueMs[size] = nowMs + backoffMs(1);
      remarked[size] = false;
      loggedMs[size] = NEVER;
      size++;
      refreshNextDue();
    }

    /**
     * A visit found the page past the bucket's committed page count (final review A-C1): retried as an unreadable page is, with no log,
     * no count and no rebuild. A commit's second mark re-marks its new pages once they are visible, which ends the retry; a page still
     * past the count after {@value ColumnarApplier#UNREADABLE_ATTEMPTS_BEFORE_REBUILD} attempts belongs to no commit (a refused one, or a mark of a page
     * that was never allocated): it holds no record and leaves the retries.
     */
    void notVisible(final int page, final long nowMs) {
      if (failed(page, nowMs) >= UNREADABLE_ATTEMPTS_BEFORE_REBUILD)
        readable(page);
    }

    /** A visit of the whole page read every record: the page leaves the retries. */
    void readable(final int page) {
      if (size == 0)
        return;
      final int i = indexOf(page);
      if (i < 0)
        return;
      size--;
      pages[i] = pages[size];
      attempts[i] = attempts[size];
      dueMs[i] = dueMs[size];
      remarked[i] = remarked[size];
      loggedMs[i] = loggedMs[size];
      refreshNextDue();
    }

    /**
     * Whether the failure of a page {@link #failed} recorded is to be logged now: once per page per {@link #LOG_INTERVAL_MS}, so a
     * permanently corrupt record does not log on every pass that visits its page.
     */
    boolean logDue(final int page, final long nowMs) {
      final int i = indexOf(page);
      if (i < 0 || loggedMs[i] != NEVER && nowMs - loggedMs[i] < LOG_INTERVAL_MS)
        return false;
      loggedMs[i] = nowMs;
      return true;
    }

    /** Forgets every page: the projection is rebuilt. */
    void clear() {
      size = 0;
      nextDueMs = Long.MAX_VALUE;
      retrying = 0;
    }

    int sizeForTesting() {
      return size;
    }

    private int indexOf(final int page) {
      for (int i = 0; i < size; i++)
        if (pages[i] == page)
          return i;
      return -1;
    }

    private void refreshNextDue() {
      long next = Long.MAX_VALUE;
      for (int i = 0; i < size; i++)
        if (!remarked[i])
          next = Math.min(next, dueMs[i]);
      nextDueMs = next;
      retrying = size;
    }

    private static long backoffMs(final int attempt) {
      return Math.min(ColumnarIndexManager.MAINTENANCE_BACKOFF_BASE_MS << Math.min(attempt - 1, 16),
          ColumnarIndexManager.MAINTENANCE_BACKOFF_MAX_MS);
    }
  }

  /**
   * The values of one record, as {@link ValueSink} calls fill them, into slot {@link #k} of a {@link RowValues}. Shared with the
   * rebuild scan ({@link ColumnarRebuilder}), which extracts the same way into its own dictionaries.
   */
  static final class Sink implements ValueSink {
    private final RowValues target;
    /** The slot the next calls fill; set by the caller before each column. */
    int                     k;

    Sink(final RowValues target) {
      this.target = target;
    }

    @Override
    public void ordinal(final int o) {
      target.present[k] = true;
      target.ordinal[k] = o;
    }

    @Override
    public void ordinals(final int[] sorted, final int count) {
      int[] list = target.list[k];
      if (list == null || list.length < count)
        target.list[k] = list = new int[Math.max(count, 8)];
      System.arraycopy(sorted, 0, list, 0, count);
      target.listCount[k] = count;
      target.present[k] = count > 0;
    }

    @Override
    public void longValue(final long v) {
      target.present[k] = true;
      target.longs[k] = v;
    }

    @Override
    public void doubleValue(final double v) {
      target.present[k] = true;
      target.doubles[k] = v;
    }

    @Override
    public void missing() {
      target.present[k] = false;
    }

    @Override
    public void unknownValue(final Object normalized) {
      // an apply extraction grows the dictionary, so it never meets an unknown value
      target.present[k] = false;
    }
  }

  /**
   * The distinct pages of the paired external-property bucket that hold the projected values of a record, every chunk page of a value
   * stored as a chunk chain included (R7), so a tail-only rewrite of an external value re-reads its owner. Reused across records:
   * {@link #pages()} is valid until the next {@link #collect}. Shared by the apply pass and the rebuild scan.
   */
  static final class ExternalValuePages {
    private final int         externalFileId;
    private final int         maxRecordsInPage;
    private final LocalBucket bucket;
    private int[]             pages = new int[4];
    private int               count;
    /** Adds the chunk pages of an external value (its off-page pages). */
    private final LocalBucket.CommittedSlotVisitor chain = (position, kind, content, offPagePages, offPageCount) -> {
      for (int i = 0; i < offPageCount; i++)
        add(offPagePages[i]);
    };

    ExternalValuePages(final LocalBucket externalBucket) {
      this.bucket = externalBucket;
      this.externalFileId = externalBucket.getFileId();
      this.maxRecordsInPage = externalBucket.getMaxRecordsInPage();
    }

    /** Fills {@link #pages()} with the external pages of the record's values of the columns; returns their count. */
    int collect(final BinarySerializer serializer, final DatabaseInternal database, final Document document,
        final ColumnDefinition[] columns) {
      count = 0;
      final Map<String, RID> rids = serializer.findExistingExternalRids(database, document);
      if (rids.isEmpty())
        return 0;
      for (final ColumnDefinition column : columns) {
        final RID rid = rids.get(column.property());
        if (rid == null || rid.getBucketId() != externalFileId)
          continue;
        add((int) (rid.getPosition() / maxRecordsInPage));
        bucket.visitCommittedRecord(rid.getPosition(), chain);
      }
      return count;
    }

    int[] pages() {
      return pages;
    }

    private void add(final int page) {
      for (int i = 0; i < count; i++)
        if (pages[i] == page)
          return;
      if (count == pages.length)
        pages = Arrays.copyOf(pages, count * 2);
      pages[count++] = page;
    }
  }

  /**
   * Fills a {@link RowValues} with the projected values of one record, with the failing values and the coerced flags of each column:
   * from a record ({@link #decode(Document)}), or (Task 2 of #10662) straight from the record's bytes ({@link #decode(Binary)}), with
   * no record, no String for an ordinal value and no list built. A column with no writer dictionary yet (V-B4) is missing. Owned by one
   * apply pass; reused across its records and runs.
   */
  static final class RowDecoder {
    final RowValues                                   fresh;
    /** Failing values of each column of the record being read, counted only when the record is staged. */
    final int[]                                       failures;
    /** By column: whether the record being read holds a value converted from another stored type. */
    final boolean[]                                   coerced;
    private final ColumnDefinition[]                  columns;
    private final GlobalDictionary[]                  dictionaries;
    private final ColumnarValueExtractor              extractor;
    private final ColumnarValueExtractor.RecordReader reader;
    private final Sink                                sink;
    // per run
    private BinarySerializer                          serializer;
    private DatabaseInternal                          database;
    private boolean                                   direct;

    /** @param dictionaries the writer dictionaries by column, filled by the owner before each run (shared, not copied) */
    RowDecoder(final String typeName, final ColumnDefinition[] columns, final GlobalDictionary[] dictionaries, final ColumnarStats stats) {
      this.columns = columns;
      this.dictionaries = dictionaries;
      // the dictionaries are passed explicitly, the resolver is never asked
      this.extractor = new ColumnarValueExtractor(typeName, columns, (bucketId, property) -> {
        throw new IllegalStateException("The apply pass passes its dictionaries explicitly");
      }, stats);
      this.reader = new ColumnarValueExtractor.RecordReader(columns);
      this.fresh = new RowValues(columns.length);
      this.sink = new Sink(fresh);
      this.failures = new int[columns.length];
      this.coerced = new boolean[columns.length];
    }

    ColumnarValueExtractor extractor() {
      return extractor;
    }

    /**
     * Once per run. The bytes are read directly only when they hold exactly what the record read would answer: no paired external
     * bucket (an external value is read through its RID), no data encryption, no String a declared temporal property converts when
     * read, and every column walked from the header.
     */
    void prepare(final LocalDatabase db, final DocumentType type, final boolean externalBucket) {
      reader.prepare(db.getSchema().getDictionary());
      serializer = db.getSerializer();
      database = db.getWrappedDatabaseInstance();
      direct = !externalBucket && serializer.getDataEncryption() == null && type instanceof LocalDocumentType local
          && !local.mayConvertStoredStrings() && reader.walksEveryColumn();
    }

    /**
     * Decodes the record from its bytes (positioned at its record type byte); false, with nothing to use, when the bytes cannot be read
     * here exactly as the record read reads them (the caller then decodes the record). A dictionary append that
     * fails fails the pass, as on the record path; any other exception (a deserializer failure included) declines. The content's
     * position is restored.
     * <p>
     * A decline after this record's earlier columns (or earlier items of a list) were appended to the dictionary leaves those ordinals
     * in it even when the record path then reads the record differently, which only corrupt bytes cause: an ordinal no row uses, as a
     * deleted value leaves one, so counts are unaffected. It is the one case where the ordinals assigned differ from the record path's.
     */
    boolean decode(final Binary content) {
      if (!direct)
        return false;
      final int before = content.position();
      try {
        if (!reader.begin(content, serializer))
          return false;
        for (int k = 0; k < columns.length; k++) {
          fresh.present[k] = false;
          failures[k] = 0;
          coerced[k] = false;
          final ColumnDefinition column = columns[k];
          final ColumnKind kind = column.kind();
          if ((kind == ColumnKind.ORDINAL || kind == ColumnKind.ORDINAL_LIST) && dictionaries[k] == null)
            continue;
          sink.k = k;
          final int at = reader.position(k);
          if (at < 0) {
            failures[k] = extractor.extractValueForApplyUncounted(null, column, dictionaries[k], sink);
            continue;
          }
          content.position(at);
          final byte type = content.getByte();
          if (type == BinaryTypes.TYPE_STRING && kind == ColumnKind.ORDINAL) {
            coerced[k] = ColumnarValueExtractor.isCoerced(column, String.class);
            failures[k] = ColumnarValueExtractor.extractUtf8ForApply(content, dictionaries[k], sink);
          } else if (type == BinaryTypes.TYPE_LIST && kind == ColumnKind.ORDINAL_LIST) {
            // the record read answers an ArrayList
            coerced[k] = ColumnarValueExtractor.isCoerced(column, ArrayList.class);
            failures[k] = extractor.extractListForApply(content, serializer, database, dictionaries[k], sink);
          } else {
            final Object raw = ColumnarValueExtractor.deserializeForApply(serializer, database, content, type);
            coerced[k] = raw != null && ColumnarValueExtractor.isCoerced(column, raw);
            failures[k] = extractor.extractValueForApplyUncounted(raw, column, dictionaries[k], sink);
          }
        }
        return true;
      } catch (final ColumnarTestHooks.SimulatedCrash | UncheckedIOException e) {
        // a dictionary append that failed (the only UncheckedIOException left here: deserializeForApply converts the deserializer's)
        throw e;
      } catch (final RuntimeException e) {
        // bytes the record read refuses or reads as missing (it logs them): left to it, which decides the same way as before
        return false;
      } finally {
        content.position(before);
      }
    }

    /** Decodes the record through its one-walk reader. */
    void decode(final Document document) {
      reader.begin(document);
      try {
        for (int k = 0; k < columns.length; k++) {
          fresh.present[k] = false;
          failures[k] = 0;
          coerced[k] = false;
          final ColumnKind kind = columns[k].kind();
          if ((kind == ColumnKind.ORDINAL || kind == ColumnKind.ORDINAL_LIST) && dictionaries[k] == null)
            continue;
          sink.k = k;
          final Object raw = reader.raw(k);
          coerced[k] = raw != null && ColumnarValueExtractor.isCoerced(columns[k], raw);
          failures[k] = extractor.extractValueForApplyUncounted(raw, columns[k], dictionaries[k], sink);
        }
      } finally {
        reader.end();
      }
    }
  }

  /**
   * The pass itself, with the buffers it reuses: one instance per projection, column set and type name, kept by the projection and used
   * under its apply lock only. The per-run fields are set by {@link #run}.
   */
  static final class Pass implements LocalBucket.CommittedSlotVisitor {
    private final ColumnarProjection     projection;
    private final ColumnDefinition[]     columns;
    private final String                 typeName;
    private final int                    maxRecordsInPage;
    private final RowDecoder             decoder;
    private final ColumnarValueExtractor extractor;
    private final RowBuffer              rows;
    private final RowValues              fresh;
    private final RowValues              old;
    /** The old-value lookups of {@link #stage} and {@link #delete} (Task 2 of #10662); its blocks are dropped at the end of each run. */
    private final ColumnarProjection.LiveLookup lookup;
    private final long[]                 live;
    private final long[]                 records;
    private final GlobalDictionary[]     dictionaries;
    /** {@link RowDecoder#failures} of the record being read. */
    private final int[]                  failures;
    private long[]                       found       = new long[16];
    private long[]                       owners      = new long[16];
    private OverlayPageBlock[]           blocks      = new OverlayPageBlock[16];
    // owner map changes of the run, applied after its publish: [owner], whether external, and pages[pageStart[i], pageStart[i + 1])
    private long[]                       changedOwners      = new long[16];
    private boolean[]                    changedExternal    = new boolean[16];
    private int[]                        changedPageStart   = new int[17];
    private int[]                        changedPages       = new int[16];
    private int                          changedOwnerCount;

    // per run
    private ColumnarIndexManager manager;
    private LocalBucket       bucket;
    private DocumentType      type;
    private DatabaseInternal  database;
    private RecordFactory     recordFactory;
    private BinarySerializer  serializer;
    private int               externalFileId;
    private ExternalValuePages externalValues;
    private PositionBitmap    deadNow;
    private PositionBitmap    superseded;
    /**
     * The records to re-read on the drained primary pages marked by record (Task 15 of #10662), plus the owners on those pages of a
     * drained content page; a drained page with no record here is read whole.
     */
    private PositionBitmap    marked;
    private int               blockCount;
    private long              reread;
    private long              unreadable;
    // per page
    private boolean           changed;
    private boolean           unreadableHere;
    /** The page was past the committed page count: retried, but neither counted, logged nor rebuilt as unreadable. */
    private boolean           pastCount;
    /** A record of the page was unreadable, or the page itself: a failed attempt. */
    private boolean           failedHere;
    private boolean           recordReported;
    private RuntimeException  unreadableCause;
    /** Failing values of the staged records by column, counted after the publish so a failed pass counts nothing. */
    private final long[]      stagedFailures;
    /** Values of the staged records converted from another stored type, by column. */
    private final long[]      stagedCoerced;
    /** {@link RowDecoder#coerced} of the record being read. */
    private final boolean[]   coerced;

    private Pass(final ColumnarProjection projection, final ColumnDefinition[] columns, final String typeName) {
      this.projection = projection;
      this.columns = columns;
      this.typeName = typeName;
      this.maxRecordsInPage = projection.maxRecordsInPage();
      this.dictionaries = new GlobalDictionary[columns.length];
      this.decoder = new RowDecoder(typeName, columns, dictionaries, projection.stats());
      this.extractor = decoder.extractor();
      this.rows = new RowBuffer(maxRecordsInPage, columns);
      this.fresh = decoder.fresh;
      this.old = new RowValues(columns.length);
      this.lookup = new ColumnarProjection.LiveLookup(projection);
      this.live = new long[maxRecordsInPage / 64];
      this.records = new long[maxRecordsInPage / 64];
      this.failures = decoder.failures;
      this.stagedFailures = new long[columns.length];
      this.stagedCoerced = new long[columns.length];
      this.coerced = decoder.coerced;
    }

    private int run(final ColumnarIndexManager manager) {
      final long nowMs = manager.nowMs();
      final UnreadablePages retries = projection.unreadablePages();
      retries.remarkDue(nowMs, projection.tracker());

      marked = new PositionBitmap(maxRecordsInPage);
      final int[] pages = drain(projection.tracker(), marked);
      final DirtyPageTracker externalTracker = projection.externalTracker();
      final int[] externalPages = externalTracker == null ? NO_PAGES : drain(externalTracker, null);
      if (pages.length == 0 && externalPages.length == 0)
        return 0;

      this.manager = manager;
      prepare(manager.getDatabase());
      try {
        final int ownerCount = ownersToReread(pages, externalPages);
        boolean corrupt = false;
        for (final int page : pages) {
          final boolean whole = applyPage(page);
          if (!unreadableHere) {
            // a visit of some records of the page says nothing of the others
            if (whole)
              retries.readable(page);
          } else if (pastCount)
            retries.notVisible(page, nowMs);
          else
            corrupt |= retry(retries, page, nowMs);
        }
        for (int i = 0; i < ownerCount; ) {
          final int page = (int) (owners[i] / maxRecordsInPage);
          int end = i + 1;
          while (end < ownerCount && owners[end] / maxRecordsInPage == page)
            end++;
          applyOwners(page, i, end);
          if (unreadableHere)
            corrupt |= retry(retries, page, nowMs);
          i = end;
        }

        // nothing of the run is visible before this point: a failed pass leaves the projection and its owner maps as they were
        ColumnarTestHooks.reach(BEFORE_PUBLISH);
        // the projection's conversion counts first, so a manifest that names a published row also counts its value (a failed pass
        // over-counts, which only makes a GROUP BY decline)
        for (int k = 0; k < columns.length; k++)
          if (stagedFailures[k] > 0 || stagedCoerced[k] > 0)
            projection.countConversions(columns[k].indexFileId(), stagedFailures[k], stagedCoerced[k]);
        publish();
        applyOwnerChanges();

        // counted before the pages complete, so whoever sees the pass caught up also sees its counts
        final ColumnarStats stats = projection.stats();
        stats.applyPages(typeName, pages.length + externalPages.length);
        stats.applyPass();
        stats.setOverlayRows(typeName, projection.overlay().rows());
        stats.recordsReread(typeName, reread);
        if (ownerCount > 0)
          stats.placeholdersResolved(typeName, ownerCount);
        for (long i = 0; i < unreadable; i++)
          stats.unreadable(typeName);
        for (int k = 0; k < columns.length; k++)
          if (stagedFailures[k] > 0)
            extractor.countFailures(columns[k], (int) stagedFailures[k]);
        for (int k = 0; k < columns.length; k++)
          if (stagedCoerced[k] > 0)
            extractor.countCoerced(columns[k], stagedCoerced[k]);

        projection.tracker().completed(pages, pages.length);
        projection.pagesApplied(pages, pages.length);
        if (externalTracker != null) {
          externalTracker.completed(externalPages, externalPages.length);
          projection.externalPagesApplied(externalPages, externalPages.length);
        }
        if (corrupt) {
          // the previous valid state keeps serving while the rebuild runs
          LogManager.instance().log(this, Level.WARNING, "Records of bucket '%s' stayed unreadable after %d retries: a rebuild of its "
              + "columnar projection is requested", null, projection.bucketName(), UNREADABLE_ATTEMPTS_BEFORE_REBUILD);
          retries.clear();
          projection.requestRebuild(ColumnarProjection.CORRUPT);
        }
        return pages.length + externalPages.length;
      } finally {
        Arrays.fill(blocks, 0, blockCount, null);
        blockCount = 0;
        lookup.clear();
        changedOwnerCount = 0;
        unreadableCause = null;
        deadNow = null;
        superseded = null;
        marked = null;
        this.manager = null;
        bucket = null;
        type = null;
        database = null;
        externalValues = null;
      }
    }

    /**
     * Schedules the retry of a page with a record that could not be read: a failed attempt, unless every record that failed was only
     * given up on open publications (#11364). True when the page is due for a rebuild.
     */
    private boolean retry(final UnreadablePages retries, final int page, final long nowMs) {
      if (failedHere)
        return failed(retries, page, nowMs);
      retries.inMotion(page, nowMs);
      LogManager.instance().log(this, Level.FINE, "Records of page %d of bucket '%s' were given up on open publications: the page is "
          + "retried without counting as unreadable", null, page, projection.bucketName());
      return false;
    }

    /**
     * Records the failed visit of a page in the retries and logs it; true when the page is due for a rebuild. The first failure, which
     * covers every visit before the first retry and is most likely a commit still publishing the record, logs at FINE without using up
     * the rate limit, so the WARNING of the first retry that still fails is not held back by it; the WARNINGs are rate limited.
     */
    private boolean failed(final UnreadablePages retries, final int page, final long nowMs) {
      final int attempts = retries.failed(page, nowMs);
      // with the exception of a record that failed to deserialize; a broken chain, a record in motion, an undecodable header or an
      // unreadable committed page has none
      if (attempts <= 1)
        LogManager.instance().log(this, Level.FINE, "Unreadable record on page %d of bucket '%s' (first visit, likely a commit still "
            + "publishing it): its previous columnar value is kept and the page is retried", unreadableCause, page, projection.bucketName());
      else if (retries.logDue(page, nowMs))
        LogManager.instance().log(this, Level.WARNING, "Unreadable record on page %d of bucket '%s' after %d attempts: its previous "
            + "columnar value is kept and the page is retried", unreadableCause, page, projection.bucketName(), attempts);
      return attempts >= UNREADABLE_ATTEMPTS_BEFORE_REBUILD;
    }

    private void prepare(final LocalDatabase db) {
      bucket = (LocalBucket) db.getSchema().getBucketById(projection.bucketId());
      type = db.getSchema().getTypeByBucketId(projection.bucketId());
      database = db.getWrappedDatabaseInstance();
      recordFactory = db.getRecordFactory();
      serializer = db.getSerializer();
      externalFileId = projection.externalFileId();
      externalValues = externalFileId < 0 ? null : new ExternalValuePages((LocalBucket) db.getSchema().getBucketById(externalFileId));
      for (int k = 0; k < columns.length; k++) {
        final ColumnKind kind = columns[k].kind();
        dictionaries[k] = kind == ColumnKind.ORDINAL || kind == ColumnKind.ORDINAL_LIST ? projection.dictionary(columns[k].property()) : null;
      }
      decoder.prepare(db, type, externalFileId >= 0);
      deadNow = new PositionBitmap(maxRecordsInPage);
      superseded = new PositionBitmap(maxRecordsInPage);
      Arrays.fill(stagedFailures, 0L);
      Arrays.fill(stagedCoerced, 0L);
      reread = 0;
      unreadable = 0;
    }

    /**
     * Fills {@link #owners} with the owners of the drained primary and external pages, ascending and distinct, leaving out those whose
     * own page is drained: that page's visit reads them, an owner on a page drained by record joined to its {@link #marked} records (a
     * page is visited once per pass, its block built once). Returns their count.
     */
    private int ownersToReread(final int[] pages, final int[] externalPages) {
      int count = 0;
      final ContentOwnerMap primary = projection.owners();
      if (!primary.isEmpty())
        for (final int page : pages)
          count = collectOwners(primary, page, pages, count);
      final ContentOwnerMap externalOwners = projection.externalOwners();
      if (externalOwners != null && !externalOwners.isEmpty())
        for (final int page : externalPages)
          count = collectOwners(externalOwners, page, pages, count);
      if (count < 2)
        return count;
      Arrays.sort(owners, 0, count);
      int unique = 1;
      for (int i = 1; i < count; i++)
        if (owners[i] != owners[unique - 1])
          owners[unique++] = owners[i];
      return unique;
    }

    private int collectOwners(final ContentOwnerMap map, final int contentPage, final int[] drainedPages, int count) {
      final int upperBound = map.ownerCountUpperBound(contentPage);
      if (upperBound == 0)
        return count;
      if (found.length < upperBound)
        found = new long[Math.max(upperBound, found.length * 2)];
      final int n = map.owners(contentPage, found);
      for (int i = 0; i < n; i++) {
        final long owner = found[i];
        final int ownerPage = (int) (owner / maxRecordsInPage);
        if (Arrays.binarySearch(drainedPages, ownerPage) >= 0) {
          if (marked.stripe(ownerPage) != null)
            marked.set(owner);
          continue;
        }
        if (count == owners.length)
          owners = Arrays.copyOf(owners, count * 2);
        owners[count++] = owner;
      }
      return count;
    }

    /**
     * A drained primary page: every slot, then every live position that is no record any more; a page drained by record, only its
     * {@link #marked} slots. Returns whether the whole page was read.
     */
    private boolean applyPage(final int page) {
      final long[] only = marked.stripe(page);
      final OverlayPageBlock current = startPage(page);
      Arrays.fill(records, 0L);
      final long version = only == null ? bucket.visitCommittedPage(page, this) : bucket.visitCommittedSlots(page, only, this);
      if (version < 0) {
        // nothing is known of the page, everything is kept and the page retried: a committed page that could not be read, or one past the
        // committed page count (final review A-C1: the first mark of a commit drained before its pages are published, which its second
        // mark re-marks once they are visible; never an empty page)
        if (page < bucket.getCommittedPageCount())
          unreadable();
        else
          unreadableHere = pastCount = true;
        endPage(page, current, 0);
        return only == null;
      }
      ColumnarTestHooks.reach(AFTER_PAGE_READ);
      projection.liveSlots(page, live);
      final long base = (long) page * maxRecordsInPage;
      for (int w = 0; w < live.length; w++)
        for (long gone = live[w] & ~records[w] & (only == null ? -1L : only[w]); gone != 0; gone &= gone - 1) {
          final int slot = (w << 6) + Long.numberOfTrailingZeros(gone);
          delete(base + slot, slot);
        }
      if (only == null || stampsReadVersion(page))
        endPage(page, current, version);
      else
        // the block keeps the version of its last whole read (0 without one), so neither the page check of an unclean open nor a
        // rebuild takes its rows for the committed page
        endPage(page, current, current != null ? current.pageVersion() : 0);
      return only == null;
    }

    /**
     * Task 15b: whether a page just read by record may be stamped with the version read, as a whole read is, so a flushed page is not
     * taken for changed by the page check of an unclean open or a rebuild's reuse. Every write at or below that version is in rows read
     * after it became visible, or still has a mark to come that a crash cannot lose:
     * <ul>
     * <li>a local commit runs its first mark before its publication and so before this read. A first mark not drained yet is in the
     * current epoch, checked here after the read. One drained in this pass names records read now. One drained by an earlier pass is
     * followed by the second mark, after the publication: drained before this pass, its records were read after it; landed since the
     * drain, it is in the current epoch, checked here after the read; not landed yet, the commit keeps its WAL file from every drop
     * ({@code TransactionManager.cleanWALFiles}, spec 6.2), whose replay marks the page;</li>
     * <li>an apply that marks after writing ({@code TransactionManager.applyChanges}) is counted from before its first write until its
     * mark landed: none counted, read before the epoch, means every such mark of a write this read saw is in an epoch;</li>
     * <li>a record that could not be read keeps its old row, so the page keeps its old version too.</li>
     * </ul>
     */
    private boolean stampsReadVersion(final int page) {
      // the counter BEFORE the epoch: an apply decrements it only after its mark landed, so a zero read first means that mark is
      // already in the epoch read next (the other order could miss a mark landing between the two reads)
      return !unreadableHere && !manager.hasUnmarkedApplies() && !projection.tracker().isMarkedSinceDrain(page);
    }

    /** The owners {@code owners[from, to)} of one clean page, each re-read alone. */
    private void applyOwners(final int page, final int from, final int to) {
      final OverlayPageBlock current = startPage(page);
      long version = -1;
      for (int i = from; i < to; i++) {
        final long owner = owners[i];
        recordReported = false;
        final long read = bucket.visitCommittedRecord(owner, this);
        if (read >= 0)
          version = read;
        else {
          // a page that could not be read (an owner's page is committed): the owner is kept and its page retried
          unreadable();
          continue;
        }
        if (!recordReported)
          delete(owner, (int) (owner % maxRecordsInPage));
      }
      ColumnarTestHooks.reach(AFTER_PAGE_READ);
      // the other rows of the page were read at the block's version; with no block, the owners alone stand for the version read only
      // on the terms of a page read by record (Task 15b), else 0
      endPage(page, current, current != null ? current.pageVersion() : version >= 0 && stampsReadVersion(page) ? version : 0);
    }

    private OverlayPageBlock startPage(final int page) {
      changed = false;
      unreadableHere = false;
      pastCount = false;
      failedHere = false;
      unreadableCause = null;
      final OverlayPageBlock current = projection.overlay().page(page);
      if (current != null)
        rows.seed(current);
      else
        rows.clear();
      return current;
    }

    private void endPage(final int page, final OverlayPageBlock current, final long version) {
      if (!changed)
        return;
      if (blockCount == blocks.length)
        blocks = Arrays.copyOf(blocks, blockCount * 2);
      blocks[blockCount++] = OverlayPageBlock.build(page, maxRecordsInPage, columns, version, rows);
    }

    @Override
    public void onSlot(final long position, final int kind, final Binary content, final int[] offPagePages, final int offPageCount) {
      if (kind != RECORD)
        return; // EMPTY and CONTENT: the position is no record, see delete()
      final int slot = (int) (position % maxRecordsInPage);
      if (content == null) {
        // keeps what the projection knew; the owner maps are not touched
        markRecord(slot);
        unreadable();
        return;
      }

      if (decoder.decode(content)) {
        // read from the bytes (Task 2 of #10662): no paired external bucket, so no external pages
        markRecord(slot);
        reread++;
        queueOwner(false, position, offPagePages, offPageCount);
        stage(position, slot);
        return;
      }

      final int externalCount;
      try {
        final Record record = recordFactory.newImmutableRecord(database, type, new RID(projection.bucketId(), position), content, null);
        if (!(record instanceof Document document))
          return; // not a projected record
        markRecord(slot);
        reread++;
        decoder.decode(document);
        externalCount = externalFileId < 0 ? 0 : externalValues.collect(serializer, database, document, columns);
      } catch (final ColumnarTestHooks.SimulatedCrash | UncheckedIOException e) {
        // a crash test, or a dictionary append that failed: the pass fails, its pages stay in flight
        throw e;
      } catch (final RuntimeException e) {
        // bytes that do not deserialize, an external value that cannot be read: unreadable, as a broken chain
        markRecord(slot);
        unreadable();
        if (unreadableCause == null)
          unreadableCause = e;
        return;
      }

      queueOwner(false, position, offPagePages, offPageCount);
      if (externalFileId >= 0)
        queueOwner(true, position, externalValues.pages(), externalCount);
      stage(position, slot);
    }

    private void markRecord(final int slot) {
      records[slot >>> 6] |= 1L << slot;
      recordReported = true;
    }

    private void unreadable() {
      unreadable++;
      unreadableHere = true;
      failedHere = true;
    }

    /** A record given up on open publications (#11364): kept as it was and retried, but neither counted nor a failed attempt. */
    @Override
    public void onRecordInMotion(final long position) {
      markRecord((int) (position % maxRecordsInPage));
      unreadableHere = true;
    }

    /** Stages the record's values unless they equal the live ones (spec 6.3); the value they replace stops being live. */
    private void stage(final long position, final int slot) {
      long where = ColumnarProjection.LOOKUP_ABSENT;
      boolean equal = true;
      for (int k = 0; k < columns.length && equal; k++) {
        final long at = lookup.lookup(position, k, old);
        if (k == 0)
          where = at;
        equal = same(k);
      }
      if (where != ColumnarProjection.LOOKUP_ABSENT && equal)
        return;
      rows.put(slot, fresh);
      changed = true;
      supersede(position, where);
      // once per staged record version and failing value: a re-read of an unchanged record counts nothing
      for (int k = 0; k < columns.length; k++) {
        stagedFailures[k] += failures[k];
        if (coerced[k])
          stagedCoerced[k]++;
      }
    }

    /** The position is no record any more: its live value dies, wherever it is. */
    private void delete(final long position, final int slot) {
      final long where = lookup.lookup(position, 0, old);
      if (where == ColumnarProjection.LOOKUP_OVERLAY) {
        rows.remove(slot);
        changed = true;
      } else
        supersede(position, where);
      queueOwner(false, position, NO_PAGES, 0);
      if (externalFileId >= 0)
        queueOwner(true, position, NO_PAGES, 0);
    }

    /** One live value per position (spec 5.2): an old value in a segment dies (V-M18), one in the frozen overlay is superseded (V-B2). */
    private void supersede(final long position, final long where) {
      if (where == ColumnarProjection.LOOKUP_FROZEN)
        superseded.set(position);
      else if (where >= 0)
        deadNow.set(position);
    }

    private boolean same(final int k) {
      if (fresh.present[k] != old.present[k])
        return false;
      if (!fresh.present[k])
        return true;
      return switch (columns[k].kind()) {
        case ORDINAL -> fresh.ordinal[k] == old.ordinal[k];
        case LONG -> fresh.longs[k] == old.longs[k];
        case DOUBLE -> Double.doubleToLongBits(fresh.doubles[k]) == Double.doubleToLongBits(old.doubles[k]);
        case ORDINAL_LIST -> fresh.listCount[k] == old.listCount[k] && Arrays.equals(fresh.list[k], 0, fresh.listCount[k], old.list[k], 0,
            old.listCount[k]);
      };
    }

    /**
     * Queues the owner's pages for {@link #applyOwnerChanges} when they differ from the map's (each position is read once per pass, so the
     * comparison still holds when the change is applied).
     */
    private void queueOwner(final boolean externalMap, final long owner, final int[] pages, final int count) {
      if ((externalMap ? projection.externalOwners() : projection.owners()).ownsExactly(owner, pages, count))
        return;
      final int n = changedOwnerCount;
      if (n == changedOwners.length) {
        changedOwners = Arrays.copyOf(changedOwners, n * 2);
        changedExternal = Arrays.copyOf(changedExternal, n * 2);
        changedPageStart = Arrays.copyOf(changedPageStart, n * 2 + 1);
      }
      final int start = changedPageStart[n];
      if (start + count > changedPages.length)
        changedPages = Arrays.copyOf(changedPages, Math.max(changedPages.length * 2, start + count));
      System.arraycopy(pages, 0, changedPages, start, count);
      changedOwners[n] = owner;
      changedExternal[n] = externalMap;
      changedPageStart[n + 1] = start + count;
      changedOwnerCount = n + 1;
    }

    /** After the publish: the queued owner changes, each map under its write lock once. */
    private void applyOwnerChanges() {
      if (changedOwnerCount == 0)
        return;
      applyOwnerChanges(false, projection.owners(), projection.ownersLock());
      if (externalFileId >= 0)
        applyOwnerChanges(true, projection.externalOwners(), projection.externalOwnersLock());
    }

    private void applyOwnerChanges(final boolean externalMap, final ContentOwnerMap map, final ReentrantReadWriteLock lock) {
      lock.writeLock().lock();
      try {
        for (int i = 0; i < changedOwnerCount; i++) {
          if (changedExternal[i] != externalMap)
            continue;
          final int start = changedPageStart[i];
          final int count = changedPageStart[i + 1] - start;
          if (count == 0)
            map.removeOwner(changedOwners[i]);
          else
            map.setOwnerPages(changedOwners[i], start == 0 ? changedPages : Arrays.copyOfRange(changedPages, start, start + count), count);
        }
      } finally {
        lock.writeLock().unlock();
      }
    }

    /** Dead rows first, then the new blocks and the superseded frozen rows in one overlay publish; nothing when nothing changed. */
    private void publish() {
      if (blockCount == 0 && deadNow.isEmpty() && superseded.isEmpty())
        return;
      projection.applyDeadPositions(deadNow);
      final ColumnarOverlay overlay = projection.overlay();
      for (int i = 0; i < blockCount; i++)
        overlay.replacePage(blocks[i]);
      superseded.forEach(projection::supersedeFrozen);
      projection.publishOverlay();
    }
  }
}
