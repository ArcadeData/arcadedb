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
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.Type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Mutable TimeSeries bucket backed by paginated storage.
 * Stores samples in row-oriented format within pages for ACID compliance.
 * <p>
 * Header page (page 0) layout (offsets from PAGE_HEADER_SIZE) — 44 bytes:
 * - [0..3]   magic "TSBC" (4 bytes)
 * - [4]      formatVersion (1 byte)
 * - [5..6]   column count (short)
 * - [7..14]  total sample count (long)
 * - [15..22] min timestamp (long)
 * - [23..30] max timestamp (long)
 * - [31]     compaction in progress flag (byte)
 * - [32..39] compaction watermark (long) — sealed store offset
 * - [40..43] active data page count (int)
 * <p>
 * Data pages layout (offsets from PAGE_HEADER_SIZE):
 * - [0..1]   sample count in page (short, read as unsigned with &amp; 0xFFFF)
 * - [2..9]   min timestamp in page (long)
 * - [10..17] max timestamp in page (long)
 * - [18..]   row data: fixed-size rows [timestamp(8)|col1|col2|...]
 *            For STRING columns: 2-byte length prefix + up to MAX_STRING_BYTES payload
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesBucket extends PaginatedComponent {

  public static final  String BUCKET_EXT       = "tstb";
  public static final  int    MAX_STRING_BYTES = 256;
  public static final  int    CURRENT_VERSION  = 0;
  private static final int    MAGIC_VALUE      = 0x54534243; // "TSBC"

  // Header page offsets (from PAGE_HEADER_SIZE)
  private static final int HEADER_MAGIC_OFFSET            = 0;
  private static final int HEADER_FORMAT_VERSION_OFFSET   = 4;
  private static final int HEADER_COLUMN_COUNT_OFFSET     = 5;
  private static final int HEADER_SAMPLE_COUNT_OFFSET     = 7;
  private static final int HEADER_MIN_TS_OFFSET           = 15;
  private static final int HEADER_MAX_TS_OFFSET           = 23;
  private static final int HEADER_COMPACTION_FLAG         = 31;
  private static final int HEADER_COMPACTION_WATERMARK    = 32;
  private static final int HEADER_DATA_PAGE_COUNT         = 40;
  private static final int HEADER_SIZE                    = 44;

  // Data page offsets (from PAGE_HEADER_SIZE)
  // Sample count stored as short (2 bytes), read with & 0xFFFF to treat as unsigned (0..65535).
  // A page can never hold more than (pageSize - overhead) / rowSize samples, which is well under 65535.
  private static final int DATA_SAMPLE_COUNT_OFFSET = 0;
  private static final int DATA_MIN_TS_OFFSET       = 2;
  private static final int DATA_MAX_TS_OFFSET       = 10;
  private static final int DATA_ROWS_OFFSET         = 18;

  private List<ColumnDefinition> columns;
  private int                    rowSize; // fixed row size in bytes
  // Value-column kinds, resolved with rowSize so the boxed append path does not rebuild them per call.
  private byte[]                 columnKinds;

  /**
   * Factory handler for loading existing .tstb files during schema load.
   * Columns are set later via {@link #setColumns(List)} when the TimeSeries type is initialized.
   */
  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new TimeSeriesBucket(database, name, filePath, id, new ArrayList<>());
    }
  }

  /**
   * Creates a new TimeSeries bucket.
   */
  public TimeSeriesBucket(final DatabaseInternal database, final String name, final String filePath,
      final List<ColumnDefinition> columns) throws IOException {
    super(database, name, filePath, BUCKET_EXT, ComponentFile.MODE.READ_WRITE,
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE), CURRENT_VERSION);
    this.columns = columns;
    this.rowSize = calculateRowSize(columns);
    this.columnKinds = ObjectColumnsRowSource.kindsOf(columns);
    // Note: initHeaderPage() is NOT called here.
    // TimeSeriesShard calls it in a self-contained nested transaction after registering the
    // bucket with the schema, so the nested TX commit can resolve the file by its ID.
  }

  /**
   * Opens an existing TimeSeries bucket.
   */
  public TimeSeriesBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
      final List<ColumnDefinition> columns) throws IOException {
    super(database, name, filePath, id, ComponentFile.MODE.READ_WRITE,
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE), CURRENT_VERSION);
    this.columns = columns;
    this.rowSize = calculateRowSize(columns);
    this.columnKinds = ObjectColumnsRowSource.kindsOf(columns);
  }

  /**
   * Sets column definitions (called during cold open after the factory handler creates a stub bucket).
   */
  public void setColumns(final List<ColumnDefinition> columns) {
    this.columns = columns;
    this.rowSize = calculateRowSize(columns);
    this.columnKinds = ObjectColumnsRowSource.kindsOf(columns);
  }

  /**
   * Appends samples to the mutable bucket within the current transaction.
   *
   * @param timestamps array of timestamps (millisecond epoch)
   * @param columnValues array of column value arrays, one per non-timestamp column
   */
  public void appendSamples(final long[] timestamps, final Object[]... columnValues) throws IOException {
    appendSamples(newRowSource(timestamps, columnValues));
  }

  /**
   * Wraps boxed column arrays as a {@link TimeSeriesRowSource} over this bucket's columns. The arrays
   * are held, not copied, and the column kinds come from this bucket's cached table.
   */
  TimeSeriesRowSource newRowSource(final long[] timestamps, final Object[][] columnValues) {
    return new ObjectColumnsRowSource(columnKinds, timestamps, columnValues);
  }

  /**
   * Appends samples to the mutable bucket within the current transaction, reading them straight off a
   * primitive row source so a caller holding primitive data never boxes a value (issue #5474).
   * <p>
   * The page bookkeeping is hoisted out of the per-sample loop: the header page is resolved once and
   * its counters are folded in registers until the end of the batch, and the active data page is kept
   * across samples until it fills. Doing it per sample cost three {@code PageId} allocations and three
   * transaction map lookups for every point written - more garbage than the boxing this method
   * removes.
   */
  public void appendSamples(final TimeSeriesRowSource source) throws IOException {
    final int sampleCount = source.size();
    if (sampleCount == 0)
      return;

    final TransactionContext tx = database.getTransaction();
    final MutablePage headerPage = tx.getPageToModify(new PageId(database, fileId, 0), pageSize, false);

    long totalSamples = headerPage.readLong(HEADER_SAMPLE_COUNT_OFFSET);
    long minTs = headerPage.readLong(HEADER_MIN_TS_OFFSET);
    long maxTs = headerPage.readLong(HEADER_MAX_TS_OFFSET);

    final int maxSamplesPerPage = getMaxSamplesPerPage();
    final int columnCount = columns.size();

    MutablePage dataPage = null;
    int samplesInPage = 0;
    long pageMinTs = 0;
    long pageMaxTs = 0;

    for (int i = 0; i < sampleCount; i++) {
      if (dataPage == null || samplesInPage >= maxSamplesPerPage) {
        if (dataPage != null)
          flushDataPageHeader(dataPage, samplesInPage, pageMinTs, pageMaxTs);

        dataPage = getOrCreateActiveDataPage(tx, headerPage);
        samplesInPage = dataPage.readShort(DATA_SAMPLE_COUNT_OFFSET) & 0xFFFF;
        pageMinTs = samplesInPage == 0 ? Long.MAX_VALUE : dataPage.readLong(DATA_MIN_TS_OFFSET);
        pageMaxTs = samplesInPage == 0 ? Long.MIN_VALUE : dataPage.readLong(DATA_MAX_TS_OFFSET);
      }

      final long timestamp = source.getTimestamp(i);
      final int rowOffset = DATA_ROWS_OFFSET + samplesInPage * rowSize;
      dataPage.writeLong(rowOffset, timestamp);

      int colOffset = rowOffset + 8;
      int colIdx = 0;
      for (int c = 0; c < columnCount; c++) {
        final ColumnDefinition col = columns.get(c);
        if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;

        // The width table decides, not the type name: a column with no fixed width is stored
        // length-prefixed, which is the only encoding the reader can walk past (issue #5475).
        final int fixedSize = col.getFixedSize();
        if (fixedSize > 0)
          colOffset += writeRawValue(dataPage, colOffset, fixedSize, source.getRawValue(i, colIdx));
        else
          colOffset += writeStringValue(dataPage, colOffset, col, source.getStringValue(i, colIdx));
        colIdx++;
      }

      samplesInPage++;
      if (timestamp < pageMinTs)
        pageMinTs = timestamp;
      if (timestamp > pageMaxTs)
        pageMaxTs = timestamp;

      totalSamples++;
      if (timestamp < minTs)
        minTs = timestamp;
      if (timestamp > maxTs)
        maxTs = timestamp;
    }

    flushDataPageHeader(dataPage, samplesInPage, pageMinTs, pageMaxTs);

    headerPage.writeLong(HEADER_SAMPLE_COUNT_OFFSET, totalSamples);
    headerPage.writeLong(HEADER_MIN_TS_OFFSET, minTs);
    headerPage.writeLong(HEADER_MAX_TS_OFFSET, maxTs);
  }

  private static void flushDataPageHeader(final MutablePage page, final int samplesInPage, final long minTs, final long maxTs) {
    page.writeShort(DATA_SAMPLE_COUNT_OFFSET, (short) samplesInPage);
    page.writeLong(DATA_MIN_TS_OFFSET, minTs);
    page.writeLong(DATA_MAX_TS_OFFSET, maxTs);
  }

  /**
   * Scans the mutable bucket for samples in the given time range.
   *
   * @param fromTs start timestamp (inclusive)
   * @param toTs   end timestamp (inclusive)
   * @param columnIndices which columns to return (null = all)
   *
   * @return list of sample rows: each row is Object[] { timestamp, col1, col2, ... }
   */
  public List<Object[]> scanRange(final long fromTs, final long toTs, final int[] columnIndices) throws IOException {
    final List<Object[]> results = new ArrayList<>();
    final int dataPageCount = getDataPageCount();

    for (int pageNum = 1; pageNum <= dataPageCount; pageNum++) {
      final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, pageNum), pageSize);

      final int sampleCount = page.readShort(DATA_SAMPLE_COUNT_OFFSET) & 0xFFFF;
      if (sampleCount == 0)
        continue;

      final long pageMinTs = page.readLong(DATA_MIN_TS_OFFSET);
      final long pageMaxTs = page.readLong(DATA_MAX_TS_OFFSET);

      // Skip pages outside range
      if (pageMaxTs < fromTs || pageMinTs > toTs)
        continue;

      for (int row = 0; row < sampleCount; row++) {
        final int rowOffset = DATA_ROWS_OFFSET + row * rowSize;
        final long ts = page.readLong(rowOffset);

        if (ts < fromTs || ts > toTs)
          continue;

        final Object[] sample = readRow(page, rowOffset, columnIndices);
        results.add(sample);
      }
    }
    return results;
  }

  /**
   * Scans the mutable bucket newest-first and returns at most {@code limit} rows, the newest ones in
   * the requested range.
   * <p>
   * Data pages are visited from the last to the first and are pruned on their min/max timestamp
   * header, so a top-N query never materialises the rows of a page that cannot contribute. Rows are
   * not assumed to be globally ordered across pages (late arrivals are always appended to the last
   * page), which is why a page whose maximum is older than the current cut-off is skipped rather
   * than ending the walk.
   *
   * @param fromTs        start timestamp (inclusive)
   * @param toTs          end timestamp (inclusive)
   * @param columnIndices which columns to return (null = all)
   * @param tagFilter     optional tag filter, evaluated straight off the page so non-matching rows
   *                      cost no allocation
   * @param limit         maximum number of rows to return, 0 or less means unlimited
   * @param metrics       optional page and row counters, may be {@code null}
   *
   * @return list of sample rows ordered from the newest to the oldest
   */
  public List<Object[]> scanRangeDescending(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter, final int limit, final AggregationMetrics metrics) throws IOException {
    final int need = limit > 0 ? limit : Integer.MAX_VALUE;
    final int dataPageCount = getDataPageCount();

    final TagMatcher[] matchers = tagFilter == null ? null : buildTagMatchers(tagFilter, columnIndices);
    if (tagFilter != null && matchers == null)
      // A condition on a column the caller did not ask for: nothing can match.
      return new ArrayList<>();

    // Bounded queries keep the current best rows in a min-heap on the timestamp, so the cut-off is
    // always exact and a row is materialised only when it really enters the result. Unlimited
    // queries have no cut-off to maintain and just collect.
    final PriorityQueue<Object[]> heap = need == Integer.MAX_VALUE ?
        null :
        new PriorityQueue<>(Math.min(need, 1024), (a, b) -> Long.compare((long) a[0], (long) b[0]));
    final List<Object[]> collected = heap == null ? new ArrayList<>() : null;

    // Timestamp of the oldest row retained so far: with `need` rows already held, nothing older can
    // enter the result.
    long cutoffTs = Long.MIN_VALUE;
    int held = 0;

    for (int pageNum = dataPageCount; pageNum >= 1; pageNum--) {
      final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, pageNum), pageSize);

      final int sampleCount = page.readShort(DATA_SAMPLE_COUNT_OFFSET) & 0xFFFF;
      if (sampleCount == 0)
        continue;

      final long pageMinTs = page.readLong(DATA_MIN_TS_OFFSET);
      final long pageMaxTs = page.readLong(DATA_MAX_TS_OFFSET);

      // Skip pages outside range
      if (pageMaxTs < fromTs || pageMinTs > toTs) {
        if (metrics != null)
          metrics.addSkippedPage();
        continue;
      }

      // Skip pages that cannot beat the rows already collected
      if (held >= need && pageMaxTs <= cutoffTs) {
        if (metrics != null)
          metrics.addSkippedPage();
        continue;
      }

      if (metrics != null)
        metrics.addScannedPage();

      // Rows are appended in arrival order, so walking a page backwards yields the newest rows first
      // and the cut-off starts rejecting the rest of the page almost immediately.
      for (int row = sampleCount - 1; row >= 0; row--) {
        final int rowOffset = DATA_ROWS_OFFSET + row * rowSize;
        final long ts = page.readLong(rowOffset);

        if (ts < fromTs || ts > toTs)
          continue;
        if (held >= need && ts <= cutoffTs)
          continue;
        if (matchers != null && !matchesTagFilter(page, rowOffset, matchers))
          continue;

        final Object[] materialized = readRow(page, rowOffset, columnIndices);
        if (metrics != null)
          metrics.addMaterializedRows(1);

        if (heap == null) {
          collected.add(materialized);
          held++;
          continue;
        }

        heap.add(materialized);
        if (heap.size() > need)
          heap.poll();
        held = heap.size();
        if (held >= need)
          cutoffTs = (long) heap.peek()[0];
      }
    }

    final List<Object[]> results = heap == null ? collected : new ArrayList<>(heap);
    TimeSeriesSealedStore.trimToDescendingLimit(results, need);
    return results;
  }

  /**
   * A tag condition prepared for repeated evaluation against raw page bytes.
   * <p>
   * String tags are by far the common case and used to cost a {@code byte[]} plus a {@code String}
   * per row examined. Pre-encoding the candidate values lets the comparison run straight on the page.
   */
  private static final class TagMatcher {
    private final int      columnIndex;
    private final Set<?>   values;
    private final byte[][] utf8Values;

    private TagMatcher(final int columnIndex, final Set<?> values, final byte[][] utf8Values) {
      this.columnIndex = columnIndex;
      this.values = values;
      this.utf8Values = utf8Values;
    }
  }

  /**
   * Prepares the filter for the scan, or returns {@code null} when it can never match.
   * <p>
   * Mirrors {@link TagFilter#matchesMapped(Object[], int[])}: a condition on a column that the
   * caller did not request cannot be satisfied, because the row handed back would not carry it.
   */
  private TagMatcher[] buildTagMatchers(final TagFilter tagFilter, final int[] columnIndices) {
    final List<TagFilter.Condition> conditions = tagFilter.getConditions();
    final TagMatcher[] matchers = new TagMatcher[conditions.size()];

    for (int i = 0; i < conditions.size(); i++) {
      final TagFilter.Condition cond = conditions.get(i);
      if (columnIndices != null && !isInArray(cond.columnIndex(), columnIndices))
        return null;

      boolean allStrings = !cond.matchValues().isEmpty();
      for (final Object value : cond.matchValues())
        if (!(value instanceof String)) {
          allStrings = false;
          break;
        }

      byte[][] utf8Values = null;
      if (allStrings) {
        utf8Values = new byte[cond.matchValues().size()][];
        int v = 0;
        for (final Object value : cond.matchValues())
          utf8Values[v++] = ((String) value).getBytes(StandardCharsets.UTF_8);
      }

      matchers[i] = new TagMatcher(cond.columnIndex(), cond.matchValues(), utf8Values);
    }
    return matchers;
  }

  /**
   * Evaluates the prepared conditions directly against the page, without materialising the row.
   */
  private boolean matchesTagFilter(final BasePage page, final int rowOffset, final TagMatcher[] matchers) {
    for (final TagMatcher matcher : matchers) {
      int colOffset = rowOffset + 8;
      int colIdx = 0;
      ColumnDefinition target = null;
      for (int c = 0; c < columns.size(); c++) {
        final ColumnDefinition col = columns.get(c);
        if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;
        if (colIdx == matcher.columnIndex) {
          target = col;
          break;
        }
        colOffset += getColumnStorageSize(page, colOffset, col);
        colIdx++;
      }

      if (target == null)
        return false;

      // The byte comparison only applies to a STRING column: on any other type the leading bytes are
      // not a length prefix and the value has to be decoded.
      if (matcher.utf8Values != null && target.getDataType() == Type.STRING) {
        if (!matchesUtf8(page, colOffset, matcher.utf8Values))
          return false;
      } else if (!matcher.values.contains(readColumnValue(page, colOffset, target)))
        return false;
    }
    return true;
  }

  /**
   * Compares the STRING stored at {@code offset} against the pre-encoded candidates, allocation-free.
   */
  private static boolean matchesUtf8(final BasePage page, final int offset, final byte[][] candidates) {
    final int len = page.readShort(offset) & 0xFFFF;
    for (final byte[] candidate : candidates) {
      if (candidate.length != len)
        continue;
      boolean equal = true;
      for (int i = 0; i < len; i++)
        if ((byte) page.readByte(offset + 2 + i) != candidate[i]) {
          equal = false;
          break;
        }
      if (equal)
        return true;
    }
    return false;
  }

  /**
   * Returns a lazy iterator over samples in the given time range.
   * Only one page is loaded at a time, keeping memory usage O(pageSize).
   *
   * @param fromTs        start timestamp (inclusive)
   * @param toTs          end timestamp (inclusive)
   * @param columnIndices which columns to return (null = all)
   *
   * @return iterator yielding Object[] { timestamp, col1, col2, ... }
   */
  public Iterator<Object[]> iterateRange(final long fromTs, final long toTs, final int[] columnIndices) throws IOException {
    if (getSampleCount() == 0)
      return Collections.emptyIterator();

    final int dataPageCount = getDataPageCount();

    return new Iterator<>() {
      private int      pageNum            = 1;
      private int      rowIdx             = 0;
      private BasePage currentPage        = null;
      private int      currentSampleCount = 0;
      private Object[] nextRow            = null;

      {
        advance();
      }

      private void advance() {
        nextRow = null;
        try {
          while (pageNum <= dataPageCount) {
            if (currentPage == null) {
              currentPage = database.getTransaction().getPage(new PageId(database, fileId, pageNum), pageSize);
              currentSampleCount = currentPage.readShort(DATA_SAMPLE_COUNT_OFFSET) & 0xFFFF;
              rowIdx = 0;

              if (currentSampleCount == 0) {
                currentPage = null;
                pageNum++;
                continue;
              }

              final long pageMinTs = currentPage.readLong(DATA_MIN_TS_OFFSET);
              final long pageMaxTs = currentPage.readLong(DATA_MAX_TS_OFFSET);
              if (pageMaxTs < fromTs || pageMinTs > toTs) {
                currentPage = null;
                pageNum++;
                continue;
              }
            }

            while (rowIdx < currentSampleCount) {
              final int rowOffset = DATA_ROWS_OFFSET + rowIdx * rowSize;
              final long ts = currentPage.readLong(rowOffset);
              rowIdx++;

              if (ts >= fromTs && ts <= toTs) {
                nextRow = readRow(currentPage, rowOffset, columnIndices);
                return;
              }
            }

            currentPage = null;
            pageNum++;
          }
        } catch (final IOException e) {
          throw new DatabaseOperationException("Error iterating TimeSeries bucket pages", e);
        }
      }

      @Override
      public boolean hasNext() {
        return nextRow != null;
      }

      @Override
      public Object[] next() {
        if (nextRow == null)
          throw new NoSuchElementException();
        final Object[] result = nextRow;
        advance();
        return result;
      }
    };
  }

  /**
   * Returns the total sample count stored in this bucket.
   */
  public long getSampleCount() throws IOException {
    if (getTotalPages() == 0)
      return 0;
    final BasePage headerPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);
    return headerPage.readLong(HEADER_SAMPLE_COUNT_OFFSET);
  }

  /**
   * Returns the minimum timestamp across all samples.
   */
  public long getMinTimestamp() throws IOException {
    final BasePage headerPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);
    return headerPage.readLong(HEADER_MIN_TS_OFFSET);
  }

  /**
   * Returns the maximum timestamp across all samples.
   */
  public long getMaxTimestamp() throws IOException {
    final BasePage headerPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);
    return headerPage.readLong(HEADER_MAX_TS_OFFSET);
  }

  /**
   * Returns the number of data pages (excluding header page).
   */
  public int getDataPageCount() throws IOException {
    if (getTotalPages() == 0)
      return 0;
    final BasePage headerPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);
    return headerPage.readInt(HEADER_DATA_PAGE_COUNT);
  }

  /**
   * Sets the compaction-in-progress flag. Used for crash-safe compaction.
   */
  public void setCompactionInProgress(final boolean inProgress) throws IOException {
    final TransactionContext tx = database.getTransaction();
    final MutablePage headerPage = tx.getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    headerPage.writeByte(HEADER_COMPACTION_FLAG, (byte) (inProgress ? 1 : 0));
  }

  /**
   * Returns true if a compaction was in progress (crash recovery check).
   */
  public boolean isCompactionInProgress() throws IOException {
    final BasePage headerPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);
    return headerPage.readByte(HEADER_COMPACTION_FLAG) == 1;
  }

  /**
   * Gets the compaction watermark (sealed store file offset).
   */
  public long getCompactionWatermark() throws IOException {
    final BasePage headerPage = database.getTransaction().getPage(new PageId(database, fileId, 0), pageSize);
    return headerPage.readLong(HEADER_COMPACTION_WATERMARK);
  }

  /**
   * Sets the compaction watermark.
   */
  public void setCompactionWatermark(final long watermark) throws IOException {
    final TransactionContext tx = database.getTransaction();
    final MutablePage headerPage = tx.getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    headerPage.writeLong(HEADER_COMPACTION_WATERMARK, watermark);
  }

  /**
   * Returns all data from the bucket as parallel arrays for compaction.
   * First array is timestamps (long[]), rest are column values.
   */
  public Object[] readAllForCompaction() throws IOException {
    final List<Object[]> allRows = scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null);
    return allRows.isEmpty() ? null : rowsToCompactionArrays(allRows);
  }

  /**
   * Reads samples from data pages 1..toPage using the current transaction.
   * <p>
   * Pages 1..toPage must be FULL (immutable): once a data page is full it is never
   * modified by {@link #appendSamples}, which always writes to the LAST page. This
   * makes it safe to read them inside a short read-only transaction that is rolled
   * back immediately after, with no MVCC conflict with concurrent writers.
   *
   * @param toPage last data page to read (inclusive); must be ≥ 1
   *
   * @return parallel arrays [long[] timestamps, Object[] col1, ...], or null if empty
   */
  public Object[] readFullPagesForCompaction(final int toPage) throws IOException {
    return readPagesRangeForCompaction(1, toPage);
  }

  /**
   * Reads samples from data pages fromPage..toPage using the current transaction.
   * Used by Phase 4 of lock-free compaction (under write lock) to read the partial
   * last page(s) that arrived after the Phase 0 snapshot.
   *
   * @param fromPage first data page to read (inclusive, ≥ 1)
   * @param toPage   last data page to read (inclusive, ≥ fromPage)
   *
   * @return parallel arrays [long[] timestamps, Object[] col1, ...], or null if empty
   */
  public Object[] readPagesRangeForCompaction(final int fromPage, final int toPage) throws IOException {
    final List<Object[]> allRows = new ArrayList<>();
    for (int pageNum = fromPage; pageNum <= toPage; pageNum++) {
      final BasePage page = database.getTransaction().getPage(new PageId(database, fileId, pageNum), pageSize);
      final int sampleCount = page.readShort(DATA_SAMPLE_COUNT_OFFSET) & 0xFFFF;
      if (sampleCount == 0)
        continue;
      for (int row = 0; row < sampleCount; row++)
        allRows.add(readRow(page, DATA_ROWS_OFFSET + row * rowSize, null));
    }
    return allRows.isEmpty() ? null : rowsToCompactionArrays(allRows);
  }

  /**
   * Clears data pages 1..upToPage and recomputes header stats from the remaining pages.
   * Pages are physically kept for reuse but have their sample counts reset to 0.
   * Called by lock-free compaction to clear only the pages that were compacted,
   * leaving newer pages (upToPage+1..dataPageCount) intact.
   *
   * @param upToPage last page number to clear (inclusive); must be ≥ 1
   */
  public void clearDataPagesUpTo(final int upToPage) throws IOException {
    final TransactionContext tx = database.getTransaction();
    final MutablePage headerPage = tx.getPageToModify(new PageId(database, fileId, 0), pageSize, false);

    // Clear pages 1..upToPage
    for (int p = 1; p <= upToPage; p++) {
      final MutablePage dataPage = tx.getPageToModify(new PageId(database, fileId, p), pageSize, false);
      dataPage.writeShort(DATA_SAMPLE_COUNT_OFFSET, (short) 0);
      dataPage.writeLong(DATA_MIN_TS_OFFSET, Long.MAX_VALUE);
      dataPage.writeLong(DATA_MAX_TS_OFFSET, Long.MIN_VALUE);
    }

    // Recompute header stats from the remaining pages (upToPage+1..totalDataPages)
    final int totalDataPages = headerPage.readInt(HEADER_DATA_PAGE_COUNT);
    long sampleCount = 0;
    long minTs = Long.MAX_VALUE;
    long maxTs = Long.MIN_VALUE;
    for (int p = upToPage + 1; p <= totalDataPages; p++) {
      final BasePage page = tx.getPage(new PageId(database, fileId, p), pageSize);
      final int count = page.readShort(DATA_SAMPLE_COUNT_OFFSET) & 0xFFFF;
      if (count > 0) {
        sampleCount += count;
        final long pMin = page.readLong(DATA_MIN_TS_OFFSET);
        final long pMax = page.readLong(DATA_MAX_TS_OFFSET);
        if (pMin < minTs)
          minTs = pMin;
        if (pMax > maxTs)
          maxTs = pMax;
      }
    }
    headerPage.writeLong(HEADER_SAMPLE_COUNT_OFFSET, sampleCount);
    headerPage.writeLong(HEADER_MIN_TS_OFFSET, minTs);
    headerPage.writeLong(HEADER_MAX_TS_OFFSET, maxTs);
    // Keep HEADER_DATA_PAGE_COUNT unchanged so cleared pages can be reused by new inserts
  }

  /**
   * Clears all data pages after compaction.
   * O(1): only the header page is touched; physical data pages remain allocated on disk
   * and will be transparently reused as new samples arrive.
   * {@link #getOrCreateActiveDataPage} uses {@code HEADER_DATA_PAGE_COUNT} (not the physical
   * page count) to locate the current write position, so after this reset it starts from
   * page 1 again, reinitialising its sample-count field on the first write.
   */
  public void clearDataPages() throws IOException {
    final TransactionContext tx = database.getTransaction();
    final MutablePage headerPage = tx.getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    headerPage.writeLong(HEADER_SAMPLE_COUNT_OFFSET, 0L);
    headerPage.writeLong(HEADER_MIN_TS_OFFSET, Long.MAX_VALUE);
    headerPage.writeLong(HEADER_MAX_TS_OFFSET, Long.MIN_VALUE);
    headerPage.writeInt(HEADER_DATA_PAGE_COUNT, 0);
    // Physical pages are not touched: committing a single header page is O(1) regardless
    // of how many data pages were previously allocated, preventing OOM on large datasets.
  }

  public List<ColumnDefinition> getColumns() {
    return columns;
  }

  /**
   * Returns the fixed stride, in bytes, that one sample occupies in a data page. Note that a row is
   * <em>written</em> packed: a STRING column stores its actual bytes but reserves
   * {@code 2 + MAX_STRING_BYTES} in the stride, so on a tag-heavy schema the stride is much wider than
   * the row content (issue #5519).
   */
  public int getRowSize() {
    return rowSize;
  }

  /**
   * Returns the maximum number of samples that fit in one data page.
   */
  public int getMaxSamplesPerPage() {
    return (pageSize - BasePage.PAGE_HEADER_SIZE - DATA_ROWS_OFFSET) / rowSize;
  }

  // --- Private helpers ---

  void initHeaderPage() throws IOException {
    final TransactionContext tx = database.getTransaction();
    final MutablePage headerPage = tx.addPage(new PageId(database, fileId, 0), pageSize);
    headerPage.writeInt(HEADER_MAGIC_OFFSET, MAGIC_VALUE);
    headerPage.writeByte(HEADER_FORMAT_VERSION_OFFSET, (byte) CURRENT_VERSION);
    headerPage.writeShort(HEADER_COLUMN_COUNT_OFFSET, (short) columns.size());
    headerPage.writeLong(HEADER_SAMPLE_COUNT_OFFSET, 0L);
    headerPage.writeLong(HEADER_MIN_TS_OFFSET, Long.MAX_VALUE);
    headerPage.writeLong(HEADER_MAX_TS_OFFSET, Long.MIN_VALUE);
    headerPage.writeByte(HEADER_COMPACTION_FLAG, (byte) 0);
    headerPage.writeLong(HEADER_COMPACTION_WATERMARK, 0L);
    headerPage.writeInt(HEADER_DATA_PAGE_COUNT, 0);
    pageCount.set(1);
  }

  private MutablePage getOrCreateActiveDataPage(final TransactionContext tx, final MutablePage headerPage) throws IOException {
    // Use the logical page count from the header, NOT getTotalPages() (physical).
    // After clearDataPages() resets HEADER_DATA_PAGE_COUNT to 0, the physical pages
    // still exist on disk; we transparently reuse them starting from page 1, avoiding
    // allocating new pages and avoiding wasted space.
    final int dataPageCount = headerPage.readInt(HEADER_DATA_PAGE_COUNT);

    if (dataPageCount > 0) {
      // Check if the last logical data page has room
      final MutablePage lastPage = tx.getPageToModify(new PageId(database, fileId, dataPageCount), pageSize, false);
      final int sampleCount = lastPage.readShort(DATA_SAMPLE_COUNT_OFFSET) & 0xFFFF;
      if (sampleCount < getMaxSamplesPerPage())
        return lastPage;
    }

    // Need a new (or reused) data page
    final int newPageNum = dataPageCount + 1;
    final MutablePage newPage;
    if (newPageNum < getTotalPages()) {
      // Physical page already exists — reuse it (typical after compaction clears the header)
      newPage = tx.getPageToModify(new PageId(database, fileId, newPageNum), pageSize, false);
    } else {
      // Physical page does not yet exist — allocate it
      newPage = tx.addPage(new PageId(database, fileId, newPageNum), pageSize);
      pageCount.incrementAndGet();
    }
    // Initialise the page (old data bytes beyond sample-count are ignored by readers)
    newPage.writeShort(DATA_SAMPLE_COUNT_OFFSET, (short) 0);
    newPage.writeLong(DATA_MIN_TS_OFFSET, Long.MAX_VALUE);
    newPage.writeLong(DATA_MAX_TS_OFFSET, Long.MIN_VALUE);

    headerPage.writeInt(HEADER_DATA_PAGE_COUNT, newPageNum);
    return newPage;
  }

  /**
   * Writes the raw bits of a fixed-width column in exactly the {@code fixedSize} bytes
   * {@link ColumnDefinition#getFixedSize()} reserves in the row stride, and returns the bytes written.
   * The two must agree: a column that wrote more than it reserved used to overrun its neighbours in
   * the row (issue #5475).
   */
  private static int writeRawValue(final MutablePage page, final int offset, final int fixedSize, final long raw) {
    switch (fixedSize) {
    case 8 -> page.writeLong(offset, raw);
    case 4 -> page.writeInt(offset, (int) raw);
    case 2 -> page.writeShort(offset, (short) raw);
    case 1 -> page.writeByte(offset, (byte) raw);
    default -> throw new IllegalStateException("Unsupported fixed column width " + fixedSize);
    }
    return fixedSize;
  }

  /**
   * Reads back the raw bits {@link #writeRawValue} stored for a fixed-width column.
   */
  private static long readRawValue(final BasePage page, final int offset, final int fixedSize) {
    return switch (fixedSize) {
      case 8 -> page.readLong(offset);
      case 4 -> page.readInt(offset);
      case 2 -> page.readShort(offset);
      case 1 -> page.readByte(offset);
      default -> throw new IllegalStateException("Unsupported fixed column width " + fixedSize);
    };
  }

  /**
   * Writes a STRING column in the mutable layer as a length-prefixed UTF-8 payload and returns the
   * bytes written. {@code null} is stored as a zero-length payload.
   */
  private static int writeStringValue(final MutablePage page, final int offset, final ColumnDefinition col, final String value) {
    final byte[] bytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
    if (bytes.length > MAX_STRING_BYTES)
      throw new IllegalArgumentException(
          "String value exceeds max length of " + MAX_STRING_BYTES + " bytes for column '" + col.getName() + "'");
    page.writeShort(offset, (short) bytes.length);
    if (bytes.length > 0)
      page.writeByteArray(offset + 2, bytes);
    return 2 + bytes.length;
  }

  private Object[] readRow(final BasePage page, final int rowOffset, final int[] columnIndices) {
    // First element is always the timestamp
    final int resultSize = columnIndices != null ? columnIndices.length + 1 : columns.size();
    final Object[] result = new Object[resultSize];
    result[0] = page.readLong(rowOffset);

    if (columnIndices == null) {
      // Read all columns
      int colOffset = rowOffset + 8;
      int colIdx = 0;
      for (int c = 0; c < columns.size(); c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;
        result[colIdx + 1] = readColumnValue(page, colOffset, columns.get(c));
        colOffset += getColumnStorageSize(page, colOffset, columns.get(c));
        colIdx++;
      }
    } else {
      // Read specific columns by index
      int colOffset = rowOffset + 8;
      int colIdx = 0;
      int resultIdx = 1;
      for (int c = 0; c < columns.size(); c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;
        if (isInArray(colIdx, columnIndices)) {
          result[resultIdx++] = readColumnValue(page, colOffset, columns.get(c));
        }
        colOffset += getColumnStorageSize(page, colOffset, columns.get(c));
        colIdx++;
      }
    }
    return result;
  }

  /**
   * Reads one column value, mirroring {@link #writeRawValue}/{@link #writeStringValue} exactly: same
   * width table on the way out as on the way in, and the boxing delegated to
   * {@link ColumnDefinition#boxRaw(long)} so the mutable and sealed layers cannot return different
   * Java types for the same declared column (issue #5475).
   */
  private Object readColumnValue(final BasePage page, final int offset, final ColumnDefinition col) {
    final int fixedSize = col.getFixedSize();
    if (fixedSize > 0)
      return col.boxRaw(readRawValue(page, offset, fixedSize));

    final int len = page.readShort(offset) & 0xFFFF;
    if (len == 0)
      return col.getDataType() == Type.STRING ? "" : null;

    final byte[] bytes = new byte[len];
    page.readByteArray(offset + 2, bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private int getColumnStorageSize(final BasePage page, final int offset, final ColumnDefinition col) {
    final int fixed = col.getFixedSize();
    if (fixed > 0)
      return fixed;
    // STRING: 2-byte length prefix + data
    return 2 + (page.readShort(offset) & 0xFFFF);
  }

  private static int calculateRowSize(final List<ColumnDefinition> columns) {
    int size = 8; // timestamp (always 8 bytes)
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      final int fixed = col.getFixedSize();
      if (fixed > 0)
        size += fixed;
      else
        size += 2 + MAX_STRING_BYTES; // max STRING: 2-byte length prefix + max payload
    }
    return size;
  }

  private static boolean isInArray(final int value, final int[] array) {
    for (final int v : array)
      if (v == value)
        return true;
    return false;
  }

  /**
   * Converts a list of sample rows into the parallel-array format expected by compaction.
   * First element of the returned array is long[] timestamps; subsequent elements are
   * Object[] column value arrays, one per non-timestamp column.
   */
  private Object[] rowsToCompactionArrays(final List<Object[]> allRows) {
    final int size = allRows.size();
    final int totalCols = columns.size();
    final long[] timestamps = new long[size];
    final Object[][] colArrays = new Object[totalCols - 1][];

    int colIdx = 0;
    for (int c = 0; c < totalCols; c++) {
      if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      colArrays[colIdx] = new Object[size];
      colIdx++;
    }

    for (int i = 0; i < size; i++) {
      final Object[] row = allRows.get(i);
      timestamps[i] = (long) row[0];
      for (int c = 1; c < row.length; c++)
        colArrays[c - 1][i] = row[c];
    }

    final Object[] result = new Object[totalCols];
    result[0] = timestamps;
    int idx = 1;
    for (final Object[] colArray : colArrays)
      result[idx++] = colArray;
    return result;
  }
}
