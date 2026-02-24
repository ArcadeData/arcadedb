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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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
        database.getConfiguration().getValueAsInteger(com.arcadedb.GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE), CURRENT_VERSION);
    this.columns = columns;
    this.rowSize = calculateRowSize(columns);
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
        database.getConfiguration().getValueAsInteger(com.arcadedb.GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE), CURRENT_VERSION);
    this.columns = columns;
    this.rowSize = calculateRowSize(columns);
  }

  /**
   * Sets column definitions (called during cold open after the factory handler creates a stub bucket).
   */
  public void setColumns(final List<ColumnDefinition> columns) {
    this.columns = columns;
    this.rowSize = calculateRowSize(columns);
  }

  /**
   * Appends samples to the mutable bucket within the current transaction.
   *
   * @param timestamps array of timestamps (millisecond epoch)
   * @param columnValues array of column value arrays, one per non-timestamp column
   */
  public void appendSamples(final long[] timestamps, final Object[]... columnValues) throws IOException {
    final TransactionContext tx = database.getTransaction();

    for (int i = 0; i < timestamps.length; i++) {
      final MutablePage dataPage = getOrCreateActiveDataPage(tx);

      final int sampleCountInPage = dataPage.readShort(DATA_SAMPLE_COUNT_OFFSET) & 0xFFFF;
      final int rowOffset = DATA_ROWS_OFFSET + sampleCountInPage * rowSize;

      // Write timestamp
      dataPage.writeLong(rowOffset, timestamps[i]);

      // Write each non-timestamp column value
      int colOffset = rowOffset + 8;
      int colIdx = 0;
      for (int c = 0; c < columns.size(); c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;

        final Object value = columnValues[colIdx][i];
        colOffset += writeColumnValue(dataPage, colOffset, columns.get(c), value);
        colIdx++;
      }

      // Update page sample count and min/max timestamps
      dataPage.writeShort(DATA_SAMPLE_COUNT_OFFSET, (short) (sampleCountInPage + 1));

      final long currentMinTs = dataPage.readLong(DATA_MIN_TS_OFFSET);
      final long currentMaxTs = dataPage.readLong(DATA_MAX_TS_OFFSET);

      if (sampleCountInPage == 0 || timestamps[i] < currentMinTs)
        dataPage.writeLong(DATA_MIN_TS_OFFSET, timestamps[i]);
      if (sampleCountInPage == 0 || timestamps[i] > currentMaxTs)
        dataPage.writeLong(DATA_MAX_TS_OFFSET, timestamps[i]);

      // Update header page stats
      updateHeaderStats(tx, timestamps[i]);
    }
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
      return java.util.Collections.emptyIterator();

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
          throw new com.arcadedb.exception.DatabaseOperationException("Error iterating TimeSeries bucket pages", e);
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

  private MutablePage getOrCreateActiveDataPage(final TransactionContext tx) throws IOException {
    // Use the logical page count from the header, NOT getTotalPages() (physical).
    // After clearDataPages() resets HEADER_DATA_PAGE_COUNT to 0, the physical pages
    // still exist on disk; we transparently reuse them starting from page 1, avoiding
    // allocating new pages and avoiding wasted space.
    final MutablePage headerPage = tx.getPageToModify(new PageId(database, fileId, 0), pageSize, false);
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

  private void updateHeaderStats(final TransactionContext tx, final long timestamp) throws IOException {
    final MutablePage headerPage = tx.getPageToModify(new PageId(database, fileId, 0), pageSize, false);
    final long count = headerPage.readLong(HEADER_SAMPLE_COUNT_OFFSET);
    headerPage.writeLong(HEADER_SAMPLE_COUNT_OFFSET, count + 1);

    final long currentMin = headerPage.readLong(HEADER_MIN_TS_OFFSET);
    final long currentMax = headerPage.readLong(HEADER_MAX_TS_OFFSET);
    if (timestamp < currentMin)
      headerPage.writeLong(HEADER_MIN_TS_OFFSET, timestamp);
    if (timestamp > currentMax)
      headerPage.writeLong(HEADER_MAX_TS_OFFSET, timestamp);
  }

  private int writeColumnValue(final MutablePage page, final int offset, final ColumnDefinition col, final Object value) {
    return switch (col.getDataType()) {
      case DOUBLE -> {
        page.writeLong(offset, Double.doubleToRawLongBits(value != null ? ((Number) value).doubleValue() : 0.0));
        yield 8;
      }
      case LONG, DATETIME -> {
        page.writeLong(offset, value != null ? ((Number) value).longValue() : 0L);
        yield 8;
      }
      case INTEGER -> {
        page.writeInt(offset, value != null ? ((Number) value).intValue() : 0);
        yield 4;
      }
      case FLOAT -> {
        page.writeInt(offset, Float.floatToRawIntBits(value != null ? ((Number) value).floatValue() : 0f));
        yield 4;
      }
      case SHORT -> {
        page.writeShort(offset, value != null ? ((Number) value).shortValue() : (short) 0);
        yield 2;
      }
      case BOOLEAN -> {
        page.writeByte(offset, (byte) (Boolean.TRUE.equals(value) ? 1 : 0));
        yield 1;
      }
      case STRING -> {
        // For strings in mutable layer, store length-prefixed UTF-8
        final byte[] bytes = value != null ? ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];
        if (bytes.length > MAX_STRING_BYTES)
          throw new IllegalArgumentException(
              "String value exceeds max length of " + MAX_STRING_BYTES + " bytes for column '" + col.getName() + "'");
        page.writeShort(offset, (short) bytes.length);
        if (bytes.length > 0)
          page.writeByteArray(offset + 2, bytes);
        yield 2 + bytes.length;
      }
      default -> {
        page.writeLong(offset, 0L);
        yield 8;
      }
    };
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

  private Object readColumnValue(final BasePage page, final int offset, final ColumnDefinition col) {
    return switch (col.getDataType()) {
      case DOUBLE -> Double.longBitsToDouble(page.readLong(offset));
      case LONG, DATETIME -> page.readLong(offset);
      case INTEGER -> page.readInt(offset);
      case FLOAT -> Float.intBitsToFloat(page.readInt(offset));
      case SHORT -> page.readShort(offset);
      case BOOLEAN -> page.readByte(offset) == 1;
      case STRING -> {
        final int len = page.readShort(offset) & 0xFFFF;
        if (len == 0)
          yield "";
        final byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++)
          bytes[i] = (byte) page.readByte(offset + 2 + i);
        yield new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
      }
      default -> null;
    };
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
