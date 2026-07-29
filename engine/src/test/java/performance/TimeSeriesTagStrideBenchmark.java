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
package performance;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.timeseries.TimeSeriesBucket;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Isolates what a tag-heavy TimeSeries schema actually pays on ingest (issue #5519).
 * <p>
 * {@code TimeSeriesBucket.calculateRowSize()} reserves {@code 2 + MAX_STRING_BYTES} = 258 bytes of row
 * stride for every STRING column, while {@code writeStringValue()} stores the row packed. Only the
 * stride assumes the maximum, and the stride is what positions the row inside the page, so short tag
 * values are spread across a page that is almost entirely unwritten padding.
 * <p>
 * Three arms, each appending the same number of rows, differing in exactly one thing:
 * <ol>
 *   <li>{@code WideStride} - the TSBS cpu-only shape: 10 STRING tags of ~8 chars plus 3 DOUBLE fields.</li>
 *   <li>{@code WideStrideBigValues} - the same schema with 200-char tag values. Twenty times more string
 *       bytes are encoded and copied per row, and the stride is unchanged. Isolates encoding cost.</li>
 *   <li>{@code NarrowStride} - the same tag text joined into one STRING column. Identical content, one
 *       ninth the stride. Isolates page traffic.</li>
 * </ol>
 * Arm 2 costs about the same as arm 1 and arm 3 is several times faster, which is the finding: on a
 * tag-heavy schema the ingest is paying for pages, not for encoding. Backs the numbers quoted on #5519
 * and on issue #5474; when the tag dictionary of #5519 lands, arm 1 should converge towards arm 3.
 * <p>
 * Run explicitly with
 * {@code ./mvnw -pl engine -Dtest=TimeSeriesTagStrideBenchmark -Dgroups=benchmark test}.
 * Override the batch size with {@code -Darcadedb.tagStrideBenchmark.rows=50000} to reproduce the batch
 * size quoted on those issues. The default is deliberately smaller: at the default 64 KB page size a
 * wide-stride arm writes {@code rows / 25} pages per batch, so 50k rows costs roughly 1 GB under
 * {@code target/} across the three arms.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class TimeSeriesTagStrideBenchmark extends TestHelper {
  private static final String   ROWS_PROPERTY = "arcadedb.tagStrideBenchmark.rows";
  private static final int      DEFAULT_ROWS  = 20_000;
  private static final int      WARMUP        = 1;
  private static final int      MEASURED      = 3;
  private static final long     BASE_TS       = 1_700_000_000_000L;
  private static final String[] TSBS_TAGS     = { "host_42", "us-west-2", "us-west-2b", "87", "Ubuntu16.10", "x64", "NYC",
      "19", "1", "production" };
  private static final String[] TSBS_TAG_NAMES = { "hostname", "region", "datacenter", "rack", "os", "arch", "team",
      "service", "service_version", "service_environment" };

  @Test
  void tagStrideDominatesIngestOnTagHeavySchemas() throws IOException {
    final int rows = Integer.getInteger(ROWS_PROPERTY, DEFAULT_ROWS);

    final Arm wide = new Arm("WideStride", tenTagColumns(), tenTagColumnValues(TSBS_TAGS, rows), rows);
    final Arm wideBig = new Arm("WideStrideBigValues", tenTagColumns(), tenTagColumnValues(bigValues(), rows), rows);
    final Arm narrow = new Arm("NarrowStride", "series STRING", oneTagColumnValues(String.join(",", TSBS_TAGS), rows), rows);
    final Arm[] arms = { wide, wideBig, narrow };

    // One full warmup pass over every arm before any arm is measured, so no arm profits from the JIT
    // state another arm left behind.
    for (final Arm arm : arms)
      for (int i = 0; i < WARMUP; i++)
        arm.appendOneBatch();

    for (final Arm arm : arms)
      for (int i = 0; i < MEASURED; i++)
        arm.measureOneBatch();

    System.out.printf(Locale.ROOT, "%n=== TimeSeries tag stride, %,d rows per batch, %d measured batches per arm ===%n",
        rows, MEASURED);
    System.out.printf(Locale.ROOT, "%-22s %8s %11s %10s %12s %18s %8s%n", "arm", "stride", "rows/page", "pages", "payload",
        "page traffic", "ms");
    for (final Arm arm : arms)
      arm.report();
    System.out.println();

    // The finding, stated as the two orderings that survive re-running on other hardware.

    // Twenty times more string bytes per row, same stride: encoding is not what ingest is paying for.
    assertThat(wideBig.medianNanos()).isLessThan(wide.medianNanos() * 2);

    // Same tag content, one ninth of the stride: page traffic is.
    assertThat(narrow.medianNanos() * 2).isLessThan(wide.medianNanos());

    // Structural rather than timing-based, so these hold on any host: a page holds an order of
    // magnitude fewer rows once ten tag columns each reserve 258 bytes they do not use.
    assertThat(wide.rowsPerPage()).isEqualTo(wideBig.rowsPerPage());
    assertThat(wide.rowsPerPage() * 8).isLessThan(narrow.rowsPerPage());
    assertThat(wide.pageTrafficAmplification()).isGreaterThan(10);
  }

  private static String tenTagColumns() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < TSBS_TAG_NAMES.length; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(TSBS_TAG_NAMES[i]).append(" STRING");
    }
    return sb.toString();
  }

  private static String[] bigValues() {
    final String big = "x".repeat(200);
    final String[] values = new String[TSBS_TAGS.length];
    Arrays.fill(values, big);
    return values;
  }

  /**
   * Ten tag columns plus the three DOUBLE fields, laid out as the boxed column arrays
   * {@code appendBatch} takes: {@code values[column][row]}.
   */
  private static Object[][] tenTagColumnValues(final String[] tagValues, final int rows) {
    final Object[][] columns = new Object[TSBS_TAGS.length + 3][rows];
    for (int row = 0; row < rows; row++) {
      for (int tag = 0; tag < tagValues.length; tag++)
        columns[tag][row] = tagValues[tag];
      for (int field = tagValues.length; field < columns.length; field++)
        columns[field][row] = (double) row;
    }
    return columns;
  }

  private static Object[][] oneTagColumnValues(final String tagValue, final int rows) {
    final Object[][] columns = new Object[4][rows];
    for (int row = 0; row < rows; row++) {
      columns[0][row] = tagValue;
      for (int field = 1; field < columns.length; field++)
        columns[field][row] = (double) row;
    }
    return columns;
  }

  /**
   * One schema shape under test. Each batch appends {@code rows} fresh samples with monotonically
   * increasing timestamps, so every batch pays the same steady-state per-row cost.
   */
  private final class Arm {
    private final String          name;
    private final TimeSeriesEngine engine;
    private final TimeSeriesBucket bucket;
    private final Object[][]      columnValues;
    private final int             rows;
    private final long[]          nanos  = new long[MEASURED];
    private final long            payloadBytesPerBatch;
    private       long            nextTimestamp = BASE_TS;
    private       int             measurements  = 0;

    private Arm(final String name, final String tagColumns, final Object[][] columnValues, final int rows) {
      this.name = name;
      this.columnValues = columnValues;
      this.rows = rows;

      database.command("sql", "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts TAGS (" + tagColumns + ") "
          + "FIELDS (usage_user DOUBLE, usage_system DOUBLE, usage_idle DOUBLE) SHARDS 1");
      this.engine = ((LocalTimeSeriesType) database.getSchema().getType(name)).getEngine();
      this.bucket = engine.getShard(0).getMutableBucket();

      // What the rows would occupy if the stride were not padded: the timestamp, the fixed-width
      // fields, and each tag's length prefix plus its actual UTF-8 bytes.
      long payload = 8L;
      for (final Object[] column : columnValues)
        payload += column[0] instanceof String s ? 2 + s.getBytes(StandardCharsets.UTF_8).length : 8;
      this.payloadBytesPerBatch = payload * rows;
    }

    private void appendOneBatch() throws IOException {
      final long[] timestamps = new long[rows];
      for (int i = 0; i < rows; i++)
        timestamps[i] = nextTimestamp + i * 10_000L;
      nextTimestamp = timestamps[rows - 1] + 10_000L;
      engine.appendBatch(timestamps, columnValues);
    }

    private void measureOneBatch() throws IOException {
      final long begin = System.nanoTime();
      appendOneBatch();
      nanos[measurements++] = System.nanoTime() - begin;
    }

    private long medianNanos() {
      final long[] sorted = Arrays.copyOf(nanos, measurements);
      Arrays.sort(sorted);
      return sorted[sorted.length / 2];
    }

    private int rowsPerPage() {
      return bucket.getMaxSamplesPerPage();
    }

    private long pagesPerBatch() {
      return (rows + rowsPerPage() - 1L) / rowsPerPage();
    }

    private long pageTrafficBytesPerBatch() {
      return pagesPerBatch() * bucket.getPageSize();
    }

    /**
     * How many bytes of page are moved, through the buffer pool and through the WAL, for every byte of
     * sample data actually stored. The WAL ships the whole used region of each page because
     * {@code MutablePage.MAX_MODIFIED_RANGES} is 8 and the row writes are scattered at stride distance.
     */
    private long pageTrafficAmplification() {
      return pageTrafficBytesPerBatch() / payloadBytesPerBatch;
    }

    private void report() {
      System.out.printf(Locale.ROOT, "%-22s %8d %11d %10d %12s %18s %8.1f%n", name, bucket.getRowSize(), rowsPerPage(),
          pagesPerBatch(), format(payloadBytesPerBatch),
          format(pageTrafficBytesPerBatch()) + " (" + pageTrafficAmplification() + "x)", medianNanos() / 1_000_000.0);
    }

    private String format(final long bytes) {
      return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
    }
  }
}
