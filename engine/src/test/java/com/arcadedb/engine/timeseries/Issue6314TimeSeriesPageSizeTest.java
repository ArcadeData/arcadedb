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
import com.arcadedb.TestHelper;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6314 (item 1): both TimeSeries factory handlers accepted the page size and the version that
 * {@code ComponentFactory} had parsed out of the component's own file name and threw them away, re-deriving the
 * page size from the <em>live</em> {@code arcadedb.bucketDefaultPageSize} instead and hard-coding the version.
 * <p>
 * The setting is {@code SCOPE.DATABASE} and user-settable, so no bug was needed to trigger it: change it between
 * two runs and every {@code .tstb}/{@code .tstd} file reopens at a stride that is not the one it was written
 * with. Nothing downstream catches that - {@code PageManager} resolves a page with the <em>caller's</em> page
 * size, i.e. the component's - so page N is read from {@code N * wrongStride} and what comes back is real bytes
 * at the wrong offset rather than an exception. {@code pageCount = fileSize / pageSize} is off by the same
 * factor.
 * <p>
 * The values are now passed through the way {@code LocalBucket}'s handler always has, and
 * {@code PaginatedComponent} asserts the agreement between a component and the file it holds on the page size
 * exactly as issue #6283 made it assert it on the file id.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6314TimeSeriesPageSizeTest extends TestHelper {

  private static final int CREATE_PAGE_SIZE = 65_536;
  private static final int REOPEN_PAGE_SIZE = 16_384;
  private static final int ROWS             = 3_000;

  private int savedPageSize;

  /**
   * Save and restore around every test, the way the other page-size-sensitive tests do (e.g.
   * {@code EdgeAppendMergeMultiPageChunkTest}). {@code TestHelper.afterTest()} resets the whole configuration
   * regardless of outcome, so this is not what stops the setting leaking into the next class - it is what stops the
   * restore from depending on a test body reaching its last line, and {@code endTest()} runs BEFORE the integrity
   * check and the drop, so those see the default stride rather than whatever arm the test left behind.
   */
  @Override
  protected void beginTest() {
    savedPageSize = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger();
  }

  @Override
  protected void endTest() {
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(savedPageSize);
  }

  private TimeSeriesEngine createType(final String typeName) {
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private void appendRows(final TimeSeriesEngine engine, final int rows) throws IOException {
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[2][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % 7);
      columns[1][i] = (double) i;
    }
    engine.appendBatch(timestamps, columns);
  }

  /**
   * The end-to-end shape the issue asked for: write under one {@code bucketDefaultPageSize}, reopen under
   * another. Before the fix the reopened bucket addressed its pages at 16 KB while the file on disk was written
   * at 64 KB, so the header page - which starts at offset 0 and therefore still read correctly - announced data
   * pages that were then read from the padding inside real page 0, and the query came back empty.
   */
  @Test
  void aTimeSeriesTypeReopensAtTheStrideItsFileWasWrittenWith() throws Exception {
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(CREATE_PAGE_SIZE);

    final TimeSeriesEngine engine = createType("Cpu");
    appendRows(engine, ROWS);
    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(ROWS);
    assertThat(engine.getShard(0).getMutableBucket().getPageSize()).isEqualTo(CREATE_PAGE_SIZE);

    // The only thing that changes between the two runs, and it is a supported thing to change.
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(REOPEN_PAGE_SIZE);
    reopenDatabase();

    final TimeSeriesEngine reopened = ((LocalTimeSeriesType) database.getSchema().getType("Cpu")).getEngine();
    final TimeSeriesBucket bucket = reopened.getShard(0).getMutableBucket();

    // The component takes the stride from the file it holds, not from whatever the configuration says today.
    assertThat(bucket.getPageSize()).isEqualTo(CREATE_PAGE_SIZE);
    assertThat(bucket.getPageSize()).isEqualTo(bucket.getComponentFile().getPageSize());

    final TimeSeriesTagDictionary dictionary = reopened.getTagDictionary();
    assertThat(dictionary).isNotNull();
    assertThat(dictionary.getPageSize()).isEqualTo(CREATE_PAGE_SIZE);
    assertThat(dictionary.getPageSize()).isEqualTo(dictionary.getComponentFile().getPageSize());

    // And the data is all still there, read from the offsets it was written at.
    final List<Object[]> rows = reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(ROWS);
    rows.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
    for (int i = 0; i < ROWS; i++) {
      assertThat(rows.get(i)[1]).isEqualTo("host_" + (i % 7));
      assertThat(rows.get(i)[2]).isEqualTo((double) i);
    }

    // Appending after the reopen keeps writing at the file's own stride.
    appendRows(reopened, 500);
    assertThat(reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(ROWS + 500);
  }

  /**
   * The new {@code PaginatedComponent} guard does NOT turn a database written under a different
   * {@code bucketDefaultPageSize} into one that refuses to open - which is the natural thing to assume about a
   * guard added in the same change, and it is worth pinning rather than leaving to be re-derived.
   * <p>
   * The guard compares the component against the file it holds, and every load path now takes the page size FROM
   * that file, so the two agree by construction whatever the configuration happens to say. What the guard catches
   * is a component that re-derived the value from somewhere else, i.e. the defect this change removes - it is a
   * programming-error tripwire, not a compatibility gate. A database carrying files at three different strides at
   * once opens all of them, each at its own.
   */
  @Test
  void reopeningUnderAnyConfiguredPageSizeSucceedsWhateverTheFilesWereWrittenAt() throws Exception {
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(CREATE_PAGE_SIZE);
    appendRows(createType("Wide"), 200);

    // A second type created at a different stride, so the database now holds files written at BOTH.
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(REOPEN_PAGE_SIZE);
    appendRows(createType("Narrow"), 200);

    // ...and reopened under a third value that matches neither.
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(32_768);
    reopenDatabase();

    final TimeSeriesEngine wide = ((LocalTimeSeriesType) database.getSchema().getType("Wide")).getEngine();
    final TimeSeriesEngine narrow = ((LocalTimeSeriesType) database.getSchema().getType("Narrow")).getEngine();

    assertThat(wide.getShard(0).getMutableBucket().getPageSize()).isEqualTo(CREATE_PAGE_SIZE);
    assertThat(narrow.getShard(0).getMutableBucket().getPageSize()).isEqualTo(REOPEN_PAGE_SIZE);
    assertThat(wide.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(200);
    assertThat(narrow.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(200);
  }

  /**
   * The other half of the pass-through: the component must report the version its file name carries rather than
   * claiming {@code CURRENT_VERSION} for a file that says otherwise. A type created by this build is at
   * {@code CURRENT_VERSION}, and the point of pinning it here is that the value now comes from the file.
   */
  @Test
  void aReopenedComponentReportsTheVersionItsFileNameCarries() throws Exception {
    final TimeSeriesEngine engine = createType("Sensor");
    appendRows(engine, 100);

    reopenDatabase();

    final TimeSeriesEngine reopened = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();
    final TimeSeriesBucket bucket = reopened.getShard(0).getMutableBucket();
    assertThat(bucket.getVersion()).isEqualTo(bucket.getComponentFile().getVersion());
    assertThat(bucket.getVersion()).isEqualTo(TimeSeriesBucket.CURRENT_VERSION);

    final TimeSeriesTagDictionary dictionary = reopened.getTagDictionary();
    assertThat(dictionary.getVersion()).isEqualTo(dictionary.getComponentFile().getVersion());
    assertThat(dictionary.getVersion()).isEqualTo(TimeSeriesTagDictionary.CURRENT_VERSION);
  }
}
