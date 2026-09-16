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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.Type;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7711: {@code TimeSeriesSealedStore.downsampleBlocks} decompressed the tag columns of the blocks it was
 * about to rewrite through a codec switch whose only case was {@code DICTIONARY}. Its {@code default} arm did not
 * decode the column, it DISCARDED it - yielding an array of nulls, which the grouping key a few lines down turned
 * into the empty string.
 * <p>
 * The rows were then gone rather than mislabelled: every distinct value of that tag merged into one group, the
 * rewritten blocks carried {@code ""} where the tag value had been, and the recomputed distinct-value declaration
 * was built from those same rewritten rows - so it agreed with the damage and nothing downstream could see it.
 * <p>
 * A TAG column defaults to {@code DICTIONARY} ({@code ColumnDefinition.defaultCodecFor}), so reaching this needs
 * a column given an EXPLICIT codec, which {@code TimeSeriesTypeBuilder.withColumn(ColumnDefinition)} allows since
 * #7399. That makes it latent, but it is latent DATA LOSS on a background maintenance path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7711DownsampleNonDictionaryTagTest extends TestHelper {

  /** One minute of one-second samples, downsampled into a single one-minute bucket per tag value. */
  private static final int  SAMPLE_COUNT   = 60;
  private static final long GRANULARITY_MS = 60_000L;

  /**
   * An INTEGER tag stored with {@code SIMPLE8B}: the shape the issue names. Two zones interleaved sample by
   * sample must come out of downsampling as two series, not as one series tagged with the empty string.
   */
  @Test
  void keepsTheSeriesOfASimple8bTagColumnSeparate() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("zone", Type.INTEGER, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.SIMPLE8B),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    final TimeSeriesEngine engine = sealOneBlock("ds_simple8b_tag", columns, i -> (i % 2) + 1);
    try {
      final List<Object[]> downsampled = downsample(engine);

      // Two zones in, two zones out. Before the fix this was ONE row, tagged "".
      assertThat(downsampled).hasSize(2);
      assertThat(distinctTagValues(downsampled)).containsExactlyInAnyOrder(1, 2);

      for (final Object[] row : downsampled) {
        assertThat((long) row[0]).as("both series fall in the same one-minute bucket").isZero();
        // zone 1 carries the even-indexed samples (1, 3, ... 59), zone 2 the odd-indexed ones (2, 4, ... 60).
        final double expected = (int) row[1] == 1 ? 30.0 : 31.0;
        assertThat((double) row[2]).isCloseTo(expected, Offset.offset(0.001));
      }

      assertThat(engine.getShard(0).getSealedStore().checkIntegrity())
          .as("the rewritten blocks must declare the tag values they actually hold").isEmpty();
    } finally {
      engine.close();
    }
  }

  /**
   * The same defect through the other numeric codec a tag column can be given, so the fix is the codec table
   * rather than one extra case: {@code GORILLA_XOR} on a DOUBLE tag.
   */
  @Test
  void keepsTheSeriesOfAGorillaTagColumnSeparate() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("zone", Type.DOUBLE, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.GORILLA_XOR),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    final TimeSeriesEngine engine = sealOneBlock("ds_gorilla_tag", columns, i -> (double) ((i % 2) + 1));
    try {
      final List<Object[]> downsampled = downsample(engine);

      assertThat(downsampled).hasSize(2);
      assertThat(distinctTagValues(downsampled)).containsExactlyInAnyOrder(1.0, 2.0);
    } finally {
      engine.close();
    }
  }

  /**
   * The control: a tag column on its DEFAULT codec is what every existing downsampling test uses, and it must
   * keep answering exactly as it did. This is the case the old switch handled, so a regression here would mean
   * the shared decoder boxes a dictionary value differently from the copy it replaced.
   */
  @Test
  void leavesADictionaryTagColumnAnswering() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("zone", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    final TimeSeriesEngine engine = sealOneBlock("ds_dictionary_tag", columns, i -> "zone_" + (i % 2));
    try {
      final List<Object[]> downsampled = downsample(engine);

      assertThat(downsampled).hasSize(2);
      assertThat(distinctTagValues(downsampled)).containsExactlyInAnyOrder("zone_0", "zone_1");
    } finally {
      engine.close();
    }
  }

  /** The tag value of sample {@code i}. */
  private interface TagValues {
    Object at(int i);
  }

  /**
   * Appends {@link #SAMPLE_COUNT} samples one second apart and compacts them into a single sealed block, which is
   * what downsampling reads. {@code temperature} is {@code i + 1}, so each tag's average is known in advance.
   */
  private TimeSeriesEngine sealOneBlock(final String typeName, final List<ColumnDefinition> columns,
      final TagValues tags) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, typeName, columns, 1);

    final long[] timestamps = new long[SAMPLE_COUNT];
    final Object[] zones = new Object[SAMPLE_COUNT];
    final Object[] temperatures = new Object[SAMPLE_COUNT];
    for (int i = 0; i < SAMPLE_COUNT; i++) {
      timestamps[i] = i * 1000L;
      zones[i] = tags.at(i);
      temperatures[i] = (double) (i + 1);
    }
    engine.appendSamples(timestamps, zones, temperatures);
    database.commit();

    database.begin();
    engine.compactAll();
    database.commit();

    assertThat(engine.getShard(0).getSealedStore().getBlockCount())
        .as("the samples have to be SEALED for downsampling to see them").isEqualTo(1);
    return engine;
  }

  /** Downsamples everything to one bucket per minute and reads the result back. */
  private List<Object[]> downsample(final TimeSeriesEngine engine) throws Exception {
    engine.applyDownsampling(List.of(new DownsamplingTier(1L, GRANULARITY_MS)), SAMPLE_COUNT * 1000L + 1L);

    database.begin();
    try {
      return engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    } finally {
      database.commit();
    }
  }

  /** The tag values the rows carry, as the read path hands them back. */
  private static Set<Object> distinctTagValues(final List<Object[]> rows) {
    final Set<Object> values = new LinkedHashSet<>();
    for (final Object[] row : rows)
      values.add(row[1]);
    return values;
  }
}
