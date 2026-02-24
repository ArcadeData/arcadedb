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
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that DictionaryCodec overflow (>65,535 distinct tag values) fails cleanly
 * and leaves the shard in a consistent state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DictionaryCodecOverflowTest extends TestHelper {

  @Test
  void testCodecOverflowThrows() {
    // Direct codec test: more than MAX_DICTIONARY_SIZE distinct values
    final String[] values = new String[DictionaryCodec.MAX_DICTIONARY_SIZE + 1];
    for (int i = 0; i < values.length; i++)
      values[i] = "tag_" + i;

    assertThatThrownBy(() -> DictionaryCodec.encode(values))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Dictionary overflow");
  }

  @Test
  void testCompactionAutoSplitsOnOverflow() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("tag", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_dict_overflow", 0, columns);

    // Insert data with more distinct tag values than the dictionary can handle in one block
    final int overflowCount = DictionaryCodec.MAX_DICTIONARY_SIZE + 100;
    final long[] timestamps = new long[overflowCount];
    final Object[] tags = new Object[overflowCount];
    final Object[] values = new Object[overflowCount];
    for (int i = 0; i < overflowCount; i++) {
      timestamps[i] = i * 1000L;
      tags[i] = "unique_tag_" + i;
      values[i] = (double) i;
    }

    shard.appendSamples(timestamps, tags, values);
    database.commit();

    // Compaction should succeed by auto-splitting into multiple blocks
    shard.compact();

    // Multiple sealed blocks should be created (at least 2)
    assertThat(shard.getSealedStore().getBlockCount()).isGreaterThanOrEqualTo(2);

    // Mutable bucket should be empty after compaction
    database.begin();
    assertThat(shard.getMutableBucket().getSampleCount()).isEqualTo(0);
    database.commit();

    // All data should be readable from sealed store
    database.begin();
    final List<Object[]> results = shard.scanRange(0, Long.MAX_VALUE, null, null);
    database.commit();

    assertThat(results).hasSize(overflowCount);

    shard.close();
  }

  @Test
  void testCodecAtExactLimit() throws java.io.IOException {
    // Exactly MAX_DICTIONARY_SIZE distinct values should succeed
    final String[] values = new String[DictionaryCodec.MAX_DICTIONARY_SIZE];
    for (int i = 0; i < values.length; i++)
      values[i] = "tag_" + i;

    final byte[] encoded = DictionaryCodec.encode(values);
    assertThat(encoded).isNotEmpty();

    final String[] decoded = DictionaryCodec.decode(encoded);
    assertThat(decoded).hasSize(values.length);
    assertThat(decoded[0]).isEqualTo("tag_0");
    assertThat(decoded[values.length - 1]).isEqualTo("tag_" + (values.length - 1));
  }
}
