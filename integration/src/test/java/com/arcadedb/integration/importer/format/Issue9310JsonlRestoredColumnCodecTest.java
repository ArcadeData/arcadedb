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
package com.arcadedb.integration.importer.format;

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.ColumnDefinition.ColumnRole;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9310: an export written before the codec validation can name a codec the builder now refuses, on any role. The
 * restore falls back to the default codec for that column instead of failing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9310JsonlRestoredColumnCodecTest {

  @Test
  void aCodecTheStorageCannotRunFallsBackToTheDefault() {
    final ColumnDefinition tag = JsonlImporterFormat.restoredColumn("t", "host", Type.STRING, ColumnRole.TAG, TimeSeriesCodec.NONE);
    assertThat(tag.getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
    assertThat(tag.isExplicitCodec()).isFalse();

    final ColumnDefinition textWithNumericCodec = JsonlImporterFormat.restoredColumn("t", "host", Type.STRING, ColumnRole.TAG,
        TimeSeriesCodec.SIMPLE8B);
    assertThat(textWithNumericCodec.getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);

    final ColumnDefinition field = JsonlImporterFormat.restoredColumn("t", "v", Type.DOUBLE, ColumnRole.FIELD, TimeSeriesCodec.NONE);
    assertThat(field.getCompressionHint()).isEqualTo(TimeSeriesCodec.GORILLA_XOR);

    final ColumnDefinition timestamp = JsonlImporterFormat.restoredColumn("t", "ts", Type.LONG, ColumnRole.TIMESTAMP,
        TimeSeriesCodec.SIMPLE8B);
    assertThat(timestamp.getCompressionHint()).isEqualTo(TimeSeriesCodec.DELTA_OF_DELTA);
  }

  @Test
  void aHonouredCodecIsKeptAsExplicit() {
    final ColumnDefinition field = JsonlImporterFormat.restoredColumn("t", "v", Type.DOUBLE, ColumnRole.FIELD, TimeSeriesCodec.DICTIONARY);
    assertThat(field.getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
    assertThat(field.isExplicitCodec()).isTrue();

    assertThat(JsonlImporterFormat.restoredColumn("t", "v", Type.DOUBLE, ColumnRole.FIELD, null).isExplicitCodec()).isFalse();
  }
}
