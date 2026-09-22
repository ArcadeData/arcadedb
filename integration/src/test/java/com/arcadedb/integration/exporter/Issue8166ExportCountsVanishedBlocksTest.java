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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.exporter.format.JsonlExporterFormat;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8166, the observability half: an {@code EXPORT DATABASE} that lost sealed blocks mid-walk wrote a SHORT
 * file and said nothing about it.
 * <p>
 * The engine has counted those blocks since #8043 - {@code AggregationMetrics.addVanishedBlock} - but the only
 * reader of that count anywhere in the tree was the PromQL/HTTP metrics surface, and
 * {@code JsonlExporterFormat.exportTimeSeries} passed {@code null} for the metrics. So the one caller the
 * counter was added for could not see it: no exception, no log line, no count. The export now passes an
 * {@code AggregationMetrics}, logs a WARNING naming the type, and reports the total under
 * {@code vanishedTimeSeriesBlocks} in the export's own statistics.
 * <p>
 * Reported, not fatal, and that is deliberate: retention dropping blocks older than the policy while a long
 * export runs is legitimate, and those samples are genuinely gone rather than somewhere else - unlike a block a
 * DOWNSAMPLE coarsened, for which the engine raises instead (see {@code Issue8166WalkAcrossDownsampleTest}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8166">issue #8166</a>
 */
class Issue8166ExportCountsVanishedBlocksTest {
  private static final String SOURCE_PATH = "target/databases/issue8166-export-source";
  private static final String FILE        = "target/issue8166-export.jsonl.tgz";
  private static final long   BASE_TS     = 1_700_000_000_000L;
  /**
   * Over two sealed blocks: {@code DeltaOfDeltaCodec.MAX_BLOCK_SIZE} is 65,536 samples, so this compacts into
   * three, which is the minimum that lets a truncate strand blocks a walk has not reached yet.
   */
  private static final int    SAMPLES     = 140_000;

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    new File(FILE).delete();
  }

  @Test
  void anExportThatLosesBlocksToRetentionReportsThemInsteadOfWritingSilentlyShort() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) source.getSchema().getType("Reading");
      final TimeSeriesEngine engine = tsType.getEngine();

      final long[] timestamps = new long[SAMPLES];
      final Object[] hosts = new Object[SAMPLES];
      final Object[] values = new Object[SAMPLES];
      for (int i = 0; i < SAMPLES; i++) {
        timestamps[i] = BASE_TS + i * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
      }
      source.begin();
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      source.commit();
      engine.compactAll();

      assertThat(engine.getShard(0).getSealedStore().getBlockCount())
          .as("the walk needs blocks it has not reached yet for a truncate to strand any").isGreaterThan(2);

      final ExporterContext context = new ExporterContext();
      final ExporterSettings settings = new ExporterSettings();
      settings.file = FILE;
      settings.format = JsonlExporterFormat.NAME;
      settings.overwriteFile = true;

      // The retention pass, fired from inside the export's own chunk flush so it lands mid-walk deterministically
      // rather than by racing a background thread against it.
      final JsonlExporterFormat format = new JsonlExporterFormat((DatabaseInternal) source, settings, context,
          new ConsoleLogger(0)) {
        private boolean fired;

        @Override
        protected void writeJsonLine(final String type, final JSONObject json) throws IOException {
          super.writeJsonLine(type, json);
          if (!fired && "ts".equals(type)) {
            fired = true;
            // Keeps only the last block; every block after the one the walk is inside is now stranded.
            engine.getShard(0).getSealedStore().truncateBefore(BASE_TS + (SAMPLES - 1_000L) * 1_000L);
          }
        }
      };

      format.exportDatabase();

      assertThat(context.vanishedTimeSeriesBlocks.get())
          .as("the blocks retention removed from under the walk must be counted, not stepped over in silence")
          .isGreaterThan(0);
      assertThat(context.timeSeriesSamples.get())
          .as("and the export is short, which is the whole reason the count has to exist")
          .isLessThan(SAMPLES);
    }
  }

  /**
   * The control: an export nobody disturbs reports no vanished block and writes every sample, so passing the
   * metrics through did not turn an ordinary export into a suspicious one.
   */
  @Test
  void anUndisturbedExportReportsNoVanishedBlock() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      final TimeSeriesEngine engine = ((LocalTimeSeriesType) source.getSchema().getType("Reading")).getEngine();
      final long[] timestamps = new long[2_000];
      final Object[] hosts = new Object[2_000];
      final Object[] values = new Object[2_000];
      for (int i = 0; i < 2_000; i++) {
        timestamps[i] = BASE_TS + i * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
      }
      source.begin();
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      source.commit();
      engine.compactAll();

      final Map<String, Object> result = new Exporter(source, FILE).setFormat(JsonlExporterFormat.NAME)
          .setOverwrite(true).exportDatabase();

      assertThat(result).doesNotContainKey("vanishedTimeSeriesBlocks");
      assertThat(result).containsEntry("timeSeriesSamples", 2_000L);
    }
  }
}
