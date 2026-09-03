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
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #7032: three defects in the JSONL export/import round trip.
 *
 * <ol>
 *   <li>A TIMESERIES type is exported as {@code "type":"t"} and the importer's schema switch had no case for it,
 *       so the switch's {@code default} threw: exporting a database with any TIMESERIES type produced a file that
 *       could not be imported AT ALL - the failure aborted the whole import at the schema line.</li>
 *   <li>The export carries type-level CUSTOM metadata, ALIASES and the bucket-selection strategy; the importer
 *       restored none of the three, while property-level CUSTOM - adjacent code, same JSON - was restored.</li>
 *   <li>Every exported vertex carried the RID of every one of its edges, in both directions, and no import path
 *       reads them.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7032JsonlRoundTripIT {
  private static final String SOURCE_PATH = "target/databases/issue7032-jsonl-source";
  private static final String TARGET_PATH = "target/databases/issue7032-jsonl-target";
  private static final String FILE        = "target/issue7032-jsonl.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  @Test
  void theWholeSchemaAndItsTimeSeriesSamplesSurviveAnExportImportCycle() throws Exception {
    createSourceDatabase();

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE).exists()).isTrue();

    // (3) No vertex line carries edge RID lists any more, and the edge lines still carry their endpoints.
    int vertexLines = 0;
    int edgeLines = 0;
    for (final String line : readExportedLines()) {
      final JSONObject json = new JSONObject(line);
      final JSONObject content = json.getJSONObject("c");
      switch (json.getString("t")) {
      case "v" -> {
        vertexLines++;
        assertThat(content.has("o")).as("a vertex must not carry its outgoing edge RIDs: %s", line).isFalse();
        assertThat(content.has("i")).as("a vertex must not carry its incoming edge RIDs: %s", line).isFalse();
      }
      case "e" -> {
        edgeLines++;
        assertThat(content.has("o")).as("an edge's endpoints ARE the edge: %s", line).isTrue();
        assertThat(content.has("i")).as("an edge's endpoints ARE the edge: %s", line).isTrue();
      }
      default -> {
      }
      }
    }
    assertThat(vertexLines).isEqualTo(2);
    assertThat(edgeLines).isEqualTo(1);

    // (1) The import used to abort here, at the schema line, on the TIMESERIES type's "t" marker.
    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      assertThat(target.countType("Person", false)).isEqualTo(2);
      assertThat(target.countType("Friend", false)).isEqualTo(1);
      assertThat(target.query("sql", "SELECT FROM Person WHERE id = 0").next().getVertex().get()
          .getVertices(Vertex.DIRECTION.OUT, "Friend").iterator().hasNext())
          .as("the edge must be rebuilt from the edge line, which is the only thing that ever rebuilt it").isTrue();

      // (2) Type-level ALIASES, CUSTOM and the bucket-selection strategy.
      final DocumentType invoice = target.getSchema().getType("Invoice");
      assertThat(invoice.getAliases()).containsExactlyInAnyOrder("Bill", "Receipt");
      assertThat(invoice.getCustomValue("retention")).isEqualTo("7y");
      assertThat(invoice.getBucketSelectionStrategy().getName()).isEqualTo("thread");
      assertThat(invoice.getProperty("code").getCustomValue("pii")).as("property-level CUSTOM must keep working")
          .isEqualTo("false");

      // The type is reachable through its restored alias, which is the point of restoring one.
      assertThat(target.query("sql", "SELECT FROM Bill").hasNext()).isTrue();

      // (1) The TIMESERIES type comes back with its definition AND its samples.
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) target.getSchema().getType("Sensor");
      assertThat(tsType.getShardCount()).isEqualTo(2);
      assertThat(tsType.getTsColumns().stream().map(ColumnDefinition::getName).toList())
          .containsExactly("ts", "host", "value", "ratio");
      assertThat(tsType.getTsColumns().get(2).getCompressionHint()).as("the per-column codec is not re-derivable")
          .isEqualTo(TimeSeriesCodec.GORILLA_XOR);

      // Every non-default TIMESERIES setting the export carries must come back, tiers in order.
      final LocalTimeSeriesType policy = (LocalTimeSeriesType) target.getSchema().getType("SensorPolicy");
      assertThat(policy.getTimestampColumn()).isEqualTo("ts");
      assertThat(policy.getPrecision()).isEqualTo("MICROSECOND");
      assertThat(policy.getShardCount()).isEqualTo(3);
      assertThat(policy.getRetentionMs()).isEqualTo(90L * 86_400_000L);
      assertThat(policy.getCompactionBucketIntervalMs()).isEqualTo(30_000L);
      assertThat(policy.getDownsamplingTiers()).hasSize(2);
      assertThat(policy.getDownsamplingTiers().get(0).afterMs()).isEqualTo(7L * 86_400_000L);
      assertThat(policy.getDownsamplingTiers().get(0).granularityMs()).isEqualTo(3_600_000L);
      assertThat(policy.getDownsamplingTiers().get(1).afterMs()).isEqualTo(30L * 86_400_000L);
      assertThat(policy.getDownsamplingTiers().get(1).granularityMs()).isEqualTo(86_400_000L);

      final List<Double> values = new ArrayList<>();
      final List<Float> ratios = new ArrayList<>();
      try (final ResultSet rs = target.query("sql", "SELECT value, ratio FROM Sensor ORDER BY ts")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(((Number) row.getProperty("value")).doubleValue());
          ratios.add(((Number) row.getProperty("ratio")).floatValue());
        }
      }
      assertThat(values).hasSize(3);
      assertThat(values.get(0)).isEqualTo(1.5);
      // NaN must survive: JSONArray.put(Number) rewrites a non-finite double to 0, so an unencoded NaN would come
      // back as a measurement of zero.
      assertThat(values.get(1)).isNaN();
      assertThat(values.get(2)).isEqualTo(3.5);

      // Same encoding, narrower column: a non-finite FLOAT must not come back as 0 either.
      assertThat(ratios.get(0)).isEqualTo(0.5f);
      assertThat(ratios.get(1)).isNaN();
      assertThat(ratios.get(2)).isEqualTo(Float.POSITIVE_INFINITY);
    }
  }

  /**
   * The alias restore must not be able to abort an import. {@code setAliases} refuses an alias another type
   * already answers to, which an import into a NON-EMPTY target reaches without anything being wrong with the
   * export - and aborting there would discard every record the file has not reached yet.
   */
  @Test
  void anAliasAlreadyTakenInTheTargetDowngradesToAWarningInsteadOfAbortingTheImport() throws Exception {
    createSourceDatabase();
    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      // Claims "Bill" before the import gets there, so Invoice's alias cannot be restored.
      target.getSchema().createDocumentType("Bill");
      target.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      // Every record after the schema line still made it in.
      assertThat(target.countType("Person", false)).isEqualTo(2);
      assertThat(target.countType("Friend", false)).isEqualTo(1);
      assertThat(target.countType("Invoice", false)).isEqualTo(1);
      assertThat(target.query("sql", "SELECT count(*) AS c FROM Sensor").next().<Number>getProperty("c").longValue())
          .isEqualTo(3L);

      // The type keeps its own name; the alias it could not take is simply not registered.
      final DocumentType invoice = target.getSchema().getType("Invoice");
      assertThat(invoice.getAliases()).doesNotContain("Bill");
      assertThat(invoice.getCustomValue("retention")).as("the rest of the type metadata is unaffected")
          .isEqualTo("7y");
    }
  }

  /**
   * A {@code "ts"} chunk whose samples do not match the type's column count is refused by the caller's row-error
   * policy, and refused for the WHOLE chunk before any of it is appended. A longer sample is the one that matters:
   * reading only the schema positions would have truncated it silently, discarding data without saying so.
   */
  @Test
  void aTimeSeriesSampleOfTheWrongArityIsRejectedRatherThanTruncated() throws Exception {
    createSourceDatabase();
    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    // Give one sample of the "ts" chunk a fifth value the 4-column type does not declare.
    final List<String> lines = new ArrayList<>();
    for (final String line : readExportedLines()) {
      final JSONObject json = new JSONObject(line);
      if ("ts".equals(json.getString("t"))) {
        json.getJSONObject("c").getJSONArray("s").getJSONArray(0).put(999.0);
        lines.add(json.toString());
      } else
        lines.add(line);
    }
    writeExportedLines(lines);

    final Throwable thrown = catchThrowable(() -> new Importer(
        ("-url " + new File(FILE).getAbsolutePath() + " -database " + TARGET_PATH + " -forceDatabaseCreate true")
            .split(" ")).load());

    assertThat(thrown).isNotNull();
    assertThat(thrown).hasStackTraceContaining("has 5 value(s) but the type declares 4 column(s)");
  }

  private void createSourceDatabase() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.transaction(() -> {
        final VertexType person = source.getSchema().buildVertexType().withName("Person").create();
        person.createProperty("id", Type.INTEGER);
        source.getSchema().buildEdgeType().withName("Friend").create();

        final MutableVertex a = source.newVertex("Person").set("id", 0).save();
        final MutableVertex b = source.newVertex("Person").set("id", 1).save();
        a.newEdge("Friend", b);

        final DocumentType invoice = source.getSchema().createDocumentType("Invoice", 4);
        invoice.createProperty("code", Type.STRING).setCustomValue("pii", "false");
        invoice.setAliases(Set.of("Bill", "Receipt"));
        invoice.setCustomValue("retention", "7y");
        invoice.setBucketSelectionStrategy("thread");
        source.newDocument("Invoice").set("code", "INV-1").save();
      });

      // The FLOAT column is here for the non-finite encoding: JSONArray.put(Number) rewrites NaN and +/-Infinity
      // to 0 whatever the width, so both float and double columns need the string markers.
      source.command("sql",
          "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE, ratio FLOAT) SHARDS 2");

      // A second TIMESERIES type whose retention / precision / compaction / downsampling settings are all
      // NON-default, so the restore of each one is actually pinned. It carries no samples on purpose: a retention
      // policy would otherwise expire the epoch-1970 timestamps the sample-carrying type uses.
      source.command("sql", """
          CREATE TIMESERIES TYPE SensorPolicy
            TIMESTAMP ts PRECISION MICROSECOND
            TAGS (host STRING)
            FIELDS (value DOUBLE)
            SHARDS 3
            RETENTION 90 DAYS
            COMPACTION INTERVAL 30 SECONDS
          """);
      source.command("sql",
          "ALTER TIMESERIES TYPE SensorPolicy ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS "
              + "AFTER 30 DAYS GRANULARITY 1 DAYS");

      // A NaN sample has no SQL literal, so the samples go in through the engine.
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) source.getSchema().getType("Sensor");
      source.begin();
      tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L, 3_000L },
          new Object[] { "h1", "h1", "h2" },
          new Object[] { 1.5, Double.NaN, 3.5 },
          new Object[] { 0.5f, Float.NaN, Float.POSITIVE_INFINITY });
      source.commit();
    }
  }

  private void writeExportedLines(final List<String> lines) throws Exception {
    try (final Writer writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(FILE)),
        StandardCharsets.UTF_8)) {
      for (final String line : lines)
        writer.write(line + "\n");
    }
  }

  private List<String> readExportedLines() throws Exception {
    final List<String> lines = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(FILE)), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null)
        lines.add(line);
    }
    return lines;
  }
}
