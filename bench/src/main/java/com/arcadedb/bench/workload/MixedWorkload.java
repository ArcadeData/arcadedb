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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.data.DatasetGenerator;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.workload.mixed.MixedPhase;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Six kinds of work at once against one server ({@link MixedPhase}): ingest, full-text and vector search, index compaction,
 * schema changes, time-series ingest and embedding updates, for one window per repetition. It publishes the p99 of each kind,
 * the longest stretch in which nothing committed and the operations per second of all of them. A loop that failed, or a
 * database that does not hold exactly what the loops acknowledged (checked after the window), fails the case with its metrics.
 */
public class MixedWorkload implements Workload {
  public static final String DATABASE = MixedPhase.DATABASE;

  static final String CASE = "mixed-phase";

  private static final Logger LOGGER = LoggerFactory.getLogger(MixedWorkload.class);

  private static final int    LOAD_BATCH   = 200;
  private static final double NANOS_PER_MS = 1_000_000.0;

  private MixedPhase phase;

  static int baseDocs(final Scale scale) {
    return MixedPhase.baseDocs(scale);
  }

  static Duration window(final Scale scale) {
    return switch (scale) {
      case XS -> Duration.ofSeconds(10);
      case S -> Duration.ofSeconds(90);
      case M -> Duration.ofSeconds(90);
      case L -> Duration.ofSeconds(120);
    };
  }

  @Override
  public String name() {
    return "mixed";
  }

  @Override
  public List<String> cases() {
    return List.of(CASE);
  }

  @Override
  public OptionalInt reps() {
    return OptionalInt.of(3);
  }

  @Override
  public OptionalInt warmup() {
    return OptionalInt.of(1);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    if (!CASE.equals(caseName))
      throw new IllegalArgumentException("Unknown case: " + caseName);
    prepareDatabase(ctx);
    phase = new MixedPhase(ctx.server(), ctx.seed(), ctx.scale());
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    final MixedPhase.Result result = phase.run(window(ctx.scale()));
    final Measurement measurement = publish(result);
    try {
      phase.check();
    } catch (final IllegalStateException e) {
      throw new CaseFailedWithMetrics(e.getMessage(), measurement.perRep());
    }
    return measurement;
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    phase = null;
  }

  /**
   * The metrics of one window: {@code <loop>_p99_ms} of each loop, {@code max_commit_gap_ms}, {@code ops_per_s} and
   * {@code <loop>_ops} (the operations each loop completed). Throws them in a {@link CaseFailedWithMetrics} when a loop failed.
   */
  static Measurement publish(final MixedPhase.Result result) throws CaseFailedWithMetrics {
    final List<Metric> metrics = new ArrayList<>(MixedPhase.LOOPS.size() * 2 + 2);
    for (final String loop : MixedPhase.LOOPS) {
      final LatencyRecorder recorder = result.latencies().get(loop);
      metrics.add(Metrics.of(loop + "_p99_ms", "ms", Metric.Better.LOWER, recorder.percentile(99) / NANOS_PER_MS));
    }
    metrics.add(Metrics.of("max_commit_gap_ms", "ms", Metric.Better.LOWER, result.longestCommitGapNanos() / NANOS_PER_MS));
    metrics.add(Metrics.rate("ops_per_s", "ops/s", result.operations() / result.seconds()));
    for (final String loop : MixedPhase.LOOPS)
      metrics.add(Metrics.rate(loop + "_ops", "operations", result.latencies().get(loop).count()));
    if (!result.failures().isEmpty())
      throw new CaseFailedWithMetrics(String.join("; ", result.failures()), metrics);
    return new Measurement(metrics);
  }

  /**
   * Untimed: the database {@value #DATABASE} with its base documents, indexes and the empty time-series type. Kept when it holds
   * exactly that (no document or point an earlier window added, no scratch type), else built again from nothing.
   */
  public static void prepareDatabase(final WorkloadContext ctx) {
    final int base = baseDocs(ctx.scale());
    if (RemoteClients.databaseExists(ctx.server(), DATABASE)) {
      try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
        if (ready(db, base))
          return;
      }
    }
    LOGGER.info("Building the mixed database: {} documents", base);
    RemoteClients.recreateDatabase(ctx.server(), DATABASE);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      final String type = MixedPhase.DOC_TYPE;
      db.command("sql", "CREATE DOCUMENT TYPE " + type).close();
      db.command("sql", "CREATE PROPERTY " + type + ".id LONG").close();
      db.command("sql", "CREATE PROPERTY " + type + ".body STRING").close();
      db.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS").close();
      db.command("sql", "CREATE TIMESERIES TYPE " + MixedPhase.SENSOR_TYPE + " TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (value DOUBLE)")
          .close();
      load(db, ctx.seed(), base);
      // the indexes are built once over the loaded documents, which is faster than maintaining them during the load
      db.command("sql", "CREATE INDEX ON " + type + " (id) UNIQUE").close();
      db.command("sql", "CREATE INDEX ON " + type + " (body) FULL_TEXT").close();
      db.command("sql", "CREATE INDEX ON " + type + " (embedding) LSM_VECTOR METADATA {dimensions: " + MixedPhase.DIM
          + ", similarity: 'COSINE'}").close();
    }
  }

  private static void load(final RemoteDatabase db, final long seed, final int base) {
    final DatasetGenerator.EmbeddingSpace space = MixedPhase.space(seed);
    final StringBuilder script = new StringBuilder(LOAD_BATCH * (MixedPhase.WORDS_PER_DOC * 10 + MixedPhase.DIM * 12));
    for (int from = 0; from < base; from += LOAD_BATCH) {
      final int to = Math.min(from + LOAD_BATCH, base);
      final float[][] vectors = new DatasetGenerator(seed + from).embeddings(to - from, space);
      script.setLength(0);
      script.append("BEGIN;\n");
      for (int i = from; i < to; i++)
        script.append("INSERT INTO ").append(MixedPhase.DOC_TYPE).append(" SET id = ").append(i).append(", body = '")
            .append(MixedPhase.docText(seed, i)).append("', embedding = ").append(MixedPhase.literal(vectors[i - from])).append(";\n");
      script.append("COMMIT;");
      db.command("sqlscript", script.toString()).close();
    }
  }

  private static boolean ready(final RemoteDatabase db, final int base) {
    try {
      if (RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + MixedPhase.DOC_TYPE) != base
          || RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + MixedPhase.SENSOR_TYPE) != 0)
        return false;
      final Set<String> indexes = new HashSet<>();
      try (final ResultSet rs = db.query("sql", "SELECT name FROM schema:indexes")) {
        while (rs.hasNext())
          indexes.add(rs.next().getProperty("name"));
      }
      if (!indexes.containsAll(List.of(MixedPhase.ID_INDEX, MixedPhase.TEXT_INDEX, MixedPhase.VECTOR_INDEX)))
        return false;
      try (final ResultSet rs = db.query("sql", "SELECT name FROM schema:types")) {
        while (rs.hasNext())
          if (rs.next().<String>getProperty("name").startsWith(MixedPhase.SCRATCH_PREFIX))
            return false;
      }
      return true;
    } catch (final RuntimeException e) {
      // a missing type means "build"
      return false;
    }
  }
}
