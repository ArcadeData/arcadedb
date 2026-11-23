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
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.remote.RemoteDatabase;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Write throughput: a CSV bulk import and SQL inserts in transactions of 500 rows over HTTP, from one and from eight clients.
 * Every repetition starts from a fresh type, so each one measures the same work.
 */
public class IngestionWorkload implements Workload {
  public static final String DATABASE = "bench_ingestion";

  static final String BULK = "bulk-import-csv";
  static final String TX_1T = "tx-insert-1t";
  static final String TX_8T = "tx-insert-8t";

  private static final String CONTAINER_CSV = "/tmp/rows.csv";
  // the type the CSV importer creates for a documents file
  private static final String IMPORT_TYPE = "Document";
  private static final String TX_TYPE = "TxRow";
  private static final int    BATCH = 500;

  private Path csv;

  static int rows(final Scale scale) {
    return switch (scale) {
      case XS -> 10_000;
      case S -> 200_000;
      case M -> 2_000_000;
      case L -> 10_000_000;
    };
  }

  @Override
  public String name() {
    return "ingestion";
  }

  @Override
  public List<String> cases() {
    return List.of(BULK, TX_1T, TX_8T);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) throws Exception {
    RemoteClients.ensureDatabase(ctx.server(), DATABASE);
    if (BULK.equals(caseName)) {
      csv = Files.createTempFile("bench-rows", ".csv");
      try (final BufferedWriter writer = Files.newBufferedWriter(csv, StandardCharsets.UTF_8)) {
        final Iterator<String> rows = new DatasetGenerator(ctx.seed()).csvRows(rows(ctx.scale()));
        while (rows.hasNext()) {
          writer.write(rows.next());
          writer.write('\n');
        }
      }
      ctx.server().copyToContainer(csv, CONTAINER_CSV);
    }
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    return switch (caseName) {
      case BULK -> bulkImport(ctx);
      case TX_1T -> txInsert(ctx, 1);
      case TX_8T -> txInsert(ctx, 8);
      default -> throw new IllegalArgumentException("Unknown case: " + caseName);
    };
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    if (csv != null) {
      try {
        Files.deleteIfExists(csv);
      } catch (final IOException e) {
        // a temp file left behind is harmless
      }
      csv = null;
    }
  }

  private Measurement bulkImport(final WorkloadContext ctx) {
    final ServerUnderTest server = ctx.server();
    final int expected = rows(ctx.scale());
    try (final RemoteDatabase db = RemoteClients.open(server, DATABASE)) {
      db.command("sql", "DROP TYPE " + IMPORT_TYPE + " IF EXISTS UNSAFE");
      server.resetPeak();
      final long start = System.nanoTime();
      db.command("sql", "IMPORT DATABASE file://" + CONTAINER_CSV).close();
      final double seconds = (System.nanoTime() - start) / 1e9;

      final long imported = count(db, IMPORT_TYPE);
      if (imported != expected)
        throw new IllegalStateException("The import created " + imported + " records, expected " + expected);

      return new Measurement(List.of(Metrics.rate("records_per_s", "records/s", expected / seconds),
          Metrics.seconds("total_s", seconds), Metrics.bytes("disk_bytes", Metrics.settledDiskBytes(server, DATABASE)),
          Metrics.bytes("peak_rss_bytes", server.peakRssBytes())));
    }
  }

  private Measurement txInsert(final WorkloadContext ctx, final int threads) throws Exception {
    final ServerUnderTest server = ctx.server();
    final int total = rows(ctx.scale());
    final int perThread = total / threads;
    try (final RemoteDatabase db = RemoteClients.open(server, DATABASE)) {
      db.command("sql", "DROP TYPE " + TX_TYPE + " IF EXISTS UNSAFE");
      db.command("sql", "CREATE DOCUMENT TYPE " + TX_TYPE);
    }

    // everything but the HTTP calls happens before the clock starts: the scripts are built and the clients are open. The scripts
    // of all threads are held in memory (about 90 bytes per row, so 900 MB at scale L: size the client heap for it)
    final List<String[]> scripts = new ArrayList<>(threads);
    for (int t = 0; t < threads; t++)
      scripts.add(buildScripts(ctx.seed() + t, perThread));
    final List<RemoteDatabase> clients = new ArrayList<>(threads);
    final LatencyRecorder latencies = new LatencyRecorder(total / BATCH + threads);
    final double seconds;
    try {
      for (int t = 0; t < threads; t++)
        clients.add(RemoteClients.open(server, DATABASE));

      final long start = System.nanoTime();
      if (threads == 1) {
        insertScripts(clients.get(0), scripts.get(0), latencies);
        // a single client: the throughput is the rows over the time spent in the calls
        seconds = latencies.totalNanos() / 1e9;
      } else {
        final ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
          final Thread t = new Thread(r, "bench-insert");
          t.setDaemon(true);
          return t;
        });
        try {
          final List<Future<?>> futures = new ArrayList<>(threads);
          for (int t = 0; t < threads; t++) {
            final RemoteDatabase client = clients.get(t);
            final String[] mine = scripts.get(t);
            futures.add(pool.submit(() -> {
              insertScripts(client, mine, latencies);
              return null;
            }));
          }
          for (final Future<?> f : futures)
            try {
              f.get();
            } catch (final ExecutionException e) {
              throw e.getCause() instanceof Exception c ? c : e;
            }
        } finally {
          pool.shutdownNow();
        }
        seconds = (System.nanoTime() - start) / 1e9;
      }
    } finally {
      clients.forEach(RemoteDatabase::close);
    }

    try (final RemoteDatabase db = RemoteClients.open(server, DATABASE)) {
      final long inserted = count(db, TX_TYPE);
      if (inserted != (long) perThread * threads)
        throw new IllegalStateException("Inserted " + inserted + " records, expected " + (long) perThread * threads);
    }

    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("records_per_s", "records/s", (double) perThread * threads / seconds));
    metrics.addAll(Metrics.latencies(latencies));
    return Measurement.of(metrics, latencies);
  }

  private static String[] buildScripts(final long seed, final int rows) {
    final Iterator<String> it = new DatasetGenerator(seed).csvRows(rows);
    it.next(); // header
    final String[] scripts = new String[(rows + BATCH - 1) / BATCH];
    final StringBuilder script = new StringBuilder(BATCH * 90);
    int done = 0;
    for (int b = 0; b < scripts.length; b++) {
      script.setLength(0);
      script.append("BEGIN;\n");
      final int n = Math.min(BATCH, rows - done);
      for (int i = 0; i < n; i++)
        appendInsert(script, it.next(), seed);
      script.append("COMMIT;");
      done += n;
      scripts[b] = script.toString();
    }
    return scripts;
  }

  private static void insertScripts(final RemoteDatabase db, final String[] scripts, final LatencyRecorder latencies) {
    for (final String script : scripts) {
      final long start = System.nanoTime();
      db.command("sqlscript", script).close();
      final long elapsed = System.nanoTime() - start;
      synchronized (latencies) {
        latencies.record(elapsed);
      }
    }
  }

  // the row is "id,name,age,city,score" from the generator, which never produces quotes or commas inside a value
  private static void appendInsert(final StringBuilder script, final String row, final long thread) {
    final String[] c = row.split(",");
    script.append("INSERT INTO ").append(TX_TYPE).append(" SET id = ").append(c[0]).append(", thread = ").append(thread)
        .append(", name = '").append(c[1]).append("', age = ").append(c[2]).append(", city = '").append(c[3])
        .append("', score = ").append(c[4]).append(";\n");
  }

  private static long count(final RemoteDatabase db, final String type) {
    return RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + type);
  }
}
