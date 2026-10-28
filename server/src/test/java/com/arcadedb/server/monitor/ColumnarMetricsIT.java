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
package com.arcadedb.server.monitor;

import com.arcadedb.database.Database;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.block.BlockCache;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarStore;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.GlobalConfiguration;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The columnar meters of {@link ColumnarMetrics}: tags, meter kinds, restart and corruption behaviour, values of the gauges, removal
 * with the database. The binder refreshes on a 5 s timer, so every check polls until the refresh published what it expects. The
 * Prometheus rendering is checked by the metrics module.
 */
class ColumnarMetricsIT extends BaseGraphServerTest {
  private static final long AWAIT_MS = 60_000;

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void rebuildCounterHasDatabaseTypeReasonTags() throws Exception {
    createAssetWithColumnarIndex(200);

    eventually(() -> {
      assertThat(rebuildsMeter("create")).isInstanceOf(FunctionCounter.class);
      assertThat(rebuilds("create")).isEqualTo(1);
      assertThat(rebuilds("corrupt")).isZero();
    });
  }

  @Test
  void restartDoesNotIncrementRebuilds() throws Exception {
    createAssetWithColumnarIndex(100);
    eventually(() -> assertThat(rebuilds("create")).isEqualTo(1));

    restartServerAndAwaitProjection();

    eventually(() -> {
      for (final String reason : List.of("create", "snapshot_install", "missing", "corrupt", "unclean_no_wal", "schema", "check_fix")) {
        assertThat(rebuildsMeter(reason)).as("rebuilds{reason=%s} series", reason).isNotNull();
        assertThat(rebuilds(reason)).as("rebuilds{reason=%s} after a clean restart", reason).isZero();
      }
    });
    assertThat(manager().getStats().rebuildsTotal()).isZero();
  }

  @Test
  void corruptReasonIsItsOwnSeries() throws Exception {
    createAssetWithColumnarIndex(100);
    manager().flushNow();
    final Path segment = firstSegmentFile();
    getServer(0).stop();
    damageFooterOffset(segment);
    startServer(0);
    awaitProjection();

    eventually(() -> {
      assertThat(rebuilds("corrupt")).isEqualTo(1);
      assertThat(rebuilds("missing")).isZero();
    });
  }

  @Test
  void closedDatabaseMetersAreRemoved() throws Exception {
    createAssetWithColumnarIndex(50);
    eventually(() -> assertThat(metersOfDatabase()).isNotEmpty());

    final ArcadeDBServer server = getServer(0);
    local().close();
    server.removeDatabase(getDatabaseName());

    eventually(() -> {
      assertThat(metersOfDatabase()).isEmpty();
      // the shared block cache meters are not a database's
      assertThat(meter("arcadedb.columnar.cache.hits", "database", "*")).isNotNull();
    });

    server.getDatabase(getDatabaseName());
  }

  @Test
  void aRefreshNeverReopensARegisteredButClosedDatabase() throws Exception {
    createAssetWithColumnarIndex(50);
    eventually(() -> assertThat(metersOfDatabase()).isNotEmpty());

    // closed behind the registry's back, as a shutdown loop or a snapshot swap leaves it
    final ArcadeDBServer server = getServer(0);
    final LocalDatabase closed = local();
    closed.close();

    eventually(() -> assertThat(metersOfDatabase()).isEmpty());
    assertThat(closed.isOpen()).isFalse();
    assertThatThrownBy(() -> server.getDatabase(getDatabaseName(), false, false)).isInstanceOf(RuntimeException.class);

    server.removeDatabase(getDatabaseName());
    server.getDatabase(getDatabaseName());
  }

  @Test
  void everySpecSection9MeterIsRegisteredAndTheGaugesCarryValues() throws Exception {
    final LocalDatabase local = createAssetWithColumnarIndex(100);
    local.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, 1);
    insertAssets(100, 100);
    manager().flushNow();
    await().atMost(AWAIT_MS, TimeUnit.MILLISECONDS).until(() -> manager().getStats().mergeCount("Asset") > 0);
    awaitProjection();

    // one segment now: deleting a seventh of its rows stays under the merge ratio, so the flush keeps and counts them as dead
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "DELETE FROM Asset WHERE brand = 'brand0'");
    awaitProjection();
    manager().flushNow();

    // a type whose distinct values pass the limit of its own index after the index was created
    db.command("sql", "CREATE DOCUMENT TYPE Wide BUCKETS 1");
    db.command("sql", "CREATE PROPERTY Wide.tag STRING");
    db.command("sql", "CREATE INDEX ON Wide (tag) COLUMNAR METADATA {\"maxCardinality\": 3}");
    db.transaction(() -> {
      for (int i = 0; i < 5; i++)
        db.newDocument("Wide").set("tag", "t" + i).save();
    });
    awaitProjection();

    eventually(() -> {
      for (final String name : List.of("apply.pages", "apply.placeholders.resolved", "flush.count", "merge.count", "apply.records.reread",
          "apply.unreadable", "rebuild.pages.reused", "flush.duration", "merge.duration", "apply.lag.ms", "overlay.rows", "segments",
          "segments.dead.ratio", "open.files"))
        assertThat(meter("arcadedb.columnar." + name, "type", "Asset")).as(name).isNotNull();
      for (final String name : List.of("dictionary.compactions", "dictionary.ordinals.reclaimed", "conversion.failures",
          "dictionary.size", "cardinality.exceeded"))
        assertThat(meter("arcadedb.columnar." + name, "type", "Asset", "property", "brand")).as(name).isNotNull();
      assertThat(meter("arcadedb.columnar.recovery.pages")).isNotNull();
      assertThat(meter("arcadedb.columnar.cache.hits", "database", "*")).isNotNull();
      assertThat(meter("arcadedb.columnar.cache.misses", "database", "*")).isNotNull();

      final Meter flushDuration = meter("arcadedb.columnar.flush.duration", "type", "Asset");
      assertThat(flushDuration).isNotNull();
      assertThat(((FunctionTimer) flushDuration).count()).isGreaterThan(0);
      assertThat(counter("flush.count", "Asset")).isGreaterThan(0);
      assertThat(counter("merge.count", "Asset")).isGreaterThan(0);
      assertThat(counter("apply.pages", "Asset")).isGreaterThan(0);
      assertThat(gauge("segments", "Asset")).isGreaterThan(0);
      assertThat(gauge("open.files", "Asset")).isGreaterThan(0);
      assertThat(gauge("apply.lag.ms", "Asset")).isGreaterThanOrEqualTo(0);
      assertThat(gauge("segments.dead.ratio", "Asset")).isGreaterThan(0);
      assertThat(propertyGauge("dictionary.size", "Asset", "brand")).isEqualTo(7);
      assertThat(propertyGauge("cardinality.exceeded", "Asset", "brand")).isZero();
      assertThat(propertyGauge("cardinality.exceeded", "Wide", "tag")).isEqualTo(1);
      final Meter cache = meter("arcadedb.columnar.cache.used.bytes", "database", "*");
      assertThat(cache).isNotNull();
      assertThat(((Gauge) cache).value()).isEqualTo((double) BlockCache.instance().usedBytes());
    });
  }

  // -------------------------------------------------------------------------------------------------------------------- helpers

  private void eventually(final ThrowingRunnable assertions) {
    await().atMost(AWAIT_MS, TimeUnit.MILLISECONDS).untilAsserted(assertions::run);
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private LocalDatabase createAssetWithColumnarIndex(final int rows) throws InterruptedException {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    db.command("sql", "CREATE PROPERTY Asset.brand STRING");
    insertAssets(0, rows);
    db.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    awaitProjection();
    return local();
  }

  private void insertAssets(final int from, final int count) {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.transaction(() -> {
      for (int i = from; i < from + count; i++)
        db.newDocument("Asset").set("brand", "brand" + i % 7).save();
    });
  }

  private LocalDatabase local() {
    return (LocalDatabase) getServer(0).getDatabase(getDatabaseName()).getEmbedded();
  }

  private ColumnarIndexManager manager() {
    return local().getColumnarIndexManager();
  }

  private void awaitProjection() throws InterruptedException {
    assertThat(manager().awaitCaughtUp(AWAIT_MS)).isTrue();
  }

  private void restartServerAndAwaitProjection() throws InterruptedException {
    getServer(0).stop();
    startServer(0);
    awaitProjection();
  }

  private Path firstSegmentFile() throws IOException {
    final Path root = Path.of(getDatabasePath(0)).resolve(ColumnarStore.ROOT_DIR);
    try (final Stream<Path> files = Files.walk(root)) {
      return files.filter(f -> f.getFileName().toString().endsWith(".acs")).findFirst().orElseThrow();
    }
  }

  /** Points the segment's footer offset past the end of the file: the reader refuses it as a corrupt block, at open. */
  private static void damageFooterOffset(final Path segment) throws IOException {
    try (final RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
      final long size = file.length();
      file.seek(size - 12);
      file.writeInt((int) size + 100);
    }
  }

  private Meter rebuildsMeter(final String reason) {
    return meter("arcadedb.columnar.rebuilds", "type", "Asset", "reason", reason);
  }

  private double rebuilds(final String reason) {
    final Meter meter = rebuildsMeter(reason);
    return meter == null ? 0 : ((FunctionCounter) meter).count();
  }

  private double counter(final String name, final String type) {
    final Meter meter = meter("arcadedb.columnar." + name, "type", type);
    assertThat(meter).as(name).isNotNull();
    return ((FunctionCounter) meter).count();
  }

  private double gauge(final String name, final String type) {
    final Meter meter = meter("arcadedb.columnar." + name, "type", type);
    assertThat(meter).as(name).isNotNull();
    return ((Gauge) meter).value();
  }

  private double propertyGauge(final String name, final String type, final String property) {
    final Meter meter = meter("arcadedb.columnar." + name, "type", type, "property", property);
    assertThat(meter).as(name + " of " + type + "." + property).isNotNull();
    return ((Gauge) meter).value();
  }

  private Meter meter(final String name, final String... tags) {
    final String[] all = new String[tags.length + (hasDatabaseTag(tags) ? 0 : 2)];
    System.arraycopy(tags, 0, all, 0, tags.length);
    if (!hasDatabaseTag(tags)) {
      all[tags.length] = "database";
      all[tags.length + 1] = getDatabaseName();
    }
    return Metrics.globalRegistry.find(name).tags(all).meter();
  }

  private static boolean hasDatabaseTag(final String[] tags) {
    for (int i = 0; i < tags.length; i += 2)
      if (tags[i].equals("database"))
        return true;
    return false;
  }

  private List<Meter> metersOfDatabase() {
    return Metrics.globalRegistry.getMeters().stream()
        .filter(m -> m.getId().getName().startsWith("arcadedb.columnar.") && getDatabaseName().equals(m.getId().getTag("database")))
        .toList();
  }
}
