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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6116: an open point-in-time snapshot window (#6075) has to be visible to an operator while it is open, not
 * only reconstructible from the log after it has gone.
 * <p>
 * Everything here is asserted as a DELTA against a baseline taken at the start of the test. {@link PageManager} is a
 * JVM-wide singleton whose counters are deliberately never reset (#5636), so absolute values carry whatever the rest
 * of the suite did in this fork before us.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PageSnapshotMetricsTest extends TestHelper {

  private static final String TYPE = "Doc";

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("id", Integer.class);
    type.createProperty("payload", String.class);

    database.transaction(() -> {
      for (int i = 0; i < 5_000; i++)
        database.newDocument(TYPE).set("id", i).set("payload", "initial-" + "x".repeat(200)).save();
    });
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
  }

  /**
   * The window's lifetime is observable from outside it: the gauges rise when it opens and return to where they were
   * when it closes, and the "opened" total moves exactly once.
   */
  @Test
  void anOpenWindowIsVisibleInTheStatsAndLeavesNoTraceInThemWhenItCloses() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final PageManager.PPageManagerStats before = pageManager.getStats();
    assertThat(before.snapshotWindowsOpen).as("no window may be open before this test opens one").isZero();

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      Thread.sleep(20);

      final PageManager.PPageManagerStats open = pageManager.getStats();
      assertThat(open.snapshotWindowsOpen).isEqualTo(1);
      assertThat(open.snapshotWindowsOpened).isEqualTo(before.snapshotWindowsOpened + 1);
      assertThat(open.snapshotWindowsInvalidated).isEqualTo(before.snapshotWindowsInvalidated);
      assertThat(open.snapshotOldestWindowMillis).as("the window's age must be measured from when it opened")
          .isPositive();
      assertThat(open.snapshotShadowedPages).as("nothing has been written yet, so nothing is shadowed").isZero();
      // #6125: THE DEFAULT CAP IS RESOLVED AT t0 FROM THE SIZE THE PAGE FILES OCCUPY, NOT READ AS A NUMBER OF MB
      long t0Size = 0;
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        t0Size += file.size();
      assertThat(snapshot.getShadowMaxSizeInBytes()).isEqualTo(t0Size);
    }

    final PageManager.PPageManagerStats after = pageManager.getStats();
    assertThat(after.snapshotWindowsOpen).isZero();
    assertThat(after.snapshotShadowedPages).isZero();
    assertThat(after.snapshotShadowBytes).isZero();
    assertThat(after.snapshotOldestWindowMillis).isZero();
    // THE TOTALS ARE COUNTERS: THEY DO NOT COME BACK DOWN WITH THE WINDOW
    assertThat(after.snapshotWindowsOpened).isEqualTo(before.snapshotWindowsOpened + 1);
  }

  /**
   * The shadow gauges measure the copy-on-write work the window is costing. The control matters as much as the
   * measurement: the identical rewrite performed with NO window open must not move the pre-image counter at all, so
   * this cannot pass by counting something the write path does anyway.
   */
  @Test
  void theShadowGaugesTrackTheCopyOnWriteWorkAndCountNothingWithNoWindowOpen() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    // CONTROL: THE SAME REWRITE, NO WINDOW OPEN
    final long capturedBeforeControl = pageManager.getStats().snapshotPreImagesCaptured;
    rewriteEveryRecord("control");
    assertThat(pageManager.getStats().snapshotPreImagesCaptured)
        .as("with no window open the write path must capture nothing").isEqualTo(capturedBeforeControl);

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      rewriteEveryRecord("shadowed");

      final PageManager.PPageManagerStats stats = pageManager.getStats();
      assertThat(stats.snapshotShadowedPages).as("the rewrite must have forced pre-image captures").isPositive();
      assertThat(stats.snapshotShadowedPages).isEqualTo(snapshot.getShadowedPages());
      assertThat(stats.snapshotShadowBytes).isEqualTo(snapshot.getShadowSizeInBytes());
      assertThat(stats.snapshotShadowSpilledBytes).isEqualTo(snapshot.getShadowSpilledBytes());
      assertThat(stats.snapshotPreImagesCaptured).isEqualTo(capturedBeforeControl + stats.snapshotShadowedPages);

      // THE HEADROOM READING: SMALL AGAINST THE 1 GB DEFAULT CAP, BUT NOT ZERO - ROUNDING IT TO A LONG WOULD TELL AN
      // OPERATOR WATCHING THE SHADOW FILL THAT IT IS EMPTY
      assertThat(stats.snapshotShadowUsagePerc).isPositive().isLessThan(100.0);
      assertThat(stats.snapshotShadowUsagePerc)
          .isEqualTo(100.0 * stats.snapshotShadowBytes / snapshot.getShadowMaxSizeInBytes());
    }
  }

  /**
   * A window that loses its point in time is invisible from the outside - its consumer falls back to the
   * suspend-and-freeze path and still completes - so the counter is the only signal that a backup has started
   * throttling the writers again.
   */
  @Test
  void aWindowThatBreachesItsCapIsCounted() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final long invalidatedBefore = pageManager.getStats().snapshotWindowsInvalidated;

    // ONE MEGABYTE OF SHADOW: A HANDFUL OF PAGES, WHICH A FULL REWRITE BLOWS THROUGH AT ONCE
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.setValue(1);
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.setValue(1);

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      for (int round = 0; round < 5 && snapshot.getStatus() == PageSnapshot.STATUS.ACTIVE; round++)
        rewriteEveryRecord("overflow-" + round);

      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.OVERFLOWED);
      assertThat(pageManager.getStats().snapshotWindowsInvalidated).isEqualTo(invalidatedBefore + 1);
    }

    // COUNTED ONCE, NOT ONCE PER FAILED CAPTURE: EVERY LATER WRITE STILL REACHES THE DEAD WINDOW
    assertThat(pageManager.getStats().snapshotWindowsInvalidated).isEqualTo(invalidatedBefore + 1);
  }

  /**
   * The same readings have to reach the two places an operator actually looks: the profiler JSON, which Studio's
   * server page renders row by row, and the metrics dump.
   */
  @Test
  void theProfilerReportsTheWindowToStudioAndToTheMetricsDump() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
      rewriteEveryRecord("profiled");

      final JSONObject json = Profiler.INSTANCE.toJSON();
      assertThat(json.getJSONObject("snapshotWindowsOpen").getLong("value")).isEqualTo(1);
      assertThat(json.getJSONObject("snapshotShadowedPages").getLong("value")).isEqualTo(snapshot.getShadowedPages());
      assertThat(json.getJSONObject("snapshotShadowSize").getLong("space")).isEqualTo(snapshot.getShadowSizeInBytes());
      assertThat(json.getJSONObject("snapshotShadowSpilledSize").getLong("space"))
          .isEqualTo(snapshot.getShadowSpilledBytes());
      assertThat(json.getJSONObject("snapshotShadowUsagePerc").getDouble("perc")).isPositive();
      assertThat(json.getJSONObject("snapshotOldestWindowAge").getLong("value")).isNotNegative();
      assertThat(json.getJSONObject("snapshotWindowsOpened").getLong("count")).isPositive();
      assertThat(json.getJSONObject("snapshotWindowsInvalidated").getLong("count")).isNotNegative();
      assertThat(json.getJSONObject("snapshotPreImagesCaptured").getLong("count")).isPositive();
      // #6087: the same question asked of the other path, and until now equally unreported
      assertThat(json.getJSONObject("deferredRAM").getLong("space")).isNotNegative();

      final ByteArrayOutputStream dump = new ByteArrayOutputStream();
      Profiler.INSTANCE.dumpMetrics(new PrintStream(dump, true, StandardCharsets.UTF_8));
      final String text = dump.toString(StandardCharsets.UTF_8);
      assertThat(text).contains("PAGE-SNAPSHOT windowsOpen=1");
      assertThat(text).contains("deferredRAM=");
    }

    assertThat(Profiler.INSTANCE.toJSON().getJSONObject("snapshotWindowsOpen").getLong("value")).isZero();
  }

  private void rewriteEveryRecord(final String marker) {
    database.transaction(() -> database.iterateType(TYPE, false).forEachRemaining(record -> {
      final MutableDocument doc = record.asDocument().modify();
      doc.set("payload", marker + "-" + "y".repeat(200));
      doc.save();
    }));
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
  }
}
