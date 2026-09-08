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
package com.arcadedb;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for finding 2 of issue #7266.
 * <p>
 * #7223 moved the profiler's free-space reading to {@code File.getUsableSpace()}, which excludes the blocks the
 * filesystem reserves for root, while the total stayed {@code getTotalSpace()}, which counts them. The Studio card
 * derived used as {@code total - free}, so the reservation - 5% of an ext4 by default - was charged to the
 * databases: on a 100 GB volume holding 1 GB the card reported roughly 6 GB used, a constant offset an operator
 * watching for growth reads as data. The server now reports the allocated figure itself, derived from the two
 * readings that count the reserved blocks the same way.
 */
class Issue7266ProfilerDiskUsedSpaceTest {

  /**
   * The three {@code File} readings are taken microseconds apart inside one {@code toJSON()}, so an unrelated write
   * landing between them moves either by a few blocks. The bound carries a page-cluster of slack, which is orders
   * of magnitude smaller than the reservation this fix is about and far larger than any such drift.
   */
  private static final long DRIFT_SLACK = 16L * 1024 * 1024;

  @TempDir
  Path tempDir;

  @AfterEach
  void restoreTheDatabaseDirectory() {
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.reset();
  }

  @Test
  void theUsedFigureIsDerivedFromTheUnallocatedReadingNotTheUsableOne() {
    final File databases = tempDir.resolve("databases").toFile();
    assertThat(databases.mkdirs()).isTrue();
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databases.getAbsolutePath());

    final JSONObject json = Profiler.INSTANCE.toJSON();

    final long used = json.getJSONObject("diskUsedSpace").getLong("space");
    final long total = json.getJSONObject("diskTotalSpace").getLong("space");

    assertThat(total).isPositive();
    assertThat(used).as("the allocated part of a volume is neither negative nor larger than the volume")
        .isBetween(0L, total);

    // The positive claim: used is total minus the UNALLOCATED reading, the one that counts the reserved blocks as
    // free exactly as getTotalSpace() counts them as total.
    assertThat(used).as("used must be derived from getFreeSpace(), which describes the same blocks as getTotalSpace()")
        .isCloseTo(databases.getTotalSpace() - databases.getFreeSpace(), within(DRIFT_SLACK));
  }

  /**
   * The regression guard proper, and it can only be observed on a filesystem that actually reserves blocks - ext4
   * does, APFS and tmpfs need not. Where the reservation is smaller than the reading drift there is nothing to
   * separate the two formulas by, so the test says so rather than passing for the wrong reason.
   */
  @Test
  void theReservedBlocksAreNotChargedToTheDatabases() {
    final File databases = tempDir.resolve("reserved").toFile();
    assertThat(databases.mkdirs()).isTrue();
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databases.getAbsolutePath());

    final long reserved = databases.getFreeSpace() - databases.getUsableSpace();
    assumeTrue(reserved > DRIFT_SLACK,
        "this filesystem reserves no blocks for root, so the two formulas cannot be told apart here");

    final JSONObject json = Profiler.INSTANCE.toJSON();

    final long used = json.getJSONObject("diskUsedSpace").getLong("space");
    final long total = json.getJSONObject("diskTotalSpace").getLong("space");
    final long usable = json.getJSONObject("diskFreeSpace").getLong("space");

    assertThat(used).as("the figure the card used to compute charged the root reservation to the databases")
        .isLessThan(total - usable);
  }

  /**
   * The reading the low-space warning and the percentage are built on is unchanged: "how much room is left" is a
   * different question from "how much is allocated", and its answer is still the space this process can write into.
   */
  @Test
  void theFreeFigureStillReportsUsableSpace() {
    final File databases = tempDir.resolve("free").toFile();
    assertThat(databases.mkdirs()).isTrue();
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databases.getAbsolutePath());

    final JSONObject json = Profiler.INSTANCE.toJSON();

    final long usable = json.getJSONObject("diskFreeSpace").getLong("space");
    final long total = json.getJSONObject("diskTotalSpace").getLong("space");

    assertThat(usable).isPositive().isLessThanOrEqualTo(total);
    assertThat(json.getJSONObject("diskFreeSpacePerc").getFloat("perc")).isEqualTo(usable * 100F / total);
  }
}
