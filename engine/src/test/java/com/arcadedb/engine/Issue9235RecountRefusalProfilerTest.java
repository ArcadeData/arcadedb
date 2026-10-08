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

import com.arcadedb.Profiler;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9235, the follow-up of #8649: the {@code count()} recomputes a bucket could not cache
 * because a replicated apply wrote it without its lock were counted per bucket and per database, but never reached
 * {@link Profiler}, so neither the JSON profiler output nor the Prometheus export carried them.
 * <p>
 * The refusal is driven through the bucket's real publish path, not by bumping the database counter directly, so the
 * test covers the whole chain from {@code LocalBucket} to the exported total. Assertions are {@code >=} deltas:
 * {@code Profiler.INSTANCE} is a JVM singleton and anything else in the same fork contributes to it too.
 */
class Issue9235RecountRefusalProfilerTest {

  private static final String DB_PATH = "target/databases/issue9235-recount-refusal-profiler";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void refusedRecountsReachTheProfilerAndSurviveTheDatabaseClose() {
    final long before = profilerCount();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.getSchema().createDocumentType("Counted");
        db.transaction(() -> {
          for (int i = 0; i < 10; i++)
            db.newDocument("Counted").set("name", "record-" + i).save();
        });
        final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();

        // What an unlocked replicated apply does to a recompute that overlapped it (#8640/#8649): the stamp the scan
        // read at its start is no longer current, so the publish is refused
        for (int i = 0; i < 2; i++) {
          bucket.invalidateCachedRecordCountForUnlockedApply();
          assertThat(bucket.publishRecomputedCount(10, bucket.getUnlockedApplyStamp() - 1)).isFalse();
        }
        assertThat(bucket.getRecountPublishesRefused()).isEqualTo(2);

        final long open = profilerCount();
        assertThat(open).as("the refused recounts of an open database must reach the profiler")
            .isGreaterThanOrEqualTo(before + 2);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Profiler.INSTANCE.dumpMetrics(new PrintStream(out));
        assertThat(out.toString()).contains("recountPublishesRefused=");

        db.close();

        // #5636: a monotonic total that went back down on a close would read as a counter reset in Prometheus
        assertThat(profilerCount()).as("closing the database must not rewind the refused-recount total")
            .isGreaterThanOrEqualTo(open);
      } finally {
        // A failed assertion above must not leave the database open for the @AfterEach delete
        if (db.isOpen())
          db.close();
      }
    }
  }

  private static long profilerCount() {
    return Profiler.INSTANCE.toJSON().getJSONObject("recountPublishesRefused").getLong("count", -1L);
  }
}
