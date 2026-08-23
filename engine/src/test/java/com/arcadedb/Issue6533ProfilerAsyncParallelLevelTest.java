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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6533: {@code Profiler.collectDatabaseStats()} folded every open database's stat into
 * one accumulator with {@code +=}, except {@code asyncParallelLevel}, which was a plain assignment. With more than
 * one database open, the exported reading was whichever database the identity-hashed iteration visited last - not a
 * sum - and could change between consecutive scrapes of an otherwise unchanged server, since {@code IdentityHashMap}
 * iteration order depends on identity hash codes that vary per run.
 * <p>
 * Asserts both that the reading is the SUM across the open databases (not one of them) and that it is STABLE across
 * repeated {@code toJSON()} calls, since the failure mode this guards against is instability, not merely a wrong
 * one-off value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6533ProfilerAsyncParallelLevelTest {

  private static final String DB_PATH_1 = "target/databases/issue6533-profiler-async-parallel-1";
  private static final String DB_PATH_2 = "target/databases/issue6533-profiler-async-parallel-2";

  @Test
  void asyncParallelLevelIsTheSumAcrossOpenDatabasesAndStaysStable() {
    FileUtils.deleteRecursively(new File(DB_PATH_1));
    FileUtils.deleteRecursively(new File(DB_PATH_2));

    try (final DatabaseFactory factory1 = new DatabaseFactory(DB_PATH_1);
         final DatabaseFactory factory2 = new DatabaseFactory(DB_PATH_2)) {

      final long baseline = profilerAsyncParallelLevel();

      final Database db1 = factory1.create();
      final Database db2 = factory2.create();
      try {
        final DatabaseAsyncExecutorImpl async1 = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) db1).async();
        final DatabaseAsyncExecutorImpl async2 = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) db2).async();

        // Two DIFFERENT levels, so an assignment that merely picked "whichever was visited last" would read as one
        // of these two values rather than their sum, and could flip between them across scrapes.
        async1.setParallelLevel(3);
        async2.setParallelLevel(5);

        final long expected = baseline + 3 + 5;

        // Read several times: the bug was not a wrong single snapshot but an UNSTABLE one across scrapes of an
        // unchanged server (IdentityHashMap iteration order depends on identity hash codes).
        for (int i = 0; i < 5; i++)
          assertThat(profilerAsyncParallelLevel())
              .as("asyncParallelLevel must be the sum across every open database, not whichever one iteration " +
                  "happened to visit last, and must stay stable across repeated scrapes")
              .isEqualTo(expected);
      } finally {
        db1.drop();
        db2.drop();
      }

      assertThat(profilerAsyncParallelLevel())
          .as("closing both databases must remove their contribution from the JVM-wide reading")
          .isEqualTo(baseline);
    }
  }

  private static long profilerAsyncParallelLevel() {
    return Profiler.INSTANCE.toJSON().getJSONObject("asyncParallelLevel").getLong("count", -1L);
  }
}
