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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7937: two threads renaming the SAME type to two different names interleaved over the
 * unsynchronised name capture and bucket renames of {@code LocalDocumentType.rename()}. The renames are now
 * serialised by a per-type lock, so the second starts from whatever name the first left: both succeed one after the
 * other and the schema ends up consistent, with the type reachable under exactly one name and its data intact.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7937ConcurrentRenameOfSameTypeTest extends TestHelper {
  private static final int ROUNDS = 25;

  @Test
  void concurrentRenamesOfOneVertexTypeLeaveAConsistentSchema() throws Exception {
    runRounds(true);
  }

  @Test
  void concurrentRenamesOfOneDocumentTypeLeaveAConsistentSchema() throws Exception {
    runRounds(false);
  }

  private void runRounds(final boolean vertex) throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < ROUNDS; round++) {
        final String start = "Start" + round;
        final String first = "First" + round;
        final String second = "Second" + round;

        final DocumentType type;
        if (vertex)
          type = database.getSchema().createVertexType(start, 1);
        else
          type = database.getSchema().createDocumentType(start, 1);
        database.transaction(() -> {
          for (int i = 0; i < 10; i++)
            if (vertex)
              database.newVertex(start).set("id", i).save();
            else
              database.newDocument(start).set("id", i).save();
        });

        final CountDownLatch go = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();
        for (final String target : new String[] { first, second })
          futures.add(executor.submit(() -> {
            go.await();
            type.rename(target);
            return null;
          }));
        go.countDown();
        for (final Future<?> future : futures)
          future.get(60, TimeUnit.SECONDS);

        // Serialised: the last renamer wins, and the type answers to that single name
        final String finalName = type.getName();
        assertThat(finalName).isIn(first, second);
        assertThat(database.getSchema().existsType(start)).isFalse();
        assertThat(database.getSchema().existsType(first.equals(finalName) ? second : first)).isFalse();
        assertThat(database.getSchema().getType(finalName)).isSameAs(type);

        // Every bucket answers to the name it is registered under, and follows the type's final name
        for (final Bucket bucket : type.getBuckets(false)) {
          assertThat(database.getSchema().existsBucket(bucket.getName())).isTrue();
          assertThat(database.getSchema().getBucketByName(bucket.getName())).isSameAs(bucket);
          assertThat(bucket.getName()).startsWith(finalName + "_");
        }

        assertThat(database.countType(finalName, false)).isEqualTo(10L);
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
