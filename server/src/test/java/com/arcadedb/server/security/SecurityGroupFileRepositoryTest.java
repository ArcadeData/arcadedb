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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class SecurityGroupFileRepositoryTest {
  private static final String TEST_CONFIG_PATH = "target/test-security-groups";

  @BeforeEach
  void setUp() {
    final File dir = new File(TEST_CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(TEST_CONFIG_PATH));
  }

  private static JSONObject groups(final int marker) {
    return new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put("*", new JSONObject()
            .put("groups", new JSONObject().put("admin", new JSONObject()
                .put("marker", marker)
                .put("access", new JSONArray().put("updateSecurity"))))));
  }

  @Test
  void saveRoundTrips() throws Exception {
    final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(TEST_CONFIG_PATH, 1_000_000);
    repo.save(groups(42));

    final SecurityGroupFileRepository repo2 = new SecurityGroupFileRepository(TEST_CONFIG_PATH, 1_000_000);
    final JSONObject loaded = repo2.getGroups();
    assertThat(loaded.getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
        .getJSONObject("admin").getInt("marker")).isEqualTo(42);
    repo.stop();
    repo2.stop();
  }

  @Test
  void saveLeavesNoTempFilesBehind() throws Exception {
    final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(TEST_CONFIG_PATH, 1_000_000);
    repo.save(groups(1));

    final File[] files = new File(TEST_CONFIG_PATH).listFiles();
    assertThat(files).isNotNull();
    for (final File f : files)
      assertThat(f.getName()).isEqualTo(SecurityGroupFileRepository.FILE_NAME);
    repo.stop();
  }

  @Test
  void concurrentSavesNeverProduceCorruptFile() throws Exception {
    final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(TEST_CONFIG_PATH, 1_000_000);
    repo.save(groups(0));

    final int writers = 6;
    final int readers = 6;
    final int iterations = 300;
    final ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    for (int w = 0; w < writers; w++) {
      final int marker = w;
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < iterations && failure.get() == null; i++)
            repo.save(groups(marker));
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });
    }

    for (int r = 0; r < readers; r++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < iterations && failure.get() == null; i++) {
            final SecurityGroupFileRepository reader = new SecurityGroupFileRepository(TEST_CONFIG_PATH, 1_000_000);
            final JSONObject loaded = reader.getGroups();
            // must always be a complete, parseable config with the expected structure
            assertThat(loaded.getJSONObject("databases").getJSONObject("*").has("groups")).isTrue();
            reader.stop();
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });
    }

    start.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(60, SECONDS)).isTrue();
    repo.stop();
    if (failure.get() != null)
      throw new AssertionError("Concurrent save/load observed a corrupt group file", failure.get());
  }
}
