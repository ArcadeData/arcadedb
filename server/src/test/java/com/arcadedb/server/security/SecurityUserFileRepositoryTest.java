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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class SecurityUserFileRepositoryTest {
  private static final String TEST_CONFIG_PATH = "target/test-security-users";

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

  private static List<JSONObject> users(final int count) {
    final List<JSONObject> list = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      list.add(new JSONObject().put("name", "user" + i)
          .put("password", "secret-hash-" + i)
          .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))));
    return list;
  }

  @Test
  void saveRoundTripsAllLines() throws Exception {
    final SecurityUserFileRepository repo = new SecurityUserFileRepository(TEST_CONFIG_PATH);
    repo.save(users(5));

    final List<JSONObject> reloaded = new SecurityUserFileRepository(TEST_CONFIG_PATH).getUsers();
    assertThat(reloaded).hasSize(5);
    assertThat(reloaded.get(0).getString("name")).isEqualTo("user0");
    assertThat(reloaded.get(4).getString("name")).isEqualTo("user4");
  }

  @Test
  void saveLeavesNoTempFilesBehind() throws Exception {
    final SecurityUserFileRepository repo = new SecurityUserFileRepository(TEST_CONFIG_PATH);
    repo.save(users(3));

    final File[] files = new File(TEST_CONFIG_PATH).listFiles();
    assertThat(files).isNotNull();
    for (final File f : files)
      assertThat(f.getName()).endsWith(SecurityUserFileRepository.FILE_NAME);
  }

  @Test
  void saveSetsOwnerOnlyPermissionsOnPosix() throws Exception {
    final SecurityUserFileRepository repo = new SecurityUserFileRepository(TEST_CONFIG_PATH);
    repo.save(users(2));

    final Path file = new File(TEST_CONFIG_PATH, SecurityUserFileRepository.FILE_NAME).toPath();
    final PosixFileAttributeView view = Files.getFileAttributeView(file, PosixFileAttributeView.class);
    if (view == null)
      return; // non-POSIX filesystem (e.g. Windows): nothing to assert

    final Set<PosixFilePermission> perms = view.readAttributes().permissions();
    assertThat(perms).containsExactlyInAnyOrder(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
  }

  @Test
  void concurrentSavesNeverProducePartialFile() throws Exception {
    final SecurityUserFileRepository repo = new SecurityUserFileRepository(TEST_CONFIG_PATH);
    // seed a valid file so readers always have something to read
    repo.save(users(10));

    final int writers = 6;
    final int readers = 6;
    final int iterations = 300;
    final ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    for (int w = 0; w < writers; w++) {
      final int size = 5 + w; // distinct, always-valid payloads
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < iterations && failure.get() == null; i++)
            repo.save(users(size));
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
            final List<JSONObject> loaded = new SecurityUserFileRepository(TEST_CONFIG_PATH).getUsers();
            // every observed snapshot must be a complete, parseable, non-empty file
            assertThat(loaded).isNotEmpty();
            for (final JSONObject u : loaded)
              assertThat(u.getString("name")).startsWith("user");
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });
    }

    start.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(60, SECONDS)).isTrue();
    if (failure.get() != null)
      throw new AssertionError("Concurrent save/load observed a corrupt or partial file", failure.get());
  }
}
