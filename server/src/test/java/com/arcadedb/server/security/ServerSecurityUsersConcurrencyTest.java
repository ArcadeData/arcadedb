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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ServerSecurityUsersConcurrencyTest {
  private static final String TEST_CONFIG_PATH = "target/test-security-store";
  private static final int    USER_COUNT       = 8;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE.setValue(256);
    final File dir = new File(TEST_CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE.reset();
    FileUtils.deleteRecursively(new File(TEST_CONFIG_PATH));
  }

  private static String plainPassword(final int i) {
    return "pwd-" + i + "-secret";
  }

  private String buildPayload(final ServerSecurity security) {
    final JSONArray payload = new JSONArray();
    for (int i = 0; i < USER_COUNT; i++)
      payload.put(new JSONObject()
          .put("name", "user" + i)
          .put("password", security.encodePassword(plainPassword(i)))
          .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))));
    return payload.toString();
  }

  @Test
  void authenticateNeverSpuriouslyFailsWhileUsersMapIsRebuilt() throws Exception {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), TEST_CONFIG_PATH);
    final String payload = buildPayload(security);
    security.applyReplicatedUsers(payload); // prime the in-memory map

    final int readers = 8;
    final int writers = 3;
    final int readerIterations = 3000;
    final int writerIterations = 300;

    final ExecutorService pool = Executors.newFixedThreadPool(readers + writers);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    for (int r = 0; r < readers; r++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < readerIterations && failure.get() == null; i++) {
            final int k = i % USER_COUNT;
            // authenticate against the in-memory map while writers rebuild it; must never see a
            // torn/empty snapshot (which would surface as ServerSecurityException "User/Password not valid")
            final ServerSecurityUser u = security.authenticate("user" + k, plainPassword(k), null);
            assertThat(u.getName()).isEqualTo("user" + k);
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });
    }

    for (int w = 0; w < writers; w++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < writerIterations && failure.get() == null; i++)
            security.applyReplicatedUsers(payload); // full rebuild + atomic publish
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });
    }

    start.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(90, SECONDS)).isTrue();
    if (failure.get() != null)
      throw new AssertionError("Spurious auth failure or lost user during concurrent rebuild", failure.get());

    // all users survived the churn
    assertThat(security.getUsers()).hasSize(USER_COUNT);
  }

  @Test
  void failedSaveSurfacesToCallerInsteadOfFalseSuccess() throws Exception {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), TEST_CONFIG_PATH);

    // Obstruct the target file with a non-empty directory of the same name so the atomic rename fails.
    final File obstruction = new File(TEST_CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
    assertThat(obstruction.mkdirs()).isTrue();
    assertThat(new File(obstruction, "keep").createNewFile()).isTrue();

    final JSONObject config = new JSONObject()
        .put("name", "alice")
        .put("password", security.encodePassword("alice-pwd"))
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));

    // The swallowed-IOException bug returned success even though nothing was persisted; the fix must
    // propagate the failure to the caller.
    assertThatExceptionOfType(ServerSecurityException.class).isThrownBy(() -> security.createUser(config));
  }
}
