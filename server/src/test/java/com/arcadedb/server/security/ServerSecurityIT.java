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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ServerSecurityIT {

  private static final String PASSWORD = "dD5ed08c";

  @Test
  void shouldCreateDefaultRootUserAndPersistsSecurityConfigurationFromSetting() throws Exception {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(PASSWORD);

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");
    security.startService();
    security.loadUsers();

    final Path securityConfPath = Path.of("./target", SecurityUserFileRepository.FILE_NAME);
    final File securityConf = securityConfPath.toFile();

    assertThat(securityConf.exists()).isTrue();

    final SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

    final List<JSONObject> jsonl = repository.load();

    assertThat(jsonl.size()).isEqualTo(1);
    assertThat(jsonl.getFirst().getString("name")).isEqualTo("root");
    passwordShouldMatch(security, PASSWORD, jsonl.getFirst().getString("password"));
  }

  @Test
  void shouldCreateDefaultRootUserAndPersistsSecurityConfigurationFromUserInput() throws Exception {
    final Path securityConfPath = Path.of("./target", SecurityUserFileRepository.FILE_NAME);
    Files.deleteIfExists(securityConfPath);

    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);

    if (System.console() != null) {
      System.console().writer().println(PASSWORD + "\r\n" + PASSWORD + "\n");
    } else {
      final InputStream is = new ByteArrayInputStream((PASSWORD + "\r\n" + PASSWORD + "\n").getBytes());
      System.setIn(is);
    }

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");
    security.startService();
    security.loadUsers();

    final File securityConf = securityConfPath.toFile();

    assertThat(securityConf.exists()).isTrue();

    final SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

    final List<JSONObject> jsonl = repository.load();

    assertThat(jsonl.size()).isEqualTo(1);
    assertThat(jsonl.getFirst().getString("name")).isEqualTo("root");
    passwordShouldMatch(security, PASSWORD, jsonl.getFirst().getString("password"));
  }

  @Test
  void shouldLoadProvidedSecurityConfiguration() throws Exception {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(PASSWORD);

    final SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");

    final JSONObject json = new JSONObject().put("name", "providedUser").put("password", security.encodePassword("MyPassword12345"))
        .put("databases", new JSONObject());

    repository.save(List.of(json));

    //when
    security.startService();
    security.loadUsers();

    assertThat(security.existsUser("providedUser")).isTrue();
    assertThat(security.existsUser("root")).isFalse();
    passwordShouldMatch(security, "MyPassword12345", security.getUser("providedUser").getPassword());
  }

  @Test
  void shouldReloadSecurityConfiguration() throws Exception {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(PASSWORD);

    final SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 200);

    final ServerSecurity security = new ServerSecurity(null, cfg, "./target");

    final JSONObject json = new JSONObject().put("name", "providedUser").put("password", security.encodePassword("MyPassword12345"))
        .put("databases", new JSONObject().put("dbtest", new JSONObject()));

    repository.save(List.of(json));

    //when
    security.startService();
    security.loadUsers();

    assertThat(security.existsUser("providedUser")).isTrue();

    ServerSecurityUser user2 = security.getUser("providedUser");
    assertThat(user2.getName()).isEqualTo("providedUser");

    assertThat(security.existsUser("root")).isFalse();
    passwordShouldMatch(security, "MyPassword12345", security.getUser("providedUser").getPassword());

    // RESET USERS ACCESSING DIRECTLY TO THE FILE
    repository.save(SecurityUserFileRepository.createDefault());

    await().atMost(10, SECONDS).until(()-> !security.existsUser("providedUser"));

  }

  @Test
  void dropUserIsThreadSafeUnderConcurrentAccess() throws Exception {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(PASSWORD);

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");
    security.startService();
    security.loadUsers();

    final int threadCount = 10;
    final int operationsPerThread = 20;
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final List<Future<?>> futures = new ArrayList<>();
    final AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      futures.add(executor.submit(() -> {
        for (int i = 0; i < operationsPerThread; i++) {
          final String userName = "threaduser_" + threadId + "_" + i;
          try {
            security.createUser(new JSONObject()
                .put("name", userName)
                .put("password", security.encodePassword(PASSWORD))
                .put("databases", new JSONObject()));
            security.dropUser(userName);
            // Verify user is gone
            assertThat(security.existsUser(userName)).isFalse();
          } catch (final Exception e) {
            errors.incrementAndGet();
          }
        }
      }));
    }

    executor.shutdown();
    assertThat(executor.awaitTermination(30, SECONDS)).isTrue();

    for (final Future<?> future : futures)
      future.get(); // Propagate any assertion errors

    assertThat(errors.get()).isEqualTo(0);
  }

  @Test
  void checkPasswordHash() {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");
    security.startService();

    assertThat(security.encodePassword("ThisIsATest", "ThisIsTheSalt")).isEqualTo("PBKDF2WithHmacSHA256$65536$ThisIsTheSalt$wIKUzWYH72cKJRnFZ0PTSevERtwZTNdN+W4/Fd7xBvw=");
    assertThat(security.encodePassword("ThisIsATest", "ThisIsTheSalt")).isEqualTo("PBKDF2WithHmacSHA256$65536$ThisIsTheSalt$wIKUzWYH72cKJRnFZ0PTSevERtwZTNdN+W4/Fd7xBvw=");

    for (int i = 0; i < 1000000; ++i) {
      assertThat(ServerSecurity.generateRandomSalt().contains("$")).isFalse();
    }

    security.stopService();
  }

  private void passwordShouldMatch(final ServerSecurity security, final String password, final String expectedHash) {
    assertThat(security.passwordMatch(password, expectedHash)).isTrue();
  }

  @BeforeEach
  void beforeAll() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
  }

  @AfterEach
  void afterAll() {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);

    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));

    TestServerHelper.checkActiveDatabases();
    TestServerHelper.deleteDatabaseFolders(1);
    GlobalConfiguration.resetAll();
  }
}
