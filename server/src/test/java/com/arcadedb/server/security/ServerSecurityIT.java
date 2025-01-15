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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ServerSecurityIT {

  private static final String PASSWORD = "dD5ed08c";

  @Test
  void shouldCreateDefaultRootUserAndPersistsSecurityConfigurationFromSetting() throws IOException {
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
    assertThat(jsonl.get(0).getString("name")).isEqualTo("root");
    passwordShouldMatch(security, PASSWORD, jsonl.get(0).getString("password"));
  }

  @Test
  void shouldCreateDefaultRootUserAndPersistsSecurityConfigurationFromUserInput() throws IOException {
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
    assertThat(jsonl.get(0).getString("name")).isEqualTo("root");
    passwordShouldMatch(security, PASSWORD, jsonl.get(0).getString("password"));
  }

  @Test
  void shouldLoadProvidedSecurityConfiguration() throws IOException {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(PASSWORD);

    final SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");

    final JSONObject json = new JSONObject().put("name", "providedUser").put("password", security.encodePassword("MyPassword12345"))
        .put("databases", new JSONObject());

    repository.save(Collections.singletonList(json));

    //when
    security.startService();
    security.loadUsers();

    assertThat(security.existsUser("providedUser")).isTrue();
    assertThat(security.existsUser("root")).isFalse();
    passwordShouldMatch(security, "MyPassword12345", security.getUser("providedUser").getPassword());
  }

  @Test
  void shouldReloadSecurityConfiguration() throws IOException {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(PASSWORD);

    final SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 200);

    final ServerSecurity security = new ServerSecurity(null, cfg, "./target");

    final JSONObject json = new JSONObject().put("name", "providedUser").put("password", security.encodePassword("MyPassword12345"))
        .put("databases", new JSONObject().put("dbtest", new JSONObject()));

    repository.save(Collections.singletonList(json));

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
  public void checkPasswordHash() {
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
  public void beforeAll() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
  }

  @AfterEach
  public void afterAll() {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);

    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));

    TestServerHelper.checkActiveDatabases();
    TestServerHelper.deleteDatabaseFolders(1);
    GlobalConfiguration.resetAll();
  }
}
