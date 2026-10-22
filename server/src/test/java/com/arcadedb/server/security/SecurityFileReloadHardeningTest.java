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
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.awaitility.Awaitility.await;

/**
 * A change an operator makes to a security file on a running server must never be replaced by a permissive default, be
 * ignored for good, or hang the server:
 * <ul>
 *   <li>the first load of the groups, which may fall back to the default and write it, happens at the start, so a watcher
 *       tick can only ever be a reload;</li>
 *   <li>a reloaded users file that leaves root without a password, with none configured, is refused instead of prompting on a
 *       console the server does not have, and a stop does not wait for a reload in progress;</li>
 *   <li>a failed rewrite of the legacy token document the file already holds does not refuse every later reload;</li>
 *   <li>a file that can neither be loaded nor preserved is not replaced by the default.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SecurityFileReloadHardeningTest {
  private static final String CONFIG_PATH   = "target/test-security-reload-hardening";
  private static final String ROOT_PASSWORD = "PlayWithData123!";

  private ServerSecurity security;
  private InputStream    originalIn;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    originalIn = System.in;
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (security != null)
      security.stopService();
    System.setIn(originalIn);
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.reset();
    makeWritable(Path.of(CONFIG_PATH));
    for (final String name : new String[] { SecurityUserFileRepository.FILE_NAME, SecurityGroupFileRepository.FILE_NAME })
      makeWritable(Path.of(CONFIG_PATH, name));
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  @Test
  void aGroupsEditOnARunningServerIsNeverTakenForAFirstRun() throws Exception {
    final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 0);
    final JSONObject document = repository.createDefault();
    document.getJSONObject("databases").getJSONObject("*").getJSONObject("groups").put("auditors",
        new JSONObject().put("access", new JSONArray()).put("types",
            new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    final Path groupsFile = Path.of(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);
    Files.writeString(groupsFile, document.toString(2), StandardCharsets.UTF_8);

    security = newSecurity(100);
    security.startService();

    // NO DATABASE OPENED, NO USER AUTHENTICATED: NOTHING HAS READ THE GROUPS YET, AND AN OPERATOR EDITS THE FILE
    final String edit = new JSONObject().put("databases", new JSONObject()).toString();
    Files.writeString(groupsFile, edit, StandardCharsets.UTF_8);
    groupsFile.toFile().setLastModified(System.currentTimeMillis() + 2_000);

    final Path errorFile = Path.of(CONFIG_PATH, SecurityGroupFileRepository.ERROR_FILE_NAME);
    await().atMost(10, TimeUnit.SECONDS).until(() -> Files.exists(errorFile));

    assertThat(Files.readString(groupsFile, StandardCharsets.UTF_8)).as("the operator's file is not replaced by the default")
        .isEqualTo(edit);
    assertThat(security.getDatabaseGroupsConfiguration("*").has("auditors")).as("the groups in force are kept").isTrue();
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aReloadThatLeavesRootWithoutAPasswordIsRefusedWithoutPrompting() throws Exception {
    final Path usersFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
    security = newSecurity(0);
    final String rootHash = security.encodePassword(ROOT_PASSWORD);
    final JSONObject root = new JSONObject().put("name", "root").put("password", rootHash)
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    final JSONObject alice = new JSONObject().put("name", "alice").put("password", security.encodePassword("AlicePassword1"))
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    new SecurityUserFileRepository(CONFIG_PATH).save(List.of(root, alice));
    security.startService();
    security.loadUsers();
    assertThat(security.getUser("root").getPassword()).isEqualTo(rootHash);

    // NO PASSWORD CONFIGURED, AND THE CONSOLE IS AT ITS END: A PROMPT WOULD LOOP FOREVER
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
    System.setIn(new ByteArrayInputStream(new byte[0]));
    final JSONObject rootWithoutPassword = new JSONObject().put("name", "root")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    final String edit = rootWithoutPassword + "\n" + alice + "\n";
    Files.writeString(usersFile, edit, StandardCharsets.UTF_8);
    usersFile.toFile().setLastModified(System.currentTimeMillis() + 2_000);

    security.loadUsers();

    assertThat(security.getUser("root").getPassword()).as("the users in force are kept").isEqualTo(rootHash);
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the operator's file is left as it is").isEqualTo(edit);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aStopDoesNotWaitForAReloadInProgress() throws Exception {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(ROOT_PASSWORD);
    security = newSecurity(0);
    security.startService();
    security.loadUsers();

    final CountDownLatch inReload = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    security.afterUsersFileRead(() -> {
      inReload.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    final Thread reloader = new Thread(security::loadUsers);
    reloader.start();
    try {
      assertThat(inReload.await(10, TimeUnit.SECONDS)).isTrue();

      final Thread stopper = new Thread(security::stopService);
      stopper.start();
      stopper.join(10_000);
      assertThat(stopper.isAlive()).as("the stop returned while the reload still held the lock").isFalse();
    } finally {
      release.countDown();
      reloader.join(10_000);
      security.afterUsersFileRead(null);
    }
  }

  @Test
  void aFailedRewriteOfALegacyTokenFileDoesNotRefuseLaterReloads() throws Exception {
    final Path dir = Path.of(CONFIG_PATH);
    final Path tokensFile = Path.of(CONFIG_PATH, ApiTokenConfiguration.FILE_NAME);
    final JSONObject legacy = new JSONObject().put("name", "legacy").put("database", "db").put("token", "at-legacy-plaintext-token")
        .put("expiresAt", 0L).put("permissions", new JSONObject());
    Files.writeString(tokensFile, new JSONObject().put("tokens", new JSONArray().put(legacy)).toString(), StandardCharsets.UTF_8);

    final ApiTokenConfiguration tokens = new ApiTokenConfiguration(CONFIG_PATH);
    makeReadOnly(dir);
    try {
      tokens.load();
    } finally {
      makeWritable(dir);
    }
    assumeThat(Files.readString(tokensFile, StandardCharsets.UTF_8)).as("the directory is still writable (running as a superuser?)")
        .contains("at-legacy-plaintext-token");
    assertThat(tokens.listTokens()).hasSize(1);

    // THE OPERATOR REVOKES THE TOKEN BY REPLACING THE FILE
    Files.writeString(tokensFile, new JSONObject().put("tokens", new JSONArray()).toString(), StandardCharsets.UTF_8);
    tokensFile.toFile().setLastModified(System.currentTimeMillis() + 2_000);

    assertThat(tokens.reload()).as("the reload is not refused").isTrue();
    assertThat(tokens.listTokens()).as("the revoked token no longer authenticates").isEmpty();
  }

  @Test
  void anUnreadableUsersFileIsNotReplacedByTheDefault() throws Exception {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(ROOT_PASSWORD);
    final Path usersFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
    final String original = new JSONObject().put("name", "root").put("password", "x").toString() + "\n"
        + new JSONObject().put("name", "alice").put("password", "y") + "\n";
    Files.writeString(usersFile, original, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(usersFile, PosixFilePermissions.fromString("---------"));
    assumeThat(Files.isReadable(usersFile)).as("the file is still readable (running as a superuser?)").isFalse();

    security = newSecurity(0);
    security.startService();
    assertThatThrownBy(security::loadUsers).isInstanceOf(ServerSecurityException.class).hasMessageContaining("cannot be preserved");

    makeWritable(usersFile);
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the operator's file is intact").isEqualTo(original);
  }

  @Test
  void anUnreadableGroupsFileIsNotReplacedByTheDefault() throws Exception {
    final Path groupsFile = Path.of(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);
    final String original = new SecurityGroupFileRepository(CONFIG_PATH, 0).createDefault().toString(2);
    Files.writeString(groupsFile, original, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(groupsFile, PosixFilePermissions.fromString("---------"));
    assumeThat(Files.isReadable(groupsFile)).as("the file is still readable (running as a superuser?)").isFalse();

    security = newSecurity(0);
    assertThatThrownBy(security::startService).isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("cannot be preserved");

    makeWritable(groupsFile);
    assertThat(Files.readString(groupsFile, StandardCharsets.UTF_8)).as("the operator's file is intact").isEqualTo(original);
  }

  private static ServerSecurity newSecurity(final int reloadEveryMs) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, reloadEveryMs);
    return new ServerSecurity(null, cfg, CONFIG_PATH);
  }

  private static void makeReadOnly(final Path dir) throws Exception {
    Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-x------"));
  }

  private static void makeWritable(final Path path) {
    try {
      if (Files.exists(path))
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(Files.isDirectory(path) ? "rwx------" : "rw-------"));
    } catch (final Exception e) {
      // BEST EFFORT, FOR THE CLEANUP
    }
  }
}
