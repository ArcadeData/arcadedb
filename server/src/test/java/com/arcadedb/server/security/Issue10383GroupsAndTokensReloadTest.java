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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for issue #10383, driven through the real watchers: a reload of {@code server-groups.json} or
 * {@code server-api-tokens.json} that cannot use the file keeps what is in force, preserves the file and never writes
 * over it. {@link Issue10383SecurityArtifactReloadConformanceTest} is the census of the four inputs for every artifact;
 * this class covers what the census cannot - the watchers themselves, the first load, and the token store's own paths.
 */
class Issue10383GroupsAndTokensReloadTest {

  private static final String   CONFIG_PATH   = "target/test-security-10383";
  private static final String   ROOT_PASSWORD = "PlayWithData123!";
  private static final Duration AT_MOST       = Duration.ofSeconds(30);
  private static final Duration POLL          = Duration.ofMillis(20);

  private final Path groupsFile      = Path.of(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);
  private final Path groupsErrorFile = Path.of(CONFIG_PATH, SecurityGroupFileRepository.ERROR_FILE_NAME);
  private final Path tokensFile      = Path.of(CONFIG_PATH, ApiTokenConfiguration.FILE_NAME);
  private final Path tokensErrorFile = Path.of(CONFIG_PATH, ApiTokenConfiguration.ERROR_FILE_NAME);

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(ROOT_PASSWORD);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.reset();
    final File dir = new File(CONFIG_PATH);
    dir.setWritable(true);
    FileUtils.deleteRecursively(dir);
  }

  /**
   * The scenario of the issue: an operator's valid hand edit adds a group, a later one drops the "version" key. The
   * watcher used to replace every group with the default AND save the default over the operator's file, with no copy.
   */
  @Test
  void aVersionlessHandEditOfTheGroupsKeepsThemAndLeavesTheFileAlone() throws Exception {
    final AtomicInteger refreshes = new AtomicInteger();
    final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 50).onReload(document -> {
      refreshes.incrementAndGet();
      return null;
    });
    try {
      final JSONObject custom = repository.getGroups().copy();
      groupsOf(custom).put("auditors", new JSONObject().put("resultSetLimit", 10L));
      handEdit(groupsFile, custom.toString(2));
      await().atMost(AT_MOST).pollInterval(POLL).until(() -> groupsOf(repository.getGroups()).has("auditors"));

      final JSONObject versionless = new JSONObject(Files.readString(groupsFile, StandardCharsets.UTF_8));
      versionless.remove("version");
      final String written = versionless.toString(2);
      await().atMost(AT_MOST).pollInterval(POLL).until(() -> refreshes.get() > 0);
      final int refreshesBefore = refreshes.get();
      handEdit(groupsFile, written);
      await().atMost(AT_MOST).pollInterval(POLL).until(() -> Files.exists(groupsErrorFile));

      assertThat(groupsOf(repository.getGroups()).has("auditors")).as("the groups in force are kept").isTrue();
      assertThat(Files.readString(groupsFile, StandardCharsets.UTF_8)).as("the operator's file is not overwritten").isEqualTo(written);
      assertThat(Files.readString(groupsErrorFile, StandardCharsets.UTF_8)).isEqualTo(written);
      assertThat(refreshes.get()).as("no permission refresh for a document that was not published").isEqualTo(refreshesBefore);

      // The watcher survived: the operator's fix - version back, and the group dropped - is applied.
      final JSONObject fixed = new JSONObject(written).put("version", ServerSecurity.LATEST_VERSION);
      groupsOf(fixed).remove("auditors");
      handEdit(groupsFile, fixed.toString(2));
      await().atMost(AT_MOST).pollInterval(POLL).until(() -> !groupsOf(repository.getGroups()).has("auditors"));
    } finally {
      repository.stop();
    }
  }

  /** The first load of a versionless file falls back to the default, as before, but now preserves the file first. */
  @Test
  void aVersionlessGroupsFileAtFirstLoadIsPreservedBeforeTheDefaultReplacesIt() throws Exception {
    final String content = new JSONObject().put("databases",
        new JSONObject().put("*", new JSONObject().put("groups", new JSONObject().put("auditors", new JSONObject())))).toString(2);
    Files.writeString(groupsFile, content, StandardCharsets.UTF_8);

    final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 600_000);
    try {
      final JSONObject groups = repository.getGroups();

      assertThat(groups.getInt("version")).as("the default is in force").isEqualTo(ServerSecurity.LATEST_VERSION);
      assertThat(groupsOf(groups).has("auditors")).isFalse();
      assertThat(Files.readString(groupsErrorFile, StandardCharsets.UTF_8)).as("the operator's file is preserved").isEqualTo(content);
    } finally {
      repository.stop();
    }
  }

  /** {@code createDefault()} builds the document and nothing else: only the first load's fallback saves it. */
  @Test
  void creatingTheDefaultGroupsDocumentDoesNotWriteIt() {
    final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 600_000);
    try {
      assertThat(repository.createDefault().getInt("version")).isEqualTo(ServerSecurity.LATEST_VERSION);
      assertThat(groupsFile).doesNotExist();
    } finally {
      repository.stop();
    }
  }

  /**
   * The token file has a watcher of its own: an edit of it is applied without any change to {@code server-users.jsonl}
   * (it used to be re-read only as a side effect of a users-file reload).
   */
  @Test
  void aHandEditOfTheTokenFileIsPickedUpByItsOwnWatcher() throws Exception {
    final ServerSecurity security = start(50);
    try {
      final String plaintext = security.getApiTokenConfiguration().createToken("ci", "*", 0, new JSONObject()).getString("token");
      assertThat(security.getApiTokenConfiguration().getToken(plaintext)).isNotNull();

      handEdit(tokensFile, new JSONObject().put("version", 1).put("tokens", new JSONArray()).toString(2));

      await().atMost(AT_MOST).pollInterval(POLL).until(() -> security.getApiTokenConfiguration().getToken(plaintext) == null);
    } finally {
      security.stopService();
    }
  }

  /**
   * The half of the issue that outlived the file: a users-file reload that found the token file unreadable emptied the
   * token store, and nothing re-read it after the file was restored.
   */
  @Test
  void aUsersFileReloadNoLongerTouchesTheTokens() throws Exception {
    final ServerSecurity security = start(600_000);
    try {
      final String plaintext = security.getApiTokenConfiguration().createToken("ci", "*", 0, new JSONObject()).getString("token");
      Files.delete(tokensFile);

      security.loadUsers();

      assertThat(security.getApiTokenConfiguration().getToken(plaintext)).as("the token in force still authenticates").isNotNull();
      assertThat(tokensFile).as("and the users reload did not write the token file").doesNotExist();
    } finally {
      security.stopService();
    }
  }

  /** A corrupt token file seen by the watcher keeps the tokens; the watcher survives and applies the next valid edit. */
  @Test
  void aCorruptTokenFileSeenByTheWatcherKeepsTheTokens() throws Exception {
    final ServerSecurity security = start(50);
    try {
      final ApiTokenConfiguration tokens = security.getApiTokenConfiguration();
      final String ci = tokens.createToken("ci", "*", 0, new JSONObject()).getString("token");
      final String backup = tokens.createToken("backup", "*", 0, new JSONObject()).getString("token");
      final String valid = Files.readString(tokensFile, StandardCharsets.UTF_8);

      handEdit(tokensFile, "{\"tokens\": [ truncated");
      await().atMost(AT_MOST).pollInterval(POLL).until(() -> Files.exists(tokensErrorFile));
      assertThat(tokens.getToken(ci)).isNotNull();
      assertThat(tokens.getToken(backup)).isNotNull();
      assertThat(Files.readString(tokensFile, StandardCharsets.UTF_8)).isEqualTo("{\"tokens\": [ truncated");

      // The restored file, with "ci" revoked by hand.
      final JSONObject restored = new JSONObject(valid);
      final JSONArray kept = new JSONArray();
      for (int i = 0; i < restored.getJSONArray("tokens").length(); i++)
        if (!"ci".equals(restored.getJSONArray("tokens").getJSONObject(i).getString("name")))
          kept.put(restored.getJSONArray("tokens").getJSONObject(i));
      handEdit(tokensFile, restored.put("tokens", kept).toString(2));

      await().atMost(AT_MOST).pollInterval(POLL).until(() -> tokens.getToken(ci) == null);
      assertThat(tokens.getToken(backup)).isNotNull();
    } finally {
      security.stopService();
    }
  }

  /**
   * A reload must not re-read a file that is OLDER than the tokens in force: a replicated revocation whose own write
   * failed is in memory only, and re-reading the file would put the revoked token back.
   */
  @Test
  void aReloadDoesNotResurrectATokenRevokedInMemoryWhoseWriteFailed() throws Exception {
    final ApiTokenConfiguration tokens = new ApiTokenConfiguration(CONFIG_PATH);
    tokens.load();
    final String plaintext = tokens.createToken("ci", "*", 0, new JSONObject()).getString("token");
    final String withToken = Files.readString(tokensFile, StandardCharsets.UTF_8);

    final File dir = new File(CONFIG_PATH);
    final PosixFileAttributeView view = Files.getFileAttributeView(dir.toPath(), PosixFileAttributeView.class);
    assumeTrue(view != null, "needs POSIX permissions to make the directory read-only");
    try {
      view.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
      final Exception failure = tokens.applyReplicated(new JSONObject().put("version", 1).put("tokens", new JSONArray()).toString());
      assumeTrue(failure != null, "the directory is still writable (running as a superuser?)");
      assertThat(tokens.getToken(plaintext)).as("the revocation is in force").isNull();

      // The file still holds the token: an operator touching it, or the stamp of the last good write, fires the watcher.
      handEdit(tokensFile, withToken);
      assertThat(tokens.reload()).isFalse();
      assertThat(tokens.getToken(plaintext)).as("the revoked token stays revoked").isNull();
    } finally {
      view.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
    }

    // Once the tokens in force reach the disk, reloads are applied again.
    assertThat(tokens.persistCurrent()).isNull();
    assertThat(tokens.reload()).isTrue();
    assertThat(tokens.getToken(plaintext)).isNull();
  }

  /**
   * At startup a token document without a "tokens" array leaves the store empty (fail-closed), as an unparseable one
   * does - and like it is now preserved, instead of loading silently as the empty set the next save writes over it.
   */
  @Test
  void aTokenFileWithoutTokensAtFirstLoadIsPreserved() throws Exception {
    final String content = new JSONObject().put("version", 1).put("tokenz", new JSONArray()).toString(2);
    Files.writeString(tokensFile, content, StandardCharsets.UTF_8);

    final ApiTokenConfiguration tokens = new ApiTokenConfiguration(CONFIG_PATH);
    tokens.load();

    assertThat(tokens.listTokens()).isEmpty();
    assertThat(Files.readString(tokensErrorFile, StandardCharsets.UTF_8)).isEqualTo(content);
  }

  private ServerSecurity start(final int reloadEveryMs) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, reloadEveryMs);
    final ServerSecurity security = new ServerSecurity(null, configuration, CONFIG_PATH);
    security.loadUsers();
    return security;
  }

  private static JSONObject groupsOf(final JSONObject document) {
    return document.getJSONObject("databases").getJSONObject("*").getJSONObject("groups");
  }

  /** Writes the file and moves its stamp clearly past the last load, so the change cannot fall in the same tick. */
  private static void handEdit(final Path file, final String content) throws Exception {
    final long previous = Files.exists(file) ? Files.getLastModifiedTime(file).toMillis() : 0L;
    Files.writeString(file, content, StandardCharsets.UTF_8);
    Files.setLastModifiedTime(file, FileTime.fromMillis(Math.max(previous, System.currentTimeMillis()) + 2_000));
  }
}
