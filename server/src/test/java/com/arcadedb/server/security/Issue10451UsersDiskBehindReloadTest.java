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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for issue #10451, the users half of #10403: a replicated user list is published in memory even when its
 * write fails (#7137), so the file on disk is then OLDER than the users in force. A reload of that file - fired by anything
 * that advances its stamp, an operator touching it or a backup tool - must not publish it over the replicated list, or a
 * user the cluster just dropped (or a password it just rotated) works again on this node.
 */
class Issue10451UsersDiskBehindReloadTest {

  private static final String   CONFIG_PATH   = "target/test-security-10451";
  private static final String   ROOT_PASSWORD = "PlayWithData123!";
  private static final String   PASSWORD      = "alice-password-secret";
  private static final Duration AT_MOST       = Duration.ofSeconds(30);
  private static final Duration POLL          = Duration.ofMillis(20);
  // Many watcher periods: long enough for a reload that was going to happen to have happened.
  private static final Duration QUIET         = Duration.ofMillis(750);
  private static final int      WATCH_EVERY   = 50;

  private final Path usersFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);

  private PosixFileAttributeView dirView;
  private ServerSecurity         security;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(ROOT_PASSWORD);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists()) {
      dir.setWritable(true);
      FileUtils.deleteRecursively(dir);
    }
    dir.mkdirs();
    dirView = Files.getFileAttributeView(dir.toPath(), PosixFileAttributeView.class);
    assumeTrue(dirView != null, "needs POSIX permissions to make the directory read-only");

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, WATCH_EVERY);
    security = new ServerSecurity(null, configuration, CONFIG_PATH);
    security.loadUsers();
  }

  @AfterEach
  void tearDown() throws Exception {
    makeWritable();
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /** The scenario of the issue: read-only directory, a replicated drop, the file touched, the watcher must keep the drop. */
  @Test
  void theWatcherDoesNotResurrectAUserDroppedByAReplicatedListWhoseWriteFailed() throws Exception {
    security.applyReplicatedUsers(payload(withAlice()));
    assertThat(security.existsUser("alice")).isTrue();
    final String withAliceOnDisk = Files.readString(usersFile, StandardCharsets.UTF_8);
    assertThat(withAliceOnDisk).contains("alice");

    makeReadOnly();
    assumeTrue(applyFails(payload(withoutAlice())), "the directory is still writable (running as a superuser?)");
    assertThat(security.existsUser("alice")).as("the replicated drop is in force").isFalse();

    // The file still holds alice: anything that advances its stamp used to make the watcher swap it back in.
    touch(usersFile);
    await().pollDelay(QUIET).atMost(QUIET.plusSeconds(5)).until(() -> true);
    assertThat(security.existsUser("alice")).as("the watcher must not resurrect the dropped user").isFalse();
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the refusal writes nothing").isEqualTo(withAliceOnDisk);

    // A retry that still fails leaves the disk behind, so the watcher still refuses.
    assertThat(security.retryReplicatedSecurityPersistence()).isFalse();
    touch(usersFile);
    await().pollDelay(QUIET).atMost(QUIET.plusSeconds(5)).until(() -> true);
    assertThat(security.existsUser("alice")).isFalse();

    // Once the users in force reach the disk (the #7752 retry), the file is theirs and a later edit is applied again.
    makeWritable();
    assertThat(security.retryReplicatedSecurityPersistence()).isTrue();
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).doesNotContain("alice");
    assertThat(security.existsUser("alice")).isFalse();

    handEdit(usersFile, withAliceOnDisk);
    await().atMost(AT_MOST).pollInterval(POLL).until(() -> security.existsUser("alice"));
  }

  /** A rotated password: the old one must not work again on this node after a touch. */
  @Test
  void theWatcherDoesNotRestoreAPasswordRotatedByAReplicatedListWhoseWriteFailed() throws Exception {
    security.applyReplicatedUsers(payload(withAlice()));
    assertThat(security.authenticate("alice", PASSWORD, null).getName()).isEqualTo("alice");

    makeReadOnly();
    final List<JSONObject> rotated = withAlice();
    rotated.get(rotated.size() - 1).put("password", security.encodePassword("rotated-password-secret"));
    assumeTrue(applyFails(payload(rotated)), "the directory is still writable (running as a superuser?)");

    touch(usersFile);
    await().pollDelay(QUIET).atMost(QUIET.plusSeconds(5)).until(() -> true);

    assertThat(security.authenticate("alice", "rotated-password-secret", null).getName()).isEqualTo("alice");
    assertThat(passwordWorks("alice", PASSWORD)).as("the rotated-away password must stay rejected").isFalse();
  }

  /**
   * An EMPTY replicated list whose write failed is in force too: it must not read as "nothing in force", which would take
   * the first-load path - the one that never refuses - and publish the older file.
   */
  @Test
  void anEmptyReplicatedListWhoseWriteFailedIsNotUndoneByTheWatcher() throws Exception {
    security.applyReplicatedUsers(payload(withAlice()));
    assertThat(security.existsUser("alice")).isTrue();

    makeReadOnly();
    assumeTrue(applyFails("[]"), "the directory is still writable (running as a superuser?)");
    assertThat(security.getUsers()).isEmpty();

    touch(usersFile);
    await().pollDelay(QUIET).atMost(QUIET.plusSeconds(5)).until(() -> true);
    assertThat(security.existsUser("alice")).as("the watcher must not republish the older file over an empty list").isFalse();
    assertThat(security.existsUser("root")).isFalse();
  }

  /** A successful local save puts the disk level with memory again: the next hand edit is applied. */
  @Test
  void aSuccessfulLocalSaveLiftsTheRefusal() throws Exception {
    security.applyReplicatedUsers(payload(withAlice()));
    final String withAliceOnDisk = Files.readString(usersFile, StandardCharsets.UTF_8);

    makeReadOnly();
    assumeTrue(applyFails(payload(withoutAlice())), "the directory is still writable (running as a superuser?)");

    makeWritable();
    security.saveUsers(security.usersToJSON());
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).doesNotContain("alice");

    handEdit(usersFile, withAliceOnDisk);
    await().atMost(AT_MOST).pollInterval(POLL).until(() -> security.existsUser("alice"));
  }

  /**
   * A local save that FAILS publishes nothing (it throws), so memory and disk still agree and an operator's edit must
   * still be applied: only a replicated install that did not reach the disk refuses reloads.
   */
  @Test
  void aFailedLocalSaveDoesNotRefuseReloads() throws Exception {
    final String rootOnly = Files.readString(usersFile, StandardCharsets.UTF_8);
    makeReadOnly();
    try {
      security.saveUsers(security.usersToJSON());
      assumeTrue(false,"the directory is still writable (running as a superuser?)");
    } catch (final ServerSecurityException expected) {
      // nothing published
    }

    // The directory is read-only, but the file itself is the owner's to rewrite in place.
    handEdit(usersFile, rootOnly + aliceJson().toString() + "\n");
    await().atMost(AT_MOST).pollInterval(POLL).until(() -> security.existsUser("alice"));
  }

  /** A replicated list that reaches the disk refuses nothing either. */
  @Test
  void aPersistedReplicatedListDoesNotRefuseReloads() throws Exception {
    security.applyReplicatedUsers(payload(withoutAlice()));
    final String rootOnly = Files.readString(usersFile, StandardCharsets.UTF_8);

    handEdit(usersFile, rootOnly + aliceJson().toString() + "\n");
    await().atMost(AT_MOST).pollInterval(POLL).until(() -> security.existsUser("alice"));
  }

  /** The repository's half on its own, without the watcher: the flag's lifecycle and the stamp the refusal advances. */
  @Test
  void theRepositoryFlagIsClearedBySaveAndMarkFileSeenSwallowsTheChange() throws Exception {
    final SecurityUserFileRepository repository = new SecurityUserFileRepository(CONFIG_PATH);
    final List<JSONObject> rootOnly = withoutAlice();
    repository.save(rootOnly);
    assertThat(repository.isDiskBehind()).isFalse();

    repository.markDiskBehind();
    assertThat(repository.isDiskBehind()).isTrue();

    touch(usersFile);
    assertThat(repository.isUserFileChanged()).isTrue();
    repository.markFileSeen();
    assertThat(repository.isUserFileChanged()).as("a refused change is not reported again on the next tick").isFalse();

    repository.save(rootOnly);
    assertThat(repository.isDiskBehind()).as("a successful save puts the disk level with memory").isFalse();
  }

  private boolean applyFails(final String payload) {
    try {
      security.applyReplicatedUsers(payload);
      return false;
    } catch (final ReplicatedUsersPersistenceException expected) {
      // the write failed, the list is in force
      return true;
    }
  }

  private boolean passwordWorks(final String user, final String password) {
    try {
      return security.authenticate(user, password, null) != null;
    } catch (final ServerSecurityException e) {
      return false;
    }
  }

  private JSONObject aliceJson() {
    return new JSONObject()
        .put("name", "alice")
        .put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
  }

  private List<JSONObject> withoutAlice() {
    final List<JSONObject> list = new ArrayList<>();
    for (final JSONObject user : security.usersToJSON())
      if (!"alice".equals(user.getString("name")))
        list.add(user);
    return list;
  }

  private List<JSONObject> withAlice() {
    final List<JSONObject> list = withoutAlice();
    list.add(aliceJson());
    return list;
  }

  private static String payload(final List<JSONObject> users) {
    final JSONArray array = new JSONArray();
    for (final JSONObject user : users)
      array.put(user);
    return array.toString();
  }

  private void makeReadOnly() throws Exception {
    dirView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
  }

  private void makeWritable() throws Exception {
    if (dirView != null)
      dirView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
  }

  /** Moves the file's stamp clearly past the last load without changing its content. */
  private static void touch(final Path file) throws Exception {
    final long previous = Files.getLastModifiedTime(file).toMillis();
    Files.setLastModifiedTime(file, FileTime.fromMillis(Math.max(previous, System.currentTimeMillis()) + 2_000));
  }

  /** Writes the file and moves its stamp clearly past the last load, so the change cannot fall in the same tick. */
  private static void handEdit(final Path file, final String content) throws Exception {
    final long previous = Files.exists(file) ? Files.getLastModifiedTime(file).toMillis() : 0L;
    Files.writeString(file, content, StandardCharsets.UTF_8);
    Files.setLastModifiedTime(file, FileTime.fromMillis(Math.max(previous, System.currentTimeMillis()) + 2_000));
  }
}
