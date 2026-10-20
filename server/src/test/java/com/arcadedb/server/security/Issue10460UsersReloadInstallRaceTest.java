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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for issue #10460: the users watcher reads {@code server-users.jsonl} and publishes what it read under its
 * own lock, but a replicated install ({@code applyReplicatedUsers}, on the Raft apply thread) and a local user change swap
 * or edit the users in force WITHOUT that lock. A reload that had already read the file must not publish it over a change
 * installed after the read, or that change is reverted on this node.
 * <p>
 * The interleaving is forced, not raced: the reload is driven on the test thread through {@code loadUsers()} with the timer
 * watcher off, and the concurrent change runs inside the reload, between its {@code read()} and its {@code publish()}.
 */
class Issue10460UsersReloadInstallRaceTest {

  private static final String CONFIG_PATH   = "target/test-security-10460";
  private static final String ROOT_PASSWORD = "PlayWithData123!";
  private static final String PASSWORD      = "alice-password-secret";

  private final Path usersFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);

  private ServerSecurity security;

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

    final ContextConfiguration configuration = new ContextConfiguration();
    // No timer: every reload in this test is the one it drives, at the moment it chooses.
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 0);
    security = new ServerSecurity(null, configuration, CONFIG_PATH);
    security.loadUsers();
  }

  @AfterEach
  void tearDown() {
    new File(CONFIG_PATH).setWritable(true);
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /** The scenario of the issue, with the install's write succeeding: the reload read alice, the install dropped her. */
  @Test
  void aReloadDoesNotPublishOverAReplicatedDropInstalledAfterItsRead() throws Exception {
    security.applyReplicatedUsers(payload(withAlice()));
    assertThat(security.existsUser("alice")).isTrue();

    final String dropAlice = payload(withoutAlice());
    duringTheNextReload(() -> security.applyReplicatedUsers(dropAlice));
    touch(usersFile);
    security.loadUsers();

    assertThat(security.existsUser("alice")).as("the reload must not swap back the list it read before the drop").isFalse();
    assertThat(security.existsUser("root")).isTrue();
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).doesNotContain("alice");
  }

  /**
   * The same with the install's write FAILING: the worse case, because the #10451 refusal then keeps whatever is in force -
   * which, without the fix, is the list the reload swapped back.
   */
  @Test
  void aReloadDoesNotPublishOverAReplicatedDropWhoseWriteFailedAfterItsRead() throws Exception {
    security.applyReplicatedUsers(payload(withAlice()));
    final PosixFileAttributeView dirView = Files.getFileAttributeView(Path.of(CONFIG_PATH), PosixFileAttributeView.class);
    assumeTrue(dirView != null, "needs POSIX permissions to make the directory read-only");

    final String dropAlice = payload(withoutAlice());
    final AtomicBoolean writeFailed = new AtomicBoolean();
    duringTheNextReload(() -> {
      try {
        dirView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
        security.applyReplicatedUsers(dropAlice);
      } catch (final ReplicatedUsersPersistenceException expected) {
        writeFailed.set(true);
      } catch (final Exception e) {
        throw new AssertionError(e);
      }
    });
    touch(usersFile);
    security.loadUsers();
    assumeTrue(writeFailed.get(), "the directory is still writable (running as a superuser?)");

    assertThat(security.existsUser("alice")).as("the reload must not swap back the list it read before the drop").isFalse();

    // And the next reload is the #10451 refusal, which keeps the drop in force rather than a reverted list.
    touch(usersFile);
    security.loadUsers();
    assertThat(security.existsUser("alice")).isFalse();
    assertThat(passwordWorks("alice", PASSWORD)).isFalse();
  }

  /** A rotated password, installed between the read and the publish: the old one must stay rejected. */
  @Test
  void aReloadDoesNotRestoreAPasswordRotatedByAReplicatedInstallAfterItsRead() throws Exception {
    security.applyReplicatedUsers(payload(withAlice()));
    assertThat(passwordWorks("alice", PASSWORD)).isTrue();

    final List<JSONObject> rotated = withAlice();
    rotated.get(rotated.size() - 1).put("password", security.encodePassword("rotated-password-secret"));
    final String rotate = payload(rotated);
    duringTheNextReload(() -> security.applyReplicatedUsers(rotate));
    touch(usersFile);
    security.loadUsers();

    assertThat(passwordWorks("alice", "rotated-password-secret")).isTrue();
    assertThat(passwordWorks("alice", PASSWORD)).as("the rotated-away password must stay rejected").isFalse();
  }

  /**
   * The first-load branch of a reload: an empty replicated list that reached the disk is "nothing in force", so the reload
   * falls back to the default root. A list installed while that load runs must not be replaced by the fallback - which is
   * why what is in force is captured BEFORE the reload decides it is a first load, not inside the first load.
   */
  @Test
  void aFirstLoadDoesNotPublishItsFallbackOverAReplicatedListInstalledDuringIt() throws Exception {
    security.applyReplicatedUsers("[]");
    assertThat(security.getUsers()).isEmpty();

    final String withAlice = payload(List.of(aliceJson()));
    duringTheNextReload(() -> security.applyReplicatedUsers(withAlice));
    touch(usersFile);
    security.loadUsers();

    assertThat(security.existsUser("alice")).as("the fallback must not replace a list installed during the first load").isTrue();
    assertThat(security.existsUser("root")).isFalse();
  }

  /**
   * The same-shape sibling on a single server: a LOCAL user change saves the file and then edits the users in force, both
   * outside the watcher's lock. A reload that read the file before the save must not publish over the change either - the
   * save stamps the file as seen, so nothing would ever re-read it and the new user would be gone until a restart.
   */
  @Test
  void aReloadDoesNotPublishOverALocalUserCreatedAfterItsRead() throws Exception {
    duringTheNextReload(() -> security.createUser(aliceJson()));
    touch(usersFile);
    security.loadUsers();

    assertThat(security.existsUser("alice")).as("the reload must not drop a user created after its read").isTrue();
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).contains("alice");
  }

  @Test
  void aReloadDoesNotPublishOverALocalUserDroppedAfterItsRead() throws Exception {
    security.createUser(aliceJson());
    duringTheNextReload(() -> security.dropUserLocally("alice"));
    touch(usersFile);
    security.loadUsers();

    assertThat(security.existsUser("alice")).as("the reload must not resurrect a user dropped after its read").isFalse();
  }

  @Test
  void aReloadDoesNotPublishOverALocalPasswordChangeAfterItsRead() throws Exception {
    security.createUser(aliceJson());
    final JSONObject rotated = aliceJson().put("password", security.encodePassword("rotated-password-secret"));
    duringTheNextReload(() -> security.updateUser(rotated));
    touch(usersFile);
    security.loadUsers();

    assertThat(passwordWorks("alice", "rotated-password-secret")).isTrue();
    assertThat(passwordWorks("alice", PASSWORD)).as("the rotated-away password must stay rejected").isFalse();
  }

  /** Nothing in between: the read is published, so the guard does not swallow an ordinary hand edit. */
  @Test
  void aReloadWithNothingInstalledAfterItsReadStillPublishes() throws Exception {
    final String rootOnly = Files.readString(usersFile, StandardCharsets.UTF_8);
    Files.writeString(usersFile, rootOnly + aliceJson().toString() + "\n", StandardCharsets.UTF_8);
    touch(usersFile);
    security.loadUsers();

    assertThat(security.existsUser("alice")).isTrue();
    assertThat(passwordWorks("alice", PASSWORD)).isTrue();
  }

  /** Runs {@code change} once, inside the next reload, after it read the file and before it publishes. */
  private void duringTheNextReload(final Runnable change) {
    final AtomicBoolean ran = new AtomicBoolean();
    security.afterUsersFileRead(() -> {
      if (ran.compareAndSet(false, true))
        change.run();
    });
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

  /** Moves the file's stamp clearly past the last load without changing its content. */
  private static void touch(final Path file) throws Exception {
    final long previous = Files.getLastModifiedTime(file).toMillis();
    Files.setLastModifiedTime(file, FileTime.fromMillis(Math.max(previous, System.currentTimeMillis()) + 2_000));
  }
}
