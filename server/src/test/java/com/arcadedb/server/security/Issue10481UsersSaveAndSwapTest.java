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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10481: a local user change saves a snapshot of the users and only then swaps the change into memory, and a replicated install
 * ({@code applyReplicatedUsers}, on the Raft apply thread) does the same pair of steps without the monitor. Landing between the other's
 * save and swap, each wrote its list over the other's save and swapped its own in: memory ended with one list and
 * {@code server-users.jsonl} with the other, until the next successful save, and a restart in between lost the change on this node.
 * <p>
 * The interleavings are forced, not raced: the second party starts inside a test seam that sits between the first one's save and swap,
 * and the test waits until it has either finished (no protection) or is parked (the two are one step). What must hold in both orders is
 * that the file and the users in force are the same list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10481UsersSaveAndSwapTest {

  private static final String CONFIG_PATH   = "target/test-security-10481";
  private static final String ROOT_PASSWORD = "PlayWithData123!";
  private static final String PASSWORD      = "user-password-secret";

  private final Path usersFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);

  private ServerSecurity security;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(ROOT_PASSWORD);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();

    final ContextConfiguration configuration = new ContextConfiguration();
    // No timer: nothing but this test touches the users in force
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 0);
    security = new ServerSecurity(null, configuration, CONFIG_PATH);
    security.loadUsers();
  }

  @AfterEach
  void tearDown() {
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /** The scenario of the issue: an install lands between a local create's save and its swap. */
  @Test
  void aReplicatedInstallBetweenALocalSaveAndItsSwapLeavesTheFileAndMemoryTheSame() throws Exception {
    final String install = payload(List.of(userJson("root"), userJson("carol")));
    final Thread[] installer = new Thread[1];
    afterTheNextLocalSave(() -> installer[0] = runAndWaitUntilDoneOrParked(() -> security.applyReplicatedUsers(install)));

    security.createUser(userJson("bob"));
    installer[0].join(10_000);

    assertThat(installer[0].isAlive()).isFalse();
    assertThat(usersInForce()).isEqualTo(usersInFile());
    // The install is the later of the two steps, and replaces the list
    assertThat(usersInForce()).containsExactly("carol", "root");
  }

  /** The same, for the drop of a user. */
  @Test
  void aReplicatedInstallBetweenALocalDropAndItsSwapLeavesTheFileAndMemoryTheSame() throws Exception {
    security.createUser(userJson("bob"));
    final String install = payload(List.of(userJson("root"), userJson("bob"), userJson("carol")));
    final Thread[] installer = new Thread[1];
    afterTheNextLocalSave(() -> installer[0] = runAndWaitUntilDoneOrParked(() -> security.applyReplicatedUsers(install)));

    security.dropUserLocally("bob");
    installer[0].join(10_000);

    assertThat(installer[0].isAlive()).isFalse();
    assertThat(usersInForce()).isEqualTo(usersInFile());
  }

  /** The other order: a local change starts between an install's save and its swap. */
  @Test
  void aLocalChangeBetweenAnInstallSaveAndItsSwapLeavesTheFileAndMemoryTheSame() throws Exception {
    final String install = payload(List.of(userJson("root"), userJson("carol")));
    final Thread[] changer = new Thread[1];
    beforeTheNextReplicatedSwap(() -> changer[0] = runAndWaitUntilDoneOrParked(() -> security.createUser(userJson("bob"))));

    security.applyReplicatedUsers(install);
    changer[0].join(10_000);

    assertThat(changer[0].isAlive()).isFalse();
    assertThat(usersInForce()).isEqualTo(usersInFile());
    // The local change is the later of the two steps and is made on the installed list
    assertThat(usersInForce()).containsExactly("bob", "carol", "root");
  }

  /** Nothing in between: the pair is untouched by the guard that makes it one step. */
  @Test
  void aLocalChangeAndAnInstallOneAfterTheOtherAreBothKept() throws Exception {
    security.createUser(userJson("bob"));
    assertThat(usersInForce()).isEqualTo(usersInFile()).containsExactly("bob", "root");

    security.applyReplicatedUsers(payload(List.of(userJson("root"), userJson("bob"), userJson("carol"))));
    assertThat(usersInForce()).isEqualTo(usersInFile()).containsExactly("bob", "carol", "root");

    security.dropUserLocally("bob");
    assertThat(usersInForce()).isEqualTo(usersInFile()).containsExactly("carol", "root");
  }

  /**
   * Starts {@code action} on a thread and waits until it is done or cannot go on: parked on a lock held by the caller, which is what
   * the protection of the pair looks like from outside. Without it the action runs to the end here, which is the interleaving of the
   * issue.
   */
  private static Thread runAndWaitUntilDoneOrParked(final Runnable action) {
    final Thread thread = new Thread(action, "issue-10481-second-party");
    thread.setDaemon(true);
    thread.start();
    final long deadline = System.nanoTime() + 10_000_000_000L;
    while (System.nanoTime() < deadline) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.TERMINATED || state == Thread.State.BLOCKED || state == Thread.State.WAITING
          || state == Thread.State.TIMED_WAITING)
        break;
      Thread.onSpinWait();
    }
    return thread;
  }

  /** Runs {@code action} once, in the next local change, after it saved the users and before it swaps them in. */
  private void afterTheNextLocalSave(final Runnable action) {
    final AtomicBoolean ran = new AtomicBoolean();
    security.afterLocalUsersSave(() -> {
      if (ran.compareAndSet(false, true))
        action.run();
    });
  }

  /** Runs {@code action} once, in the next replicated install, after it saved the list and before it swaps it in. */
  private void beforeTheNextReplicatedSwap(final Runnable action) {
    final AtomicBoolean ran = new AtomicBoolean();
    security.beforeReplicatedUsersSwap(() -> {
      if (ran.compareAndSet(false, true))
        action.run();
    });
  }

  private Set<String> usersInForce() {
    return new TreeSet<>(security.getUsers());
  }

  private Set<String> usersInFile() throws Exception {
    final Set<String> names = new TreeSet<>();
    for (final String line : Files.readAllLines(usersFile, StandardCharsets.UTF_8))
      if (!line.isBlank())
        names.add(new JSONObject(line).getString("name"));
    return names;
  }

  private JSONObject userJson(final String name) {
    if ("root".equals(name))
      for (final JSONObject user : security.usersToJSON())
        if ("root".equals(user.getString("name")))
          return user;
    return new JSONObject()
        .put("name", name)
        .put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
  }

  private static String payload(final List<JSONObject> users) {
    final JSONArray array = new JSONArray();
    for (final JSONObject user : new ArrayList<>(users))
      array.put(user);
    return array.toString();
  }
}
