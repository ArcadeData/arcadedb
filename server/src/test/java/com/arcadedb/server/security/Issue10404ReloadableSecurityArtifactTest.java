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
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #10404: the reload policy of the users, groups and API-token files lives in one class,
 * {@link ReloadableSecurityArtifact}, instead of three. The census in {@code Issue10383SecurityArtifactReloadConformanceTest}
 * pins the policy's outcome for each store; this test pins the shared class itself and what moving each store onto it
 * changed:
 * <ul>
 *   <li>the policy, driven through a minimal store over a real file;</li>
 *   <li>the users watcher is the shared one - a named daemon thread instead of an anonymous non-daemon {@code Timer} - and
 *       still picks up a hand edit;</li>
 *   <li>a non-positive reload period schedules no watcher for any store: the users and groups ones used to hand it to
 *       {@code Timer.schedule}, which throws on it.</li>
 * </ul>
 */
class Issue10404ReloadableSecurityArtifactTest {

  private static final String CONFIG_PATH   = "target/test-security-10404";
  private static final String ROOT_PASSWORD = "PlayWithData123!";

  private final File dir       = new File(CONFIG_PATH);
  private final File file      = new File(dir, "store.json");
  private final File errorFile = new File(dir, "store-error.json");

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(ROOT_PASSWORD);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.reset();
    FileUtils.deleteRecursively(dir);
  }

  /** A store whose file is a JSON object with a "names" array; the fallback is ["default"]. */
  private final class NamesStore extends ReloadableSecurityArtifact<List<String>> {
    volatile List<String> inForce;
    volatile boolean      refuse;
    volatile boolean      keepOnFirstLoadFailure;
    final AtomicInteger   reloaded = new AtomicInteger();

    NamesStore() {
      this(new Object());
    }

    NamesStore(final Object lock) {
      super(Issue10404ReloadableSecurityArtifactTest.this, lock, Issue10404ReloadableSecurityArtifactTest.this.file,
          Issue10404ReloadableSecurityArtifactTest.this.errorFile, "arcadedb-test-10404-watcher", "Test store");
    }

    @Override
    protected List<String> read() throws IOException {
      final JSONObject json = new JSONObject(Files.readString(file.toPath(), StandardCharsets.UTF_8));
      if (!json.has("names"))
        throw new JSONException("no names");
      final List<String> names = new ArrayList<>();
      final JSONArray array = json.getJSONArray("names");
      for (int i = 0; i < array.length(); i++)
        names.add(array.getString(i));
      return names;
    }

    @Override
    protected List<String> fallback() {
      return List.of("default");
    }

    @Override
    protected boolean publish(final List<String> value, final boolean isFallback) {
      inForce = value;
      return true;
    }

    @Override
    protected boolean hasInForce() {
      return inForce != null;
    }

    @Override
    protected String keeping() {
      return "keeping the names in force";
    }

    @Override
    protected String refusal() {
      return refuse ? "the test refuses it" : null;
    }

    @Override
    protected List<String> onFirstLoadFailure(final Exception failure) {
      return keepOnFirstLoadFailure ? null : super.onFirstLoadFailure(failure);
    }

    @Override
    protected void onReloaded() {
      reloaded.incrementAndGet();
    }
  }

  private void write(final String content) throws IOException {
    Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);
  }

  @Test
  void aFirstLoadPublishesTheFile() throws Exception {
    write("{\"names\":[\"a\",\"b\"]}");
    final NamesStore store = new NamesStore();

    store.firstLoad();

    assertThat(store.inForce).containsExactly("a", "b");
    assertThat(errorFile).doesNotExist();
  }

  @Test
  void aFirstLoadWithNoFileFallsBack() {
    final NamesStore store = new NamesStore();

    store.firstLoad();

    assertThat(store.inForce).containsExactly("default");
    assertThat(file).as("the policy itself writes nothing").doesNotExist();
  }

  @Test
  void aFirstLoadOfAFileItCannotUsePreservesItThenFallsBack() throws Exception {
    write("{ not json");
    final NamesStore store = new NamesStore();

    store.firstLoad();

    assertThat(store.inForce).containsExactly("default");
    assertThat(Files.readString(errorFile.toPath(), StandardCharsets.UTF_8)).isEqualTo("{ not json");
    assertThat(Files.readString(file.toPath(), StandardCharsets.UTF_8)).isEqualTo("{ not json");
  }

  @Test
  void aReloadWithNothingInForceIsAFirstLoad() {
    final NamesStore store = new NamesStore();

    assertThat(store.reload()).isTrue();

    assertThat(store.inForce).containsExactly("default");
  }

  @Test
  void aReloadKeepsWhatIsInForceForEveryInputItCannotUse() throws Exception {
    write("{\"names\":[\"a\",\"b\"]}");
    final NamesStore store = new NamesStore();
    store.firstLoad();

    // Unparseable: kept, preserved, live file untouched.
    write("{ not json");
    assertThat(store.reload()).isFalse();
    assertThat(store.inForce).containsExactly("a", "b");
    assertThat(Files.readString(errorFile.toPath(), StandardCharsets.UTF_8)).isEqualTo("{ not json");
    assertThat(Files.readString(file.toPath(), StandardCharsets.UTF_8)).isEqualTo("{ not json");

    // Wrong shape: kept, preserved.
    write("{\"other\":1}");
    assertThat(store.reload()).isFalse();
    assertThat(store.inForce).containsExactly("a", "b");
    assertThat(Files.readString(errorFile.toPath(), StandardCharsets.UTF_8)).isEqualTo("{\"other\":1}");

    // Missing: kept, nothing recreated.
    Files.delete(file.toPath());
    assertThat(store.reload()).isFalse();
    assertThat(store.inForce).containsExactly("a", "b");
    assertThat(file).doesNotExist();

    // I/O error: kept, the live path left alone.
    Files.createDirectory(file.toPath());
    assertThat(store.reload()).isFalse();
    assertThat(store.inForce).containsExactly("a", "b");
    assertThat(file).isDirectory();
  }

  @Test
  void aValidReloadReplacesWhatIsInForce() throws Exception {
    write("{\"names\":[\"a\",\"b\"]}");
    final NamesStore store = new NamesStore();
    store.firstLoad();

    write("{\"names\":[\"c\"]}");
    assertThat(store.reload()).isTrue();

    assertThat(store.inForce).containsExactly("c");
  }

  /** A first load that publishes nothing - the tokens store's answer to an I/O error - is not reported as a reload. */
  @Test
  void aReloadIntoAFirstLoadThatPublishesNothingReportsFalse() throws Exception {
    write("{ not json");
    final NamesStore store = new NamesStore();
    store.keepOnFirstLoadFailure = true;

    assertThat(store.reload()).isFalse();

    assertThat(store.inForce).isNull();
  }

  /** A refusal stamps the baseline, so the watcher logs it once per change of the file rather than once per tick. */
  @Test
  void aRefusedReloadKeepsWhatIsInForceAndStampsTheBaseline() throws Exception {
    write("{\"names\":[\"a\"]}");
    final NamesStore store = new NamesStore();
    store.firstLoad();

    write("{\"names\":[\"b\"]}");
    assertThat(file.setLastModified(System.currentTimeMillis() + 10_000)).isTrue();
    assertThat(store.fileChanged()).isTrue();
    store.refuse = true;

    assertThat(store.reload()).isFalse();

    assertThat(store.inForce).containsExactly("a");
    assertThat(store.fileChanged()).as("the refused change is not reported again").isFalse();
  }

  @Test
  void theWatcherReloadsAnEditAndCallsBack() throws Exception {
    write("{\"names\":[\"a\"]}");
    final NamesStore store = new NamesStore();
    store.firstLoad();
    store.startWatching(50);
    try {
      write("{\"names\":[\"z\"]}");
      assertThat(file.setLastModified(System.currentTimeMillis() + 10_000)).isTrue();

      await().atMost(Duration.ofSeconds(30)).until(() -> List.of("z").equals(store.inForce));
      await().atMost(Duration.ofSeconds(30)).until(() -> store.reloaded.get() >= 1);
    } finally {
      store.stop();
    }
  }

  @Test
  void theWatcherIsANamedDaemonAndOneShot() {
    final NamesStore store = new NamesStore();
    store.startWatching(60_000);
    final Thread watcher = watcherThread("arcadedb-test-10404-watcher[" + file.getPath() + "]");
    assertThat(watcher).isNotNull();
    assertThat(watcher.isDaemon()).isTrue();

    store.stop();
    await().atMost(Duration.ofSeconds(30)).until(() -> watcherThread("arcadedb-test-10404-watcher[" + file.getPath() + "]") == null);

    store.startWatching(60_000);
    assertThat(watcherThread("arcadedb-test-10404-watcher[" + file.getPath() + "]")).as("a stopped store does not watch again")
        .isNull();
  }

  @Test
  void aNonPositivePeriodSchedulesNoWatcher() {
    final NamesStore store = new NamesStore();
    store.startWatching(0);
    assertThat(watcherThread("arcadedb-test-10404-watcher[" + file.getPath() + "]")).isNull();
    store.stop();
  }

  /** The users store is on the shared watcher: a named daemon thread, and a hand edit still reaches the users in force. */
  @Test
  void theUsersWatcherIsTheSharedOneAndPicksUpAHandEdit() throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 50);
    final ServerSecurity security = new ServerSecurity(null, configuration, CONFIG_PATH);
    try {
      security.loadUsers();
      security.createUser(new JSONObject().put("name", "bob").put("password", security.encodePassword("BobPassword123!"))
          .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))));
      assertThat(security.getUsers()).contains("root", "bob");

      final Path usersFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
      final Thread watcher = watcherThread(ServerSecurity.USERS_WATCHER_PREFIX + "[" + usersFile.toFile().getPath() + "]");
      assertThat(watcher).as("the users watcher runs on the shared, named timer").isNotNull();
      assertThat(watcher.isDaemon()).isTrue();

      final StringBuilder kept = new StringBuilder();
      for (final String line : Files.readAllLines(usersFile, StandardCharsets.UTF_8))
        if (!line.contains("\"bob\""))
          kept.append(line).append('\n');
      Files.writeString(usersFile, kept.toString(), StandardCharsets.UTF_8);
      assertThat(usersFile.toFile().setLastModified(System.currentTimeMillis() + 10_000)).isTrue();

      await().atMost(Duration.ofSeconds(30)).until(() -> !security.getUsers().contains("bob"));
      assertThat(security.getUsers()).contains("root");
    } finally {
      security.stopService();
    }
  }

  /**
   * A legacy plaintext token file edited in while the server runs is published hashed and saved once; the re-read the save
   * causes publishes the same token set and saves nothing more.
   */
  @Test
  void aLegacyTokenFileIsMigratedOnReloadAndTheReReadIsIdempotent() throws Exception {
    final ApiTokenConfiguration tokens = new ApiTokenConfiguration(CONFIG_PATH);
    tokens.load();
    tokens.createToken("ci", "*", 0, new JSONObject());

    final Path tokenFile = Path.of(CONFIG_PATH, ApiTokenConfiguration.FILE_NAME);
    final String plaintext = "at-legacy-plaintext-token-0001";
    Files.writeString(tokenFile, new JSONObject().put("version", 1).put("tokens", new JSONArray().put(
        new JSONObject().put("token", plaintext).put("name", "legacy").put("database", "*").put("permissions", new JSONObject())))
        .toString(), StandardCharsets.UTF_8);

    assertThat(tokens.reload()).isTrue();
    assertThat(tokens.getToken(plaintext)).isNotNull();
    final String migrated = Files.readString(tokenFile, StandardCharsets.UTF_8);
    assertThat(migrated).contains("tokenHash").doesNotContain(plaintext);

    assertThat(tokens.reload()).isTrue();
    assertThat(tokens.getToken(plaintext)).isNotNull();
    assertThat(Files.readString(tokenFile, StandardCharsets.UTF_8)).as("nothing more to migrate, nothing rewritten").isEqualTo(migrated);
    tokens.stop();
  }

  /** A second loadUsers() - the reload path - does not schedule a second users watcher. */
  @Test
  void loadingUsersTwiceKeepsOneUsersWatcher() {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
    try {
      security.loadUsers();
      security.loadUsers();

      final String name = ServerSecurity.USERS_WATCHER_PREFIX + "[" + new File(dir, SecurityUserFileRepository.FILE_NAME).getPath() + "]";
      long count = 0;
      for (final Thread thread : Thread.getAllStackTraces().keySet())
        if (thread.isAlive() && thread.getName().equals(name))
          count++;
      assertThat(count).isEqualTo(1);
    } finally {
      security.stopService();
    }
  }

  /** A non-positive period used to reach {@code Timer.schedule}, which throws on it, from the users and groups stores. */
  @Test
  void aNonPositiveReloadPeriodStartsTheSecurityServiceWithoutWatchers() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 0);
    final ServerSecurity security = new ServerSecurity(null, configuration, CONFIG_PATH);
    try {
      security.loadUsers();
      security.startService();

      assertThat(security.getUsers()).contains("root");
      final String usersPath = new File(dir, SecurityUserFileRepository.FILE_NAME).getPath();
      final String groupsPath = new File(dir, SecurityGroupFileRepository.FILE_NAME).getPath();
      assertThat(watcherThread(ServerSecurity.USERS_WATCHER_PREFIX + "[" + usersPath + "]")).isNull();
      assertThat(watcherThread(SecurityGroupFileRepository.WATCHER_PREFIX + "[" + groupsPath + "]")).isNull();
    } finally {
      security.stopService();
    }
  }

  private static Thread watcherThread(final String name) {
    for (final Thread thread : Thread.getAllStackTraces().keySet())
      if (thread.isAlive() && thread.getName().equals(name))
        return thread;
    return null;
  }
}
