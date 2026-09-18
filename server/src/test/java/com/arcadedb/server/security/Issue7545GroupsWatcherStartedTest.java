/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7545: the {@code server-groups.json} reload watcher must be running on a node whose FIRST contact with
 * the group store is a replicated apply.
 * <p>
 * The watcher used to be scheduled only from inside {@code SecurityGroupFileRepository.load()}, and {@code load()}
 * runs only from {@code getGroups()}'s lazy-init branch. {@code applyReplicated()} publishes the document by
 * assigning the {@code volatile latestGroupConfiguration} directly, so on such a node the field became non-null
 * without the timer ever being created, every later {@code getGroups()} took the lock-free fast path, and the
 * lazy-init branch was never entered again. From then on a hand-edited {@code server-groups.json} on that node
 * was ignored for the lifetime of the process.
 * <p>
 * Every test below asserts the OUTCOME an operator would see - the edited file taking effect - rather than
 * inspecting the timer field, so it says nothing about how the watcher is scheduled and fails by timing out if it
 * is not scheduled at all.
 */
class Issue7545GroupsWatcherStartedTest {

  private static final String CONFIG_PATH     = "target/test-security-7545-watcher";
  private static final String DATABASE        = "graph";
  private static final String GROUP           = "editors";
  /** Short enough that the watcher gets several ticks inside {@link #RELOAD_TIMEOUT_MS}. */
  private static final int    RELOAD_EVERY_MS = 200;
  private static final long   RELOAD_TIMEOUT_MS = 20_000;

  private ServerSecurity security;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();
  }

  @AfterEach
  void tearDown() {
    if (security != null) {
      security.stopService();
      security = null;
    }
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /**
   * Row 1 - the reported defect, end to end through {@link ServerSecurity}. The node is started, then seeded by a
   * replicated group document without ever having opened a database, and an operator edits the file by hand.
   * <p>
   * {@code getGroups()} cannot rescue this assertion: the replicated apply left {@code latestGroupConfiguration}
   * non-null, so the only thing that can change what
   * {@link ServerSecurity#getDatabaseGroupsConfiguration(String)} answers is the watcher.
   */
  @Test
  void aHandEditedGroupFileIsPickedUpOnANodeSeededByAReplicatedApply() throws Exception {
    security = newSecurity();
    security.startService();

    // FIRST contact with the group store on this node: the replicated apply. No getGroups() before it.
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")).toString());
    assertThat(security.getDatabaseGroupsConfiguration(DATABASE).getJSONObject(GROUP).getJSONArray("access"))
        .as("the replicated document is in force").hasSize(1);

    writeGroupFileByHand(documentGranting(new JSONArray()));

    assertThat(await(() -> security.getDatabaseGroupsConfiguration(DATABASE).getJSONObject(GROUP)
        .getJSONArray("access").isEmpty()))
        .as("the hand edit reaches a node whose first group access was a replicated apply").isTrue();
  }

  /**
   * Row 2 - {@code startService()} alone has to leave the node watching, so that the watcher's existence does not
   * depend on a document ever arriving at all.
   * <p>
   * Asserted structurally, on the watcher thread, rather than on a reload outcome - deliberately, because there
   * is no outcome to assert here that is not vacuous. With nothing yet published, any read that could observe a
   * reload ({@code getGroups()}, {@code getDatabaseGroupsConfiguration()}) would itself lazy-load the file and
   * pass whether or not a watcher was ever scheduled. Rows 1 and 3-5 carry the behavioural claims; this one is
   * the tripwire for the startup call being dropped.
   */
  @Test
  void startServiceAloneLeavesTheNodeWatchingTheFile() {
    final long before = securityWatcherThreadCount();

    security = newSecurity();
    assertThat(securityWatcherThreadCount())
        .as("constructing the service does not schedule anything on its own").isEqualTo(before);

    security.startService();

    assertThat(securityWatcherThreadCount() - before)
        .as("startService() schedules the group-file watcher without any document having been touched")
        .isEqualTo(1);
  }

  /**
   * Row 3 - the repository in isolation, driven through {@code applyReplicated()} as its very first call, with the
   * reload callback as the witness. This is the unit-level shape of row 1: it holds even for a repository nothing
   * called {@code startWatching()} on.
   */
  @Test
  void theRepositoryWatchesAfterApplyReplicatedIsItsFirstCall() throws Exception {
    final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(CONFIG_PATH, RELOAD_EVERY_MS);
    try {
      assertThat(repo.applyReplicated(documentGranting(new JSONArray().put("updateSchema"))))
          .as("the replicated document was persisted").isNull();

      writeGroupFileByHand(documentGranting(new JSONArray()));

      assertThat(await(() -> repo.getGroups().getJSONObject("databases").getJSONObject(DATABASE)
          .getJSONObject("groups").getJSONObject(GROUP).getJSONArray("access").isEmpty()))
          .as("applyReplicated() as the first call still leaves the repository watching").isTrue();
    } finally {
      repo.stop();
    }
  }

  /**
   * Row 4 - {@code save()} is the third publisher, and the only one of the three that can leave the in-memory
   * document non-null without any read having happened. Through {@code ServerSecurity} it is always preceded by
   * {@code getGroups()}, but the repository is public and must hold the invariant on its own.
   */
  @Test
  void theRepositoryWatchesAfterSaveIsItsFirstCall() throws Exception {
    final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(CONFIG_PATH, RELOAD_EVERY_MS);
    try {
      repo.save(documentGranting(new JSONArray().put("updateSchema")));

      writeGroupFileByHand(documentGranting(new JSONArray()));

      assertThat(await(() -> repo.getGroups().getJSONObject("databases").getJSONObject(DATABASE)
          .getJSONObject("groups").getJSONObject(GROUP).getJSONArray("access").isEmpty()))
          .as("save() as the first call still leaves the repository watching").isTrue();
    } finally {
      repo.stop();
    }
  }

  /**
   * Row 4b - the ORDER inside {@code save()}, raised by CodeRabbit on PR #7818. {@code startWatching()} takes the
   * file's modification time as the watcher's baseline, and {@code save()} excludes other callers of this class
   * but not an operator with an editor. With the baseline taken after {@code persist()}, an edit landing in that
   * gap becomes the baseline while the in-memory document is still the one just saved - and because every later
   * tick needs a STRICTLY newer stamp, that edit is never read (CWE-863).
   * <p>
   * Asserted without trying to win a microsecond race, because the race is only the symptom: the ordering itself
   * is observable. A first-call {@code save()} that took its baseline BEFORE the write leaves the file strictly
   * newer than {@code fileLastUpdated}, so the watcher fires once on its own, with nothing edited. One that took
   * it after leaves the two equal, and the watcher never fires again. The witness is the reload callback, so the
   * test names neither field.
   */
  @Test
  void saveTakesTheWatcherBaselineBeforeItWritesTheFile() throws Exception {
    final CountDownLatch reloaded = new CountDownLatch(1);
    final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(CONFIG_PATH, RELOAD_EVERY_MS)
        .onReload(document -> {
          reloaded.countDown();
          return null;
        });
    try {
      repo.save(documentGranting(new JSONArray().put("updateSchema")));

      assertThat(reloaded.await(RELOAD_TIMEOUT_MS, TimeUnit.MILLISECONDS))
          .as("the file a first-call save() wrote is newer than the baseline that save() captured, so no edit "
              + "made in that window can be swallowed").isTrue();
    } finally {
      repo.stop();
    }
  }

  /**
   * Row 5 - the path that always worked must keep working: a lazy {@code load()} through {@code getGroups()}
   * schedules the watcher exactly as before. Without this the fix could pass every other row by having moved the
   * scheduling somewhere {@code load()} no longer reaches.
   */
  @Test
  void theRepositoryStillWatchesAfterALazyLoad() throws Exception {
    final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(CONFIG_PATH, RELOAD_EVERY_MS);
    try {
      assertThat(repo.getGroups()).as("the lazy load produced a document").isNotNull();

      writeGroupFileByHand(documentGranting(new JSONArray()));

      assertThat(await(() -> {
        final JSONObject databases = repo.getGroups().getJSONObject("databases");
        return databases.has(DATABASE) && databases.getJSONObject(DATABASE).getJSONObject("groups")
            .getJSONObject(GROUP).getJSONArray("access").isEmpty();
      })).as("the lazy-load path still schedules the watcher").isTrue();
    } finally {
      repo.stop();
    }
  }

  /**
   * {@code startWatching()} is called by three publishers plus {@code startService()}, so it has to be idempotent:
   * a second timer would double every reload and never be cancelled by {@link SecurityGroupFileRepository#stop()},
   * which holds a single reference.
   */
  @Test
  void startWatchingIsIdempotent() throws Exception {
    final long before = securityWatcherThreadCount();
    final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(CONFIG_PATH, 1_000_000);
    try {
      for (int i = 0; i < 5; i++)
        repo.startWatching();
      repo.save(documentGranting(new JSONArray()));
      repo.getGroups();

      assertThat(securityWatcherThreadCount() - before)
          .as("repeated startWatching() calls schedule exactly one watcher").isEqualTo(1);
    } finally {
      repo.stop();
    }
  }

  private ServerSecurity newSecurity() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getDatabaseNames()).thenReturn(Set.of());

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

    final ServerSecurity instance = new ServerSecurity(server, configuration, CONFIG_PATH);
    when(server.getSecurity()).thenReturn(instance);
    return instance;
  }

  /**
   * Writes {@code server-groups.json} the way an operator would - straight to disk, behind the repository's back.
   * <p>
   * The modification time is pushed clearly into the future on purpose. The watcher fires on
   * {@code file.lastModified() > fileLastUpdated}, and the document this edit replaces was written by the same
   * test milliseconds earlier; on a filesystem whose timestamp granularity is coarser than the gap - or simply
   * within the same millisecond - the two stamps compare equal and the reload would never be due, so the test
   * would fail for a reason that has nothing to do with the bug.
   */
  private static void writeGroupFileByHand(final JSONObject document) throws Exception {
    final File file = new File(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);
    Files.write(file.toPath(), document.toString(2).getBytes(StandardCharsets.UTF_8));
    assertThat(file.setLastModified(System.currentTimeMillis() + 5_000)).isTrue();
  }

  private static JSONObject documentGranting(final JSONArray access) {
    return new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put(DATABASE, new JSONObject()
            .put("groups", new JSONObject().put(GROUP, new JSONObject()
                .put("access", access)
                .put("resultSetLimit", -1L)
                .put("readTimeout", -1L)
                .put("types", new JSONObject())))));
  }

  private static long securityWatcherThreadCount() {
    return Thread.getAllStackTraces().keySet().stream()
        // Prefix, not equality: the watcher thread carries the watched document's path so that a JVM running
        // several embedded nodes can tell their watchers apart in a thread dump.
        .filter(t -> t.getName().startsWith(SecurityGroupFileRepository.WATCHER_THREAD_NAME_PREFIX))
        .count();
  }

  private static boolean await(final BooleanSupplier condition) {
    final long deadline = System.currentTimeMillis() + RELOAD_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (condition.getAsBoolean())
          return true;
      } catch (final RuntimeException ignoreWhileTheDocumentIsStillTheOldOne) {
        // The reload swaps whole documents: a read can legitimately land on the previous shape.
      }
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }
}
