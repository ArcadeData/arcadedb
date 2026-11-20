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
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServedDatabases;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Reload of {@code server-groups.json} and {@code server-api-tokens.json} through the real watchers: an unusable
 * file, a file older than the replicated document in force, and the lifecycle of the watchers themselves.
 */
class SecurityGroupsAndTokensReloadTest {
  /**
   * Regression test for issue #10383, driven through the real watchers: a reload of {@code server-groups.json} or
   * {@code server-api-tokens.json} that cannot use the file keeps what is in force, preserves the file and never writes
   * over it. {@link SecurityArtifactReloadConformanceTest} is the census of the four inputs for every artifact;
   * this class covers what the census cannot - the watchers themselves, the first load, and the token store's own paths.
   */
  @Nested
  class UnusableFileOnReload {

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

  /**
   * Regression test for issue #10403: a replicated group document is published in memory BEFORE it is persisted, so when
   * the write fails the file on disk is OLDER than the groups in force. A reload of that file - fired by the stamp of an
   * earlier save or by anything touching the file - must not publish it over the replicated document, or a permission
   * the cluster has just narrowed, or a group it has just deleted, comes back on this node.
   */
  @Nested
  class DiskBehindReplicatedGroups {

    private static final String   CONFIG_PATH   = "target/test-security-10403";
    private static final String   ROOT_PASSWORD = "PlayWithData123!";
    private static final Duration AT_MOST       = Duration.ofSeconds(30);
    private static final Duration POLL          = Duration.ofMillis(20);
    // Many watcher periods: long enough for a reload that was going to happen to have happened.
    private static final Duration QUIET         = Duration.ofMillis(750);
    private static final int      WATCH_EVERY   = 50;

    private final Path groupsFile = Path.of(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);

    private PosixFileAttributeView dirView;

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
    }

    @AfterEach
    void tearDown() throws Exception {
      makeWritable();
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
      GlobalConfiguration.SERVER_ROOT_PASSWORD.reset();
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
    }

    /** The scenario of the issue, on the repository: read-only directory, narrowed replicated document, file touched. */
    @Test
    void aReloadDoesNotRepublishTheOlderFileWhileTheReplicatedDocumentIsNotOnDisk() throws Exception {
      final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 60_000);
      try {
        final JSONObject wide = withAuditors(repository.getGroups());
        repository.save(wide);
        final String wideOnDisk = Files.readString(groupsFile, StandardCharsets.UTF_8);

        final JSONObject narrowed = withoutAuditors(wide);
        makeReadOnly();
        assumeTrue(repository.applyReplicated(narrowed) != null, "the directory is still writable (running as a superuser?)");
        assertThat(hasAuditors(repository.getGroups())).as("the narrowed document is in force").isFalse();

        // The file still holds the deleted group: the stamp of the last good save, or any touch, fires a reload.
        touch(groupsFile);
        assertThat(repository.reload()).as("an older file is not published over the groups in force").isFalse();
        assertThat(hasAuditors(repository.getGroups())).as("the deleted group stays deleted").isFalse();
        assertThat(Files.readString(groupsFile, StandardCharsets.UTF_8)).as("the refusal writes nothing").isEqualTo(wideOnDisk);

        // A retry that still fails leaves the disk behind, so reloads are still refused.
        assertThat(repository.persistCurrent()).isNotNull();
        touch(groupsFile);
        assertThat(repository.reload()).isFalse();
        assertThat(hasAuditors(repository.getGroups())).isFalse();

        // Once the document in force reaches the disk (the #7752 retry), reloads are applied again.
        makeWritable();
        assertThat(repository.persistCurrent()).isNull();
        assertThat(hasAuditors(new JSONObject(Files.readString(groupsFile, StandardCharsets.UTF_8)))).isFalse();
        handEdit(groupsFile, wideOnDisk);
        assertThat(repository.reload()).isTrue();
        assertThat(hasAuditors(repository.getGroups())).as("an operator edit made after the retry is applied").isTrue();
      } finally {
        repository.stop();
      }
    }

    /** The same scenario through the real watcher, which used to undo the replicated document within one tick. */
    @Test
    void theWatcherDoesNotUndoAReplicatedDocumentWhoseWriteFailed() throws Exception {
      final AtomicInteger refreshes = new AtomicInteger();
      final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, WATCH_EVERY).onReload(document -> {
        refreshes.incrementAndGet();
        return null;
      });
      try {
        final JSONObject wide = withAuditors(repository.getGroups());
        repository.save(wide);
        // save() never advances the watcher's stamp, so its own write is reloaded once: wait for it, then start counting.
        await().atMost(AT_MOST).pollInterval(POLL).until(() -> refreshes.get() > 0);

        makeReadOnly();
        assumeTrue(repository.applyReplicated(withoutAuditors(wide)) != null, "the directory is still writable (running as a superuser?)");
        final int refreshesBefore = refreshes.get();

        touch(groupsFile);
        await().pollDelay(QUIET).atMost(QUIET.plusSeconds(5)).until(() -> true);

        assertThat(hasAuditors(repository.getGroups())).as("the deleted group stays deleted").isFalse();
        assertThat(refreshes.get()).as("no permission refresh for a document that was not published").isEqualTo(refreshesBefore);
      } finally {
        repository.stop();
      }
    }

    /** The issue's entry point: ServerSecurity.applyReplicatedGroups on a node with a read-only volume, then the retry. */
    @Test
    void aReplicatedGroupDeletionSurvivesTheWatcherUntilTheRetryPersistsIt() throws Exception {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, WATCH_EVERY);
      final ServerSecurity security = new ServerSecurity(null, configuration, CONFIG_PATH);
      security.loadUsers();
      security.startService();
      try {
        final JSONObject wide = withAuditors(security.groupsToJSON());
        handEdit(groupsFile, wide.toString(2));
        await().atMost(AT_MOST).pollInterval(POLL).until(() -> hasAuditors(security.groupsToJSON()));

        makeReadOnly();
        final String narrowed = withoutAuditors(wide).toString();
        try {
          security.applyReplicatedGroups(narrowed);
          assumeTrue(false, "the directory is still writable (running as a superuser?)");
        } catch (final ReplicatedSecurityConfigPersistenceException expected) {
          // the write failed, the document is in force
        }
        assertThat(hasAuditors(security.groupsToJSON())).isFalse();

        touch(groupsFile);
        await().pollDelay(QUIET).atMost(QUIET.plusSeconds(5)).until(() -> true);
        assertThat(hasAuditors(security.groupsToJSON())).as("the watcher must not resurrect the deleted group").isFalse();

        makeWritable();
        assertThat(security.retryReplicatedSecurityPersistence()).isTrue();
        assertThat(hasAuditors(new JSONObject(Files.readString(groupsFile, StandardCharsets.UTF_8)))).isFalse();
        assertThat(hasAuditors(security.groupsToJSON())).isFalse();
      } finally {
        security.stopService();
      }
    }

    /** A successful local save puts the disk level with memory again: the next hand edit is applied. */
    @Test
    void aSuccessfulSaveLiftsTheRefusal() throws Exception {
      final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 60_000);
      try {
        final JSONObject wide = withAuditors(repository.getGroups());
        repository.save(wide);
        makeReadOnly();
        assumeTrue(repository.applyReplicated(withoutAuditors(wide)) != null, "the directory is still writable (running as a superuser?)");

        makeWritable();
        repository.save(withoutAuditors(wide));
        handEdit(groupsFile, wide.toString(2));
        assertThat(repository.reload()).isTrue();
        assertThat(hasAuditors(repository.getGroups())).isTrue();
      } finally {
        repository.stop();
      }
    }

    /**
     * A local save that FAILS publishes nothing (it throws), so memory and disk still agree and an operator's edit must
     * still be applied: only a replicated install that did not reach the disk refuses reloads.
     */
    @Test
    void aFailedLocalSaveDoesNotRefuseReloads() throws Exception {
      final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 60_000);
      try {
        final JSONObject original = repository.getGroups();
        makeReadOnly();
        try {
          repository.save(withAuditors(original));
          assumeTrue(false, "the directory is still writable (running as a superuser?)");
        } catch (final Exception expected) {
          // nothing published
        }
        assertThat(hasAuditors(repository.getGroups())).isFalse();

        // The directory is read-only, but the file itself is the owner's to rewrite in place.
        handEdit(groupsFile, withAuditors(original).toString(2));
        assertThat(repository.reload()).isTrue();
        assertThat(hasAuditors(repository.getGroups())).isTrue();
      } finally {
        repository.stop();
      }
    }

    /** A replicated install that reaches the disk refuses nothing either. */
    @Test
    void aPersistedReplicatedDocumentDoesNotRefuseReloads() throws Exception {
      final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 60_000);
      try {
        final JSONObject original = repository.getGroups();
        assertThat(repository.applyReplicated(original.copy())).isNull();
        handEdit(groupsFile, withAuditors(original).toString(2));
        assertThat(repository.reload()).isTrue();
        assertThat(hasAuditors(repository.getGroups())).isTrue();
      } finally {
        repository.stop();
      }
    }

    private void makeReadOnly() throws Exception {
      dirView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
    }

    private void makeWritable() throws Exception {
      if (dirView != null)
        dirView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
    }

    private static JSONObject withAuditors(final JSONObject document) {
      final JSONObject copy = document.copy();
      groupsOf(copy).put("auditors", new JSONObject().put("resultSetLimit", 10L));
      return copy;
    }

    private static JSONObject withoutAuditors(final JSONObject document) {
      final JSONObject copy = document.copy();
      groupsOf(copy).remove("auditors");
      return copy;
    }

    private static boolean hasAuditors(final JSONObject document) {
      return groupsOf(document).has("auditors");
    }

    private static JSONObject groupsOf(final JSONObject document) {
      return document.getJSONObject("databases").getJSONObject("*").getJSONObject("groups");
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
  @Nested
  class WatcherStartedByReplicatedApply {

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

    /**
     * {@code stop()} and {@code startWatching()} are both check-then-act on the same field, so they have to run
     * under the same monitor: an unsynchronized {@code stop()} could read the reference in the window after
     * {@code startWatching()}'s null check and before its assignment, return without cancelling, and leave a
     * timer polling a repository nobody uses any more. The exposure is what this issue changed - the field used
     * to be written only on a node that had touched the group document and is now written on every one.
     * <p>
     * Structural on purpose, and it says so: a racing pair cannot be made to lose reliably, so a run that hits
     * the window would be a flaky PASS on a broken build rather than a failure. The modifier IS the contract
     * here, and asserting it fails deterministically the moment somebody removes it. The stress rows below then
     * exercise the pair for real.
     */
    @Test
    void stopAndStartWatchingRunUnderTheSameMonitor() throws Exception {
      assertThat(Modifier.isSynchronized(SecurityGroupFileRepository.class.getDeclaredMethod("stop").getModifiers()))
          .as("stop() must hold the monitor startWatching() writes the timer reference under").isTrue();
      assertThat(
          Modifier.isSynchronized(SecurityGroupFileRepository.class.getDeclaredMethod("startWatching").getModifiers()))
          .as("startWatching() must hold the monitor").isTrue();
    }

    /**
     * The behavioural half of the row above: many repositories, each started and stopped from two different
     * threads racing each other, must leave no watcher thread behind.
     */
    @Test
    void aStopRacingAStartLeavesNoWatcherBehind() throws Exception {
      final long before = securityWatcherThreadCount();

      for (int i = 0; i < 200; i++) {
        final SecurityGroupFileRepository repo = new SecurityGroupFileRepository(CONFIG_PATH, 1_000_000);
        final CountDownLatch go = new CountDownLatch(1);
        final Thread starter = new Thread(() -> {
          awaitLatch(go);
          repo.startWatching();
        });
        final Thread stopper = new Thread(() -> {
          awaitLatch(go);
          repo.stop();
        });
        starter.start();
        stopper.start();
        go.countDown();
        starter.join();
        stopper.join();
        // Whichever order they landed in, the repository must not be left watching: a stop that won the race
        // cancels nothing and a startWatching() that ran after it is the one-shot the javadoc describes, while a
        // stop that lost cancels the timer the starter had already published.
        repo.stop();
      }

      assertThat(await(() -> securityWatcherThreadCount() <= before))
          .as("no watcher survives a stop racing a start").isTrue();
    }

    private static void awaitLatch(final CountDownLatch latch) {
      try {
        latch.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private ServerSecurity newSecurity() {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

      final ServerSecurity instance = new ServerSecurity(server, configuration, CONFIG_PATH);
      server.security(instance);
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
          .filter(t -> t.getName().startsWith(SecurityGroupFileRepository.WATCHER_PREFIX))
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

  /**
   * Issue #10707: a security file watcher's tick is not stopped by {@code ReloadableSecurityArtifact.stop()}, which cancels the
   * {@code Timer} and leaves a tick that has begun to run. A groups-file tick that was already sweeping the server's databases
   * when the server stopped reached {@code ArcadeDBServer.getDatabase} after the databases were closed, and that opens the
   * directory again and takes its file lock: the next server on the same directory then fails on an
   * {@code OverlappingFileLockException}.
   * <ul>
   *   <li>{@code ServerSecurity.stopService()} waits, briefly, for a sweep the groups watcher is running;</li>
   *   <li>a tick that was reading its file when the store stopped does not go on to {@code onReloaded()}.</li>
   * </ul>
   */
  @Nested
  class WatcherStop {
    @RegisterExtension
    static final ServedDatabases SERVED = new ServedDatabases();

    private static final String CONFIG_PATH     = "target/test-security-10707";
    private static final String DATABASE        = "graph";
    private static final int    RELOAD_EVERY_MS = 20;

    private ServerSecurity security;

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
      assertThat(new File(CONFIG_PATH).mkdirs()).isTrue();
    }

    @AfterEach
    void tearDown() {
      if (security != null)
        security.stopService();
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
    }

    /**
     * The sweep is held inside {@code getDatabase}, on the watcher's thread, while the service stops. {@code stopService} is
     * the call after which the server closes its databases, so it must not return before the sweep has: otherwise the sweep's
     * next {@code getDatabase} meets a closed server.
     */
    @Test
    void stopServiceWaitsForTheGroupsSweepTheWatcherIsRunning() throws Exception {
      final ServerDatabase database = SERVED.open(DATABASE);
      final CountDownLatch sweeping = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final AtomicBoolean sweepEnded = new AtomicBoolean();

      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.databaseNames(DATABASE);
      server.on("getDatabaseForOperator", args -> {
        // Only the groups watcher's thread is held; its name carries the file it watches
        if (Thread.currentThread().getName().contains(SecurityGroupFileRepository.FILE_NAME)) {
          sweeping.countDown();
          try {
            release.await(30, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          sweepEnded.set(true);
        }
        return database;
      });

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);
      security = new ServerSecurity(server, configuration, CONFIG_PATH);
      server.security(security);
      security.startService();

      // A stamp strictly newer than the baseline is a change to the watcher; the content is read and published as it is
      final File groups = new File(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);
      assertThat(groups).exists();
      assertThat(groups.setLastModified(System.currentTimeMillis() + 60_000L)).isTrue();
      assertThat(sweeping.await(10, TimeUnit.SECONDS)).as("the watcher began a sweep").isTrue();

      final AtomicBoolean sweepHadEnded = new AtomicBoolean();
      final CompletableFuture<Void> stopping = CompletableFuture.runAsync(() -> {
        security.stopService();
        sweepHadEnded.set(sweepEnded.get());
      });
      try {
        assertThatThrownBy(() -> stopping.get(500, TimeUnit.MILLISECONDS))
            .as("stopService() returned while the watcher's sweep was still running").isInstanceOf(TimeoutException.class);
      } finally {
        release.countDown();
      }
      stopping.get(30, TimeUnit.SECONDS);

      assertThat(sweepHadEnded.get()).as("the sweep had ended when stopService() returned").isTrue();
    }

    /**
     * A tick that began before the store stopped and was reading its file when it did. It has reloaded by the time it could
     * sweep, but a stopped store must not: {@code onReloaded()} is where the groups store reaches the server's databases.
     */
    @Test
    void aTickThatWasReadingWhenTheStoreStoppedDoesNotRunOnReloaded() throws Exception {
      final StubStore store = new StubStore();
      store.startWatching(RELOAD_EVERY_MS);
      assertThat(store.inTick.await(10, TimeUnit.SECONDS)).as("a tick began").isTrue();

      store.stop();
      store.releaseTick.countDown();

      final Thread tick = store.tickThread.get();
      // Timer.cancel() ends the timer thread once the task it is running returns: joining it is waiting for the tick
      tick.join(TimeUnit.SECONDS.toMillis(10));
      assertThat(tick.isAlive()).as("the tick ended").isFalse();
      assertThat(store.reloaded.get()).as("onReloaded() runs of a stopped store").isZero();
    }

    /** A store over a file that is always seen as changed, whose first tick is held inside {@code fileChanged()}. */
    private static final class StubStore extends ReloadableSecurityArtifact<String> {
      final CountDownLatch         inTick      = new CountDownLatch(1);
      final CountDownLatch         releaseTick = new CountDownLatch(1);
      final AtomicReference<Thread> tickThread  = new AtomicReference<>();
      final AtomicInteger          reloaded    = new AtomicInteger();

      StubStore() throws IOException {
        super(new Object(), new Object(), writtenFile("store.json"), new File(CONFIG_PATH, "store-error.json"),
            "arcadedb-test-10707-watcher", "Test store");
      }

      private static File writtenFile(final String name) throws IOException {
        final File file = new File(CONFIG_PATH, name);
        java.nio.file.Files.writeString(file.toPath(), "stored");
        return file;
      }

      @Override
      protected boolean fileChanged() {
        tickThread.compareAndSet(null, Thread.currentThread());
        inTick.countDown();
        try {
          releaseTick.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return true;
      }

      @Override
      protected String read() throws IOException {
        return java.nio.file.Files.readString(this.file.toPath());
      }

      @Override
      protected String fallback() {
        return "default";
      }

      @Override
      protected boolean publish(final String value, final boolean isFallback) {
        return true;
      }

      @Override
      protected boolean hasInForce() {
        return true;
      }

      @Override
      protected String keeping() {
        return "keeping what is in force";
      }

      @Override
      protected void onReloaded() {
        reloaded.incrementAndGet();
      }
    }
  }
}
