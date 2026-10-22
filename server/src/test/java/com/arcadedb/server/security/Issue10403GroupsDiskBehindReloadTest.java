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
 * Regression test for issue #10403: a replicated group document is published in memory BEFORE it is persisted, so when
 * the write fails the file on disk is OLDER than the groups in force. A reload of that file - fired by the stamp of an
 * earlier save or by anything touching the file - must not publish it over the replicated document, or a permission
 * the cluster has just narrowed, or a group it has just deleted, comes back on this node.
 */
class Issue10403GroupsDiskBehindReloadTest {

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
