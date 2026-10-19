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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for issue #10326: a {@code server-users.jsonl} that cannot be loaded when the users-file watcher
 * RELOADS it on a running server must leave the users already in force untouched. Before the fix the reload took the
 * startup fallback - a single passwordless {@code root} from {@link SecurityUserFileRepository#createDefault()} - so every
 * other user was dropped at once (and their sessions revoked), and {@code askForRootPassword()} re-created {@code root}
 * and saved it over the operator's file.
 * <p>
 * The first load keeps falling back to the default (there is no previous list to keep); that path is covered by
 * {@link Issue10321UsersFileParseErrorTest}.
 */
class Issue10326UsersFileReloadParseErrorTest {

  private static final String CONFIG_PATH   = "target/test-security-10326";
  private static final String ROOT_PASSWORD = "PlayWithData123!";
  private static final String BOB_PASSWORD  = "BobPassword123!";
  private static final String MALFORMED     = "{\"name\":\"root\",\"password\":\"x\"}\n{\"name\":\"bob\", oops\n";

  private final Path usersFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
  private final Path errorFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.ERROR_FILE_NAME);

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
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /** The scenario of the issue, driven through the real watcher: a typo in a hand edit of the users file. */
  @Test
  void aMalformedHandEditPickedUpByTheWatcherKeepsTheUsersInForce() throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 50);
    final ServerSecurity security = startWithRootAndBob(configuration);
    try {
      final List<String> lines = logged(captured -> {
        handEdit(MALFORMED);
        // On the SEVERE, not on the error file: it is logged after the file is renamed into place, on the watcher thread.
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20))
            .until(() -> captured.stream().anyMatch(Issue10326UsersFileReloadParseErrorTest::isLoadFailure));
      });

      assertUsersInForce(security);
      assertThat(Files.readString(errorFile, StandardCharsets.UTF_8)).as("the operator's edit is preserved").isEqualTo(MALFORMED);
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("nothing was saved over the operator's file")
          .isEqualTo(MALFORMED);

      final List<String> severe = lines.stream().filter(Issue10326UsersFileReloadParseErrorTest::isLoadFailure).toList();
      assertThat(severe).hasSize(1);
      assertThat(severe.getFirst()).contains(SecurityUserFileRepository.FILE_NAME).contains("keeping the 2 users already in force")
          .doesNotContain("default configuration");
    } finally {
      security.stopService();
    }
  }

  /** The watcher survives a failed reload: the operator's corrected edit is applied on a later tick. */
  @Test
  void aCorrectedEditAfterAFailedReloadIsApplied() throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 50);
    final ServerSecurity security = startWithRootAndBob(configuration);
    try {
      final String validFile = Files.readString(usersFile, StandardCharsets.UTF_8);
      handEdit(MALFORMED);
      await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> Files.exists(errorFile));
      assertUsersInForce(security);

      // The fixed edit: bob removed by hand.
      final String withoutBob = validFile.lines().filter(l -> !l.contains("\"bob\"")).reduce("", (a, l) -> a + l + "\n");
      handEdit(withoutBob);
      await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> !security.existsUser("bob"));

      assertThat(security.authenticate("root", ROOT_PASSWORD, null).getName()).isEqualTo("root");
    } finally {
      security.stopService();
    }
  }

  /** Same contract on a direct reload call, for a record that is valid JSON but not a user (no "name"). */
  @Test
  void aReloadWithAnInvalidRecordKeepsTheUsersInForce() throws Exception {
    final ServerSecurity security = startWithRootAndBob(new ContextConfiguration());
    try {
      final String content = "{\"nome\":\"bob\"}\n";
      handEdit(content);

      security.loadUsers();

      assertUsersInForce(security);
      assertThat(Files.readString(errorFile, StandardCharsets.UTF_8)).isEqualTo(content);
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).isEqualTo(content);
    } finally {
      security.stopService();
    }
  }

  /**
   * A record of an unexpected shape ("databases" not an object) on reload keeps the users in force and leaves the
   * watcher alive, whatever exception the record raises.
   */
  @Test
  void aReloadWithARecordOfAnUnexpectedShapeKeepsTheUsersInForce() throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 50);
    final ServerSecurity security = startWithRootAndBob(configuration);
    try {
      final String validFile = Files.readString(usersFile, StandardCharsets.UTF_8);
      final String content = "{\"name\":\"bob\",\"databases\":5}\n";
      handEdit(content);
      await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20))
          .until(() -> Files.exists(errorFile) && Files.readString(errorFile, StandardCharsets.UTF_8).equals(content));
      assertUsersInForce(security);

      // The watcher survived: a later valid edit is still applied.
      handEdit(validFile.lines().filter(l -> !l.contains("\"bob\"")).reduce("", (a, l) -> a + l + "\n"));
      await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> !security.existsUser("bob"));
    } finally {
      security.stopService();
    }
  }

  /**
   * The same-shape sibling: a file caught EMPTY - an editor that truncates before it writes, observed by the watcher
   * between the two - used to take the same {@code createDefault()} fallback inside the repository.
   */
  @Test
  void aReloadOfAnEmptyFileKeepsTheUsersInForce() throws Exception {
    final ServerSecurity security = startWithRootAndBob(new ContextConfiguration());
    try {
      handEdit("");

      final List<String> lines = logged(captured -> security.loadUsers());

      assertUsersInForce(security);
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the empty file was not replaced by a root-only one").isEmpty();
      assertThat(lines.stream().filter(l -> l.startsWith("WARNING") && l.contains("is missing or empty")).toList()).hasSize(1);
    } finally {
      security.stopService();
    }
  }

  /**
   * A file that cannot be READ on reload (an I/O error rather than a parse error) used to be swallowed by
   * {@link SecurityUserFileRepository#getUsers()} into the same {@code createDefault()} fallback.
   */
  @Test
  void aReloadOfAnUnreadableFileKeepsTheUsersInForce() throws Exception {
    final ServerSecurity security = startWithRootAndBob(new ContextConfiguration());
    final PosixFileAttributeView view = Files.getFileAttributeView(usersFile, PosixFileAttributeView.class);
    assumeTrue(view != null, "needs POSIX permissions to make the file unreadable");
    try {
      handEdit(MALFORMED);
      view.setPermissions(Set.of());
      assumeTrue(!Files.isReadable(usersFile), "the file is still readable (running as a superuser?)");

      final List<String> lines = logged(captured -> security.loadUsers());

      view.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
      assertUsersInForce(security);
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("nothing was saved over the unreadable file").isEqualTo(MALFORMED);
      assertThat(lines.stream().filter(Issue10326UsersFileReloadParseErrorTest::isLoadFailure).toList()).hasSize(1).first().asString()
          .contains("keeping the 2 users already in force");
    } finally {
      view.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
      security.stopService();
    }
  }

  /** Control: a valid hand edit on reload is still applied, including a user it drops. */
  @Test
  void aValidReloadStillReplacesTheUsers() throws Exception {
    final ServerSecurity security = startWithRootAndBob(new ContextConfiguration());
    try {
      final String withoutBob = Files.readString(usersFile, StandardCharsets.UTF_8).lines().filter(l -> !l.contains("\"bob\""))
          .reduce("", (a, l) -> a + l + "\n");
      handEdit(withoutBob);

      security.loadUsers();

      assertThat(security.existsUser("bob")).isFalse();
      assertThat(security.authenticate("root", ROOT_PASSWORD, null).getName()).isEqualTo("root");
      assertThat(errorFile).doesNotExist();
    } finally {
      security.stopService();
    }
  }

  private ServerSecurity startWithRootAndBob(final ContextConfiguration configuration) {
    final ServerSecurity security = new ServerSecurity(null, configuration, CONFIG_PATH);
    security.loadUsers();
    security.createUser(new JSONObject().put("name", "bob").put("password", security.encodePassword(BOB_PASSWORD))
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))));
    assertUsersInForce(security);
    return security;
  }

  private static void assertUsersInForce(final ServerSecurity security) {
    assertThat(security.getUsers()).hasSize(2);
    assertThat(security.authenticate("root", ROOT_PASSWORD, null).getName()).isEqualTo("root");
    assertThat(security.authenticate("bob", BOB_PASSWORD, null).getName()).isEqualTo("bob");
  }

  /** Writes the file and moves its stamp clearly past the last load, so the change cannot fall in the same tick. */
  private void handEdit(final String content) throws Exception {
    final long previous = Files.getLastModifiedTime(usersFile).toMillis();
    Files.writeString(usersFile, content, StandardCharsets.UTF_8);
    Files.setLastModifiedTime(usersFile, FileTime.fromMillis(Math.max(previous, System.currentTimeMillis()) + 2_000));
  }

  private static boolean isLoadFailure(final String line) {
    return line.startsWith("SEVERE") && line.contains("Error on loading file");
  }

  /** Runs {@code action} capturing everything logged as "LEVEL message args"; the action can watch the lines as they arrive. */
  private static List<String> logged(final CapturingAction action) throws Exception {
    final List<String> lines = Collections.synchronizedList(new ArrayList<>());
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object r, final Level level, final String message, final Throwable e, final String context,
          final Object a1, final Object a2, final Object a3, final Object a4, final Object a5, final Object a6, final Object a7,
          final Object a8, final Object a9, final Object a10, final Object a11, final Object a12, final Object a13,
          final Object a14, final Object a15, final Object a16, final Object a17) {
        lines.add(level + " " + message + " " + a1 + " " + a2 + " " + a3 + " " + a4);
      }

      @Override
      public void log(final Object r, final Level level, final String message, final Throwable e, final String context,
          final Object... args) {
        final StringBuilder b = new StringBuilder(level + " " + message);
        if (args != null)
          for (final Object a : args)
            b.append(' ').append(a);
        lines.add(b.toString());
      }

      @Override
      public void flush() {
      }
    });
    try {
      action.run(lines);
    } finally {
      LogManager.instance().setLogger(previous);
    }
    return new ArrayList<>(lines);
  }

  @FunctionalInterface
  private interface CapturingAction {
    void run(List<String> captured) throws Exception;
  }
}
