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
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Users file ({@code server-users.jsonl}): creation conflicts, parse errors on first load and on reload, a
 * file older than the users in force, and the ordering of persistence against publication of a user change.
 */
class ServerSecurityUsersFileTest {
  /**
   * Issue #10073: a taken user name is a conflict, not a security refusal. It must be raised as its own exception type, NOT
   * a {@link ServerSecurityException}, because the HTTP boundary maps that whole family to 403 "Security error" - the
   * status a non-root caller gets. Both creation entry points of {@link ServerSecurity} raise it: the node-local
   * {@code createUser} and the cluster-aware {@code createUserClusterWide} (which, with no HA plugin, delegates).
   */
  @Nested
  class UserAlreadyExists {
    private static final String CONFIG_PATH = "target/test-issue10073-security";

    private ServerSecurity security;

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      final File dir = new File(CONFIG_PATH);
      if (dir.exists())
        FileUtils.deleteRecursively(dir);
      dir.mkdirs();
      security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
    }

    @AfterEach
    void tearDown() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
    }

    private JSONObject user(final String name) {
      return new JSONObject().put("name", name).put("password", security.encodePassword("a-long-password"))
          .put("databases", new JSONObject());
    }

    @Test
    void createUserRaisesAConflictNotASecurityFailure() {
      security.createUser(user("alice"));

      assertThatThrownBy(() -> security.createUser(user("alice")))
          .isInstanceOf(UserAlreadyExistsException.class)
          .isNotInstanceOf(ServerSecurityException.class)
          .isNotInstanceOf(SecurityException.class)
          .hasMessageContaining("User 'alice' already exists");
    }

    @Test
    void createUserClusterWideRaisesTheSameConflict() {
      security.createUserClusterWide(user("bob"));

      assertThatThrownBy(() -> security.createUserClusterWide(user("bob")))
          .isInstanceOf(UserAlreadyExistsException.class)
          .isNotInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("User 'bob' already exists");
      assertThat(security.existsUser("bob")).isTrue();
    }
  }

  /**
   * Regression test for issue #10321: a security file that cannot be parsed must be reported under its OWN name and
   * preserved verbatim as {@code <name>-error.<ext>} before the fallback configuration replaces it.
   * <p>
   * Before the fix a {@code server-users.jsonl} parse error was routed to the groups repository: the SEVERE named
   * {@code server-groups.json}, {@code server-groups-error.json} was written with the (valid) in-memory groups document,
   * and the corrupt users file was lost as soon as the root user was saved over it.
   */
  @Nested
  class ParseErrorOnFirstLoad {

    private static final String CONFIG_PATH = "target/test-security-10321";

    /** The pretty-printed, not-JSONL file the native image used to ship (issue #10320). */
    private static final String MULTI_LINE_USERS = """
        {
          "root": {
            "access": [
              "updateSecurity"
            ],
            "databases": {
              "*": [
                "admin"
              ]
            }
          }
        }
        """;

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("PlayWithData123!");
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

    @Test
    void aMalformedUsersFileIsReportedAndPreservedUnderItsOwnName() throws Exception {
      final Path usersFile = Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
      Files.writeString(usersFile, MULTI_LINE_USERS, StandardCharsets.UTF_8);

      final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
      try {
        // Groups loaded first, as at server start: the old code then had a groups document to write in error.
        security.groupsToJSON();

        final List<String> lines = logged(security::loadUsers);

        final List<String> severe = lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnFirstLoad::isLoadFailure).toList();
        assertThat(severe).as("one SEVERE for the users file, naming it and where it was preserved").hasSize(1);
        assertThat(severe.getFirst()).contains(SecurityUserFileRepository.FILE_NAME).contains("server-users-error.jsonl")
            .doesNotContain(SecurityGroupFileRepository.FILE_NAME);

        final Path errorFile = Path.of(CONFIG_PATH, "server-users-error.jsonl");
        assertThat(errorFile).as("the corrupt users file is preserved").exists();
        assertThat(Files.readString(errorFile, StandardCharsets.UTF_8)).isEqualTo(MULTI_LINE_USERS);
        assertThat(Path.of(CONFIG_PATH, "server-groups-error.json")).as("the valid groups file is not reported").doesNotExist();

        assertOwnerOnly(errorFile);

        // The fallback still works: the root user from the configured password.
        assertThat(security.authenticate("root", "PlayWithData123!", null).getName()).isEqualTo("root");
        assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the fallback replaced the corrupt users file")
            .isNotEqualTo(MULTI_LINE_USERS);
      } finally {
        security.stopService();
      }
    }

    /** Valid JSON on every line but not a user record (no "name"): the users file is still the culprit. */
    @Test
    void aUsersFileWithAnInvalidRecordIsPreservedToo() throws Exception {
      final String content = "{\"nome\":\"bob\"}\n";
      Files.writeString(Path.of(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME), content, StandardCharsets.UTF_8);

      final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
      try {
        final List<String> lines = logged(security::loadUsers);

        assertThat(lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnFirstLoad::isLoadFailure).toList()).hasSize(1).first().asString()
            .contains(SecurityUserFileRepository.FILE_NAME);
        assertThat(Files.readString(Path.of(CONFIG_PATH, "server-users-error.jsonl"), StandardCharsets.UTF_8)).isEqualTo(content);
        assertThat(Path.of(CONFIG_PATH, "server-groups-error.json")).doesNotExist();
      } finally {
        security.stopService();
      }
    }

    /**
     * The same-shape sibling on the groups side: {@code saveInError} returned early because the document had not been
     * published yet, so the corrupt groups file was never preserved and the default document was saved over it.
     */
    @Test
    void aMalformedGroupsFileIsReportedAndPreservedUnderItsOwnName() throws Exception {
      final String content = "{ \"version\": 2, \"databases\": ";
      final Path groupsFile = Path.of(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);
      Files.writeString(groupsFile, content, StandardCharsets.UTF_8);

      final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 60_000);
      try {
        final List<String> lines = logged(repository::getGroups);

        final List<String> severe = lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnFirstLoad::isLoadFailure).toList();
        assertThat(severe).hasSize(1);
        assertThat(severe.getFirst()).contains(SecurityGroupFileRepository.FILE_NAME).contains("server-groups-error.json");

        final Path errorFile = Path.of(CONFIG_PATH, "server-groups-error.json");
        assertThat(errorFile).as("the corrupt groups file is preserved").exists();
        assertThat(Files.readString(errorFile, StandardCharsets.UTF_8)).isEqualTo(content);

        assertThat(repository.getGroups().getInt("version")).as("the default document is in force").isEqualTo(ServerSecurity.LATEST_VERSION);
        assertThat(Files.readString(groupsFile, StandardCharsets.UTF_8)).isNotEqualTo(content);
      } finally {
        repository.stop();
      }
    }

    /** A second failure replaces the earlier error file, and the copy leaves no temp file behind. */
    @Test
    void aLaterFailureReplacesTheErrorFileAndLeavesNoTempFile() throws Exception {
      final File file = new File(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
      final File errorFile = new File(CONFIG_PATH, SecurityUserFileRepository.ERROR_FILE_NAME);
      Files.writeString(errorFile.toPath(), "an older failure that is much longer than the new one\n", StandardCharsets.UTF_8);
      Files.writeString(file.toPath(), "{bad\n", StandardCharsets.UTF_8);

      final List<String> lines = logged(
          () -> SecurityUserFileRepository.preserveInError(this, file, errorFile, new RuntimeException("parse")));

      assertThat(lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnFirstLoad::isLoadFailure).toList()).hasSize(1).first().asString()
          .contains("saving the corrupt file as");
      assertThat(Files.readString(errorFile.toPath(), StandardCharsets.UTF_8)).isEqualTo("{bad\n");
      try (final Stream<Path> files = Files.list(Path.of(CONFIG_PATH))) {
        assertThat(files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".tmp")).toList()).isEmpty();
      }
    }

    /** When the copy cannot be written the SEVERE says so, rather than claiming the file was saved. */
    @Test
    void aCopyThatCannotBeWrittenIsReportedAsNotSaved() throws Exception {
      final File file = new File(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
      Files.writeString(file.toPath(), "{bad\n", StandardCharsets.UTF_8);
      // A non-empty directory where the error file should go: the publishing rename cannot replace it.
      final File errorFile = new File(CONFIG_PATH, SecurityUserFileRepository.ERROR_FILE_NAME);
      assertThat(errorFile.mkdirs()).isTrue();
      assertThat(new File(errorFile, "occupied").createNewFile()).isTrue();

      final List<String> lines = logged(
          () -> SecurityUserFileRepository.preserveInError(this, file, errorFile, new RuntimeException("parse")));

      assertThat(lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnFirstLoad::isLoadFailure).toList()).hasSize(1).first().asString()
          .contains("could not be saved as").doesNotContain("saving the corrupt file as");
      try (final Stream<Path> files = Files.list(Path.of(CONFIG_PATH))) {
        assertThat(files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".tmp")).toList()).isEmpty();
      }
    }

    /** The file vanished between the failed load and the report: nothing to copy, and the SEVERE says so. */
    @Test
    void aFileThatIsGoneIsReportedAsNothingSaved() {
      final File file = new File(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
      final File errorFile = new File(CONFIG_PATH, SecurityUserFileRepository.ERROR_FILE_NAME);

      final List<String> lines = logged(
          () -> SecurityUserFileRepository.preserveInError(this, file, errorFile, new RuntimeException("parse")));

      assertThat(lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnFirstLoad::isLoadFailure).toList()).hasSize(1).first().asString()
          .contains("nothing was saved");
      assertThat(errorFile).doesNotExist();
    }

    /** Control: a well-formed users file logs nothing and leaves no error file. */
    @Test
    void aValidUsersFileLeavesNoErrorFile() {
      final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
      try {
        security.loadUsers();
        security.stopService();

        final ServerSecurity reloaded = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
        try {
          final List<String> lines = logged(reloaded::loadUsers);
          assertThat(lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnFirstLoad::isLoadFailure).toList()).isEmpty();
          assertThat(Path.of(CONFIG_PATH, "server-users-error.jsonl")).doesNotExist();
        } finally {
          reloaded.stopService();
        }
      } finally {
        security.stopService();
      }
    }

    /** Only this fix's messages: a SEVERE from an unrelated component cannot make a count assertion flaky. */
    private static boolean isLoadFailure(final String line) {
      return line.startsWith("SEVERE") && line.contains("Error on loading file");
    }

    private static void assertOwnerOnly(final Path path) throws Exception {
      final PosixFileAttributeView view = Files.getFileAttributeView(path, PosixFileAttributeView.class);
      if (view == null)
        return;
      assertThat(view.readAttributes().permissions()).as("the preserved users file carries password hashes")
          .isEqualTo(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    }

    /** Runs {@code action} capturing everything logged as "LEVEL message args". */
    private static List<String> logged(final Runnable action) {
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
        action.run();
      } finally {
        LogManager.instance().setLogger(previous);
      }
      return new ArrayList<>(lines);
    }
  }

  /**
   * Regression test for issue #10326: a {@code server-users.jsonl} that cannot be loaded when the users-file watcher
   * RELOADS it on a running server must leave the users already in force untouched. Before the fix the reload took the
   * startup fallback - a single passwordless {@code root} from {@link SecurityUserFileRepository#createDefault()} - so every
   * other user was dropped at once (and their sessions revoked), and {@code askForRootPassword()} re-created {@code root}
   * and saved it over the operator's file.
   * <p>
   * The first load keeps falling back to the default (there is no previous list to keep); that path is covered by
   * {@link ServerSecurityUsersFileTest.ParseErrorOnFirstLoad}.
   */
  @Nested
  class ParseErrorOnReload {

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
              .until(() -> captured.stream().anyMatch(ServerSecurityUsersFileTest.ParseErrorOnReload::isLoadFailure));
        });

        assertUsersInForce(security);
        assertThat(Files.readString(errorFile, StandardCharsets.UTF_8)).as("the operator's edit is preserved").isEqualTo(MALFORMED);
        assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("nothing was saved over the operator's file")
            .isEqualTo(MALFORMED);

        final List<String> severe = lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnReload::isLoadFailure).toList();
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
        assertThat(lines.stream().filter(ServerSecurityUsersFileTest.ParseErrorOnReload::isLoadFailure).toList()).hasSize(1).first().asString()
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

  /**
   * Regression test for issue #10451, the users half of #10403: a replicated user list is published in memory even when its
   * write fails (#7137), so the file on disk is then OLDER than the users in force. A reload of that file - fired by anything
   * that advances its stamp, an operator touching it or a backup tool - must not publish it over the replicated list, or a
   * user the cluster just dropped (or a password it just rotated) works again on this node.
   */
  @Nested
  class DiskBehindReplicatedUsers {

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

  /**
   * Regression test for issue #10460: the users watcher reads {@code server-users.jsonl} and publishes what it read under its
   * own lock, but a replicated install ({@code applyReplicatedUsers}, on the Raft apply thread) and a local user change swap
   * or edit the users in force WITHOUT that lock. A reload that had already read the file must not publish it over a change
   * installed after the read, or that change is reverted on this node.
   * <p>
   * The interleaving is forced, not raced: the reload is driven on the test thread through {@code loadUsers()} with the timer
   * watcher off, and the concurrent change runs inside the reload, between its {@code read()} and its {@code publish()}.
   */
  @Nested
  class ReloadVersusInstallRace {

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
  @Nested
  class SaveAndSwap {

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
      if (security != null) {
        security.afterLocalUsersSave(null);
        security.beforeReplicatedUsersSwap(null);
        security.stopService();
      }
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
     * Starts {@code action} on a thread and waits until it is done or cannot go on: blocked on a lock held by the caller, which is what
     * the protection of the pair looks like from outside. Without it the action runs to the end here, which is the interleaving of the
     * issue.
     */
    private static Thread runAndWaitUntilDoneOrParked(final Runnable action) {
      final Thread thread = new Thread(action, "issue-10481-second-party");
      thread.setDaemon(true);
      thread.start();
      final long deadline = System.nanoTime() + 10_000_000_000L;
      while (System.nanoTime() < deadline) {
        // The lock is a monitor: a thread that cannot go on is BLOCKED on it, and any other state is a thread still on its way
        final Thread.State state = thread.getState();
        if (state == Thread.State.TERMINATED || state == Thread.State.BLOCKED)
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

  /**
   * Regression test for issue #10535: the no-argument {@code saveUsers()} serialised the live user map after its caller had
   * already mutated a live user, the order the javadoc of its sibling says must not happen (#10461). On a read-only
   * configuration directory, the ordinary shape of a Kubernetes ConfigMap mount, the mutation stayed authoritative in memory
   * while nothing reached the disk, and the next reload reverted it.
   * <p>
   * Every user mutation must either reach the disk or be refused, so each case makes the directory read-only and asserts that
   * what is in force still equals what the file holds.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MutationPersistBeforePublish {

    private static final String CONFIG_PATH   = "target/test-security-10535";
    private static final String ROOT_PASSWORD = "PlayWithData123!";
    private static final String PASSWORD      = "alice-password-secret";

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

      security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
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

    /**
     * The root password path of {@code askForRootPassword()}: a users file whose root has no password. The new password used
     * to be set on the live root before the save, so a failed save left root authenticating against a password the file
     * never received.
     */
    @Test
    void aFailedRootPasswordSaveLeavesMemoryAsTheFile() throws Exception {
      security.stopService();
      final String rootWithoutPassword = new JSONObject().put("name", "root")
          .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))).toString() + "\n";
      Files.writeString(usersFile, rootWithoutPassword, StandardCharsets.UTF_8);
      makeReadOnly();

      security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
      assumeTrue(failsToLoad(), "the directory is still writable (running as a superuser?)");

      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the refusal writes nothing").isEqualTo(rootWithoutPassword);
      assertThat(security.getUser("root").getPassword()).as("a password that never reached the disk must not be in force").isNull();
    }

    /** The server's default-database path: a grant is in force and in the file, and keeps the groups the user already held. */
    @Test
    void aGrantIsPersistedAndPublishedAndMergesTheGroups() throws Exception {
      security.createUser(aliceJson());

      security.addDatabaseToUser("alice", "Universe", new String[] { "admin" });
      security.addDatabaseToUser("alice", "Universe", new String[] { "reader" });

      assertThat(security.getUser("alice").canAccessToDatabase("Universe")).isTrue();
      assertThat(security.getUser("alice").toJSON().getJSONObject("databases").getJSONArray("Universe").toList())
          .containsExactlyInAnyOrder("admin", "reader");
      final String onDisk = Files.readString(usersFile, StandardCharsets.UTF_8);
      assertThat(onDisk).contains("Universe").contains("admin").contains("reader");

      // What reached the disk is what a restart reads back
      security.stopService();
      security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
      security.loadUsers();
      assertThat(security.getUser("alice").canAccessToDatabase("Universe")).isTrue();
    }

    /** A user created without the {@code databases} entry at all, which the grant has to add rather than trip over. */
    @Test
    void aGrantToAUserWithoutADatabasesEntryCreatesIt() {
      security.createUser(new JSONObject().put("name", "bob").put("password", security.encodePassword(PASSWORD)));

      security.addDatabaseToUser("bob", "Universe", new String[] { "admin" });

      assertThat(security.getUser("bob").canAccessToDatabase("Universe")).isTrue();
    }

    /** The refusal that used to be impossible to see from memory: the grant was already in force when the save failed. */
    @Test
    void aFailedGrantLeavesMemoryAsTheFile() throws Exception {
      security.createUser(aliceJson());
      final String aliceOnDisk = Files.readString(usersFile, StandardCharsets.UTF_8);
      makeReadOnly();

      assumeTrue(grantFails("alice", "Universe"), "the directory is still writable (running as a superuser?)");

      assertThat(security.getUser("alice").canAccessToDatabase("Universe")).as("a grant that never reached the disk is not in force").isFalse();
      assertThat(security.getUser("alice").toJSON().getJSONObject("databases").has("Universe")).isFalse();
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the refusal writes nothing").isEqualTo(aliceOnDisk);

      // Nothing was left half-applied: once the directory is writable the same grant goes through
      makeWritable();
      security.addDatabaseToUser("alice", "Universe", new String[] { "admin" });
      assertThat(security.getUser("alice").canAccessToDatabase("Universe")).isTrue();
    }

    /** Repeating what the user already holds writes nothing, so the server's startup does not need a writable directory for it. */
    @Test
    void repeatingAGrantTheUserAlreadyHoldsWritesNothing() throws Exception {
      security.createUser(aliceJson());
      security.addDatabaseToUser("alice", "Universe", new String[] { "admin" });
      final ServerSecurityUser held = security.getUser("alice");
      makeReadOnly();

      assertThat(security.addDatabaseToUser("alice", "Universe", new String[] { "admin" })).isSameAs(held);
      assertThat(security.getUser("alice")).isSameAs(held);
    }

    @Test
    void aGrantToAnUnknownUserIsRefused() {
      assertThatThrownBy(() -> security.addDatabaseToUser("nobody", "Universe", new String[] { "admin" }))
          .isInstanceOf(ServerSecurityException.class).hasMessageContaining("nobody");
    }

    private boolean grantFails(final String userName, final String databaseName) {
      try {
        security.addDatabaseToUser(userName, databaseName, new String[] { "admin" });
        return false;
      } catch (final ServerSecurityException expected) {
        return true;
      }
    }

    private JSONObject aliceJson() {
      return new JSONObject().put("name", "alice").put("password", security.encodePassword(PASSWORD))
          .put("databases", new JSONObject());
    }

    private boolean failsToLoad() {
      try {
        security.loadUsers();
        return false;
      } catch (final ServerSecurityException expected) {
        return true;
      }
    }

    private void makeReadOnly() throws Exception {
      dirView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
    }

    private void makeWritable() throws Exception {
      if (dirView != null)
        dirView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
    }
  }
}
