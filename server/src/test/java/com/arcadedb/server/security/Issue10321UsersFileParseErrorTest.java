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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10321: a security file that cannot be parsed must be reported under its OWN name and
 * preserved verbatim as {@code <name>-error.<ext>} before the fallback configuration replaces it.
 * <p>
 * Before the fix a {@code server-users.jsonl} parse error was routed to the groups repository: the SEVERE named
 * {@code server-groups.json}, {@code server-groups-error.json} was written with the (valid) in-memory groups document,
 * and the corrupt users file was lost as soon as the root user was saved over it.
 */
class Issue10321UsersFileParseErrorTest {

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

      final List<String> severe = lines.stream().filter(Issue10321UsersFileParseErrorTest::isLoadFailure).toList();
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

      assertThat(lines.stream().filter(Issue10321UsersFileParseErrorTest::isLoadFailure).toList()).hasSize(1).first().asString()
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

      final List<String> severe = lines.stream().filter(Issue10321UsersFileParseErrorTest::isLoadFailure).toList();
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

    assertThat(lines.stream().filter(Issue10321UsersFileParseErrorTest::isLoadFailure).toList()).hasSize(1).first().asString()
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

    assertThat(lines.stream().filter(Issue10321UsersFileParseErrorTest::isLoadFailure).toList()).hasSize(1).first().asString()
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

    assertThat(lines.stream().filter(Issue10321UsersFileParseErrorTest::isLoadFailure).toList()).hasSize(1).first().asString()
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
        assertThat(lines.stream().filter(Issue10321UsersFileParseErrorTest::isLoadFailure).toList()).isEmpty();
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
