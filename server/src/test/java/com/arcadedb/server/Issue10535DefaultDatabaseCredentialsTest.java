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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.security.SecurityUserFileRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #10535: the credentials of a default database ({@code name[user:password:group]}) used to grant the database to an
 * existing user by changing the live user and saving the whole map after it. A configuration directory that cannot be written, the
 * ordinary shape of a Kubernetes ConfigMap mount, left the grant in force in memory with nothing on disk, and for a user that
 * already had access the failed save was reported as {@code "already exists with a different password"}.
 * <p>
 * The grant is now made on a copy and persisted before it is published, so a startup that cannot write it fails with the reason, and
 * a grant the user already holds is not written at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10535DefaultDatabaseCredentialsTest extends StaticBaseServerTest {
  private static final String DATABASE = "Db10535";
  private static final String USER     = "alice10535";
  private static final String PASSWORD = "Password10535!";

  private final File configDirectory = new File("./target/config");
  private final Path usersFile       = configDirectory.toPath().resolve(SecurityUserFileRepository.FILE_NAME);
  private       ArcadeDBServer server;

  @AfterEach
  @Override
  public void endTest() {
    try {
      stopServer();
    } finally {
      // Writable again before the helper deletes the directory, or it stays behind for the next class of the fork
      configDirectory.setWritable(true);
      super.endTest();
    }
  }

  /** A user that can already reach the database and proves it with its password gains the new group, persisted. */
  @Test
  void anExistingUserWithTheRightPasswordGainsTheGroup() throws Exception {
    startWith(credentials(PASSWORD, "admin"));
    stopServer();

    startWith(credentials(PASSWORD, "extra"));

    assertThat(groupsOfUser()).containsExactlyInAnyOrder("admin", "extra");
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).contains("extra");
  }

  /** The mismatch the warning describes: nothing is granted to a caller that does not know the password. */
  @Test
  void aWrongPasswordGrantsNothing() {
    startWith(credentials(PASSWORD, "admin"));
    stopServer();

    startWith(credentials("another-Password10535!", "extra"));

    assertThat(groupsOfUser()).containsExactly("admin");
  }

  /**
   * A users file that cannot be written fails the startup with the reason: the grant is not in force and nothing says it is, where it
   * used to be in force in memory and blamed on the password.
   */
  @Test
  void anUnwritableUsersFileFailsTheStartupInsteadOfBlamingThePassword() throws Exception {
    startWith(credentials(PASSWORD, "admin"));
    stopServer();
    final String beforeTheRefusal = Files.readString(usersFile, StandardCharsets.UTF_8);
    configDirectory.setWritable(false);
    assumeTrue(!configDirectory.canWrite(), "the directory is still writable (running as a superuser?)");

    server = new ArcadeDBServer(configuration(credentials(PASSWORD, "extra")));

    assertThatThrownBy(() -> server.start()).hasStackTraceContaining(SecurityUserFileRepository.FILE_NAME);
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the refusal writes nothing").isEqualTo(beforeTheRefusal);
  }

  /** What the first start made is what every later start asks for: it needs no write, so a read-only directory is fine. */
  @Test
  void aGrantTheUserAlreadyHoldsNeedsNoWrite() {
    startWith(credentials(PASSWORD, "admin"));
    stopServer();
    configDirectory.setWritable(false);
    assumeTrue(!configDirectory.canWrite(), "the directory is still writable (running as a superuser?)");

    startWith(credentials(PASSWORD, "admin"));

    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
    assertThat(groupsOfUser()).containsExactly("admin");
  }

  /**
   * An entry written without a group, {@code Db[user:password]}: the user is created, the grant reaches the file, and a restart reads the
   * file back and finds the same user able to authenticate against the database.
   */
  @Test
  void anEntryWithoutAGroupRoundTripsThroughTheUsersFile() throws Exception {
    final String withoutGroup = DATABASE + "[" + USER + ":" + PASSWORD + "]";
    startWith(withoutGroup);
    assertThat(server.getSecurity().authenticate(USER, PASSWORD, DATABASE).getName()).isEqualTo(USER);
    final String firstStart = Files.readString(usersFile, StandardCharsets.UTF_8);
    assertThat(firstStart).contains(USER).contains(DATABASE);
    stopServer();

    // The grant is held, so the restart writes nothing: on a directory that cannot be written it still starts, and the file is as it was
    configDirectory.setWritable(false);
    assumeTrue(!configDirectory.canWrite(), "the directory is still writable (running as a superuser?)");
    startWith(withoutGroup);

    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
    assertThat(server.getSecurity().authenticate(USER, PASSWORD, DATABASE).getName()).isEqualTo(USER);
    assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("a restart does not rewrite the file").isEqualTo(firstStart);
  }

  private static String credentials(final String password, final String group) {
    return DATABASE + "[" + USER + ":" + password + ":" + group + "]";
  }

  private List<Object> groupsOfUser() {
    return server.getSecurity().getUser(USER).toJSON().getJSONObject("databases").getJSONArray(DATABASE).toList();
  }

  private void startWith(final String defaultDatabases) {
    server = new ArcadeDBServer(configuration(defaultDatabases));
    server.start();
  }

  private static ContextConfiguration configuration(final String defaultDatabases) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, defaultDatabases);
    return config;
  }

  private void stopServer() {
    if (server == null)
      return;
    try {
      if (server.isStarted())
        server.stop();
    } finally {
      server = null;
    }
  }
}
