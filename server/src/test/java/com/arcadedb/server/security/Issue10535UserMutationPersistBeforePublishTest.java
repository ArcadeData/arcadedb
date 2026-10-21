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
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
class Issue10535UserMutationPersistBeforePublishTest {

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
