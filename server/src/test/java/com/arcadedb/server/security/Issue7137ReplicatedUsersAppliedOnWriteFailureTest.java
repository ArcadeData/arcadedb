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
import com.arcadedb.server.ServerException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the security half of issue #7137.
 * <p>
 * Making a users-file write failure non-fatal to the node (so a full or read-only config volume stops
 * crash-looping the pod) must not turn into "this node keeps honouring credentials the operator has just
 * revoked". Returning early on the write failure would do exactly that: the entry committed cluster-wide, every
 * other node applied it, and this one would go on authenticating against the PREVIOUS list for as long as the
 * volume stayed broken.
 * <p>
 * So the new list is published in memory FIRST and the persistence failure is reported afterwards. The
 * revocation takes effect here immediately; what is outstanding is only its durability, and the Raft entry is
 * replayed on the next start.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7137ReplicatedUsersAppliedOnWriteFailureTest {

  private static final String CONFIG_PATH = "target/test-security-7137";
  private static final String PASSWORD    = "new-password-secret";

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /**
   * Makes the users file unwritable in a way that is deterministic on every platform and does not depend on
   * running as an unprivileged user: the target path is a non-empty directory, so the publishing rename cannot
   * replace it. That stands in for the full / read-only / bad-mode volume of the report.
   */
  private static void makeUsersFileUnwritable() throws Exception {
    final File asDirectory = new File(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME);
    FileUtils.deleteRecursively(asDirectory);
    assertThat(asDirectory.mkdirs()).isTrue();
    assertThat(new File(asDirectory, "occupied").createNewFile()).isTrue();
  }

  private static String usersPayload(final ServerSecurity security) {
    return new JSONArray()
        .put(new JSONObject()
            .put("name", "alice")
            .put("password", security.encodePassword(PASSWORD))
            .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))))
        .toString();
  }

  @Test
  void aWriteFailureIsReportedButTheNewListIsStillEnforced() throws Exception {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
    makeUsersFileUnwritable();

    assertThatThrownBy(() -> security.applyReplicatedUsers(usersPayload(security)))
        .as("the caller must learn the list did not reach disk")
        .isInstanceOf(ServerException.class)
        .hasMessageContaining(SecurityUserFileRepository.FILE_NAME);

    // The crux: the entry was applied in memory anyway, so a password change or a drop the operator just made
    // is effective on this node NOW rather than after the volume is fixed.
    assertThatCode(() -> assertThat(security.authenticate("alice", PASSWORD, null).getName()).isEqualTo("alice"))
        .doesNotThrowAnyException();
  }

  /** Control: with a writable file the same payload applies cleanly and reports nothing. */
  @Test
  void aWritableFileAppliesWithoutReportingAFailure() {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);

    assertThatCode(() -> security.applyReplicatedUsers(usersPayload(security))).doesNotThrowAnyException();
    assertThat(security.authenticate("alice", PASSWORD, null).getName()).isEqualTo("alice");
    assertThat(new File(CONFIG_PATH, SecurityUserFileRepository.FILE_NAME)).isFile();
  }
}
