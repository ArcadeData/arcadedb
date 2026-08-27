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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@code arcadedb.server.rootPasswordPath}, the way the shipped Kubernetes StatefulSet
 * supplies the root password: a Secret mounted as a file rather than an argument on the command
 * line, which any process in the pod can read out of /proc. Every ordinary way of producing that
 * file appends a trailing newline, and storing it as part of the password would leave the operator
 * with a root password nobody can type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RootPasswordFromFileTest {

  private static final String PASSWORD    = "dD5ed08c";
  private static final String CONFIG_PATH = "./target/rootPasswordFromFileTest";

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
    new File(CONFIG_PATH).mkdirs();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
    GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.setValue(null);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
    GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.setValue(null);
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  @Test
  void passwordFileWithoutTrailingNewlineIsUsedVerbatim() throws Exception {
    assertRootPasswordFileYields(PASSWORD, PASSWORD);
  }

  @Test
  void trailingUnixNewlineIsNotPartOfThePassword() throws Exception {
    assertRootPasswordFileYields(PASSWORD + "\n", PASSWORD);
  }

  @Test
  void trailingWindowsNewlineIsNotPartOfThePassword() throws Exception {
    assertRootPasswordFileYields(PASSWORD + "\r\n", PASSWORD);
  }

  @Test
  void anEmptyPasswordFileIsRejectedWithTheOffendingPath() throws Exception {
    final Path passwordFile = writePasswordFile("\n");
    GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.setValue(passwordFile.toString());

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
    try {
      assertThatThrownBy(security::loadUsers).isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining(passwordFile.toString());
    } finally {
      security.stopService();
    }
  }

  @Test
  void anUnreadablePasswordFileIsRejectedWithTheOffendingPath() {
    final String missing = CONFIG_PATH + "/does-not-exist";
    GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.setValue(missing);

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
    try {
      assertThatThrownBy(security::loadUsers).isInstanceOf(ServerSecurityException.class).hasMessageContaining(missing);
    } finally {
      security.stopService();
    }
  }

  @Test
  void onlyTheFinalLineTerminatorIsStripped() {
    // A password may legitimately end with spaces, and only the terminator the file format adds is noise.
    assertThat(ServerSecurity.stripTrailingNewline("a b ")).isEqualTo("a b ");
    assertThat(ServerSecurity.stripTrailingNewline("a\nb\n")).isEqualTo("a\nb");
    assertThat(ServerSecurity.stripTrailingNewline("a\n\n")).isEqualTo("a\n");
    assertThat(ServerSecurity.stripTrailingNewline("a \n")).isEqualTo("a ");
    assertThat(ServerSecurity.stripTrailingNewline("")).isEmpty();
    assertThat(ServerSecurity.stripTrailingNewline(null)).isNull();
  }

  private void assertRootPasswordFileYields(final String fileContent, final String expectedPassword) throws Exception {
    GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.setValue(writePasswordFile(fileContent).toString());

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
    try {
      security.startService();
      security.loadUsers();

      final String storedHash = security.getUser("root").getPassword();
      assertThat(security.passwordMatch(expectedPassword, storedHash)).isTrue();
      if (!fileContent.equals(expectedPassword))
        assertThat(security.passwordMatch(fileContent, storedHash)).isFalse();
    } finally {
      security.stopService();
    }
  }

  private Path writePasswordFile(final String content) throws Exception {
    final Path passwordFile = Path.of(CONFIG_PATH, "rootPassword");
    Files.writeString(passwordFile, content);
    return passwordFile;
  }
}
