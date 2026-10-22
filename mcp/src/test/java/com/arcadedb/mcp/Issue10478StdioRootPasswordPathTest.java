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
package com.arcadedb.mcp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.security.ServerSecurityException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10478: the MCP stdio mode read only {@code arcadedb.server.rootPassword}, so a server configured the recommended way, with
 * the root password in a file ({@code arcadedb.server.rootPasswordPath}, a mounted Secret), could not run it although the server
 * itself honours that file. The password is resolved the way {@code ServerSecurity} resolves it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10478StdioRootPasswordPathTest {
  @TempDir
  Path directory;

  @Test
  void thePasswordOfTheFileIsUsedWhenNoPasswordIsSet() throws Exception {
    final Path file = Files.writeString(directory.resolve("root-password"), "from-the-file\n");

    assertThat(MCPStdioServer.resolveRootPassword(configuration(null, file))).isEqualTo("from-the-file");
  }

  @Test
  void thePasswordSetDirectlyWinsOverTheFile() throws Exception {
    final Path file = Files.writeString(directory.resolve("root-password"), "from-the-file");

    assertThat(MCPStdioServer.resolveRootPassword(configuration("from-the-setting", file))).isEqualTo("from-the-setting");
  }

  @Test
  void noPasswordAtAllResolvesToNothing() {
    assertThat(MCPStdioServer.resolveRootPassword(configuration(null, null))).isNull();
    assertThat(MCPStdioServer.resolveRootPassword(configuration("", null))).isNull();
  }

  /** An empty direct setting does not shadow the file: it is the same as not setting it. */
  @Test
  void anEmptyPasswordSettingFallsBackToTheFile() throws Exception {
    final Path file = Files.writeString(directory.resolve("root-password"), "from-the-file");

    assertThat(MCPStdioServer.resolveRootPassword(configuration("", file))).isEqualTo("from-the-file");
  }

  @Test
  void aMissingFileIsReportedWithItsPath() {
    final Path missing = directory.resolve("missing");

    assertThatThrownBy(() -> MCPStdioServer.resolveRootPassword(configuration(null, missing))).isInstanceOf(
        ServerSecurityException.class).hasMessageContaining(missing.toString());
  }

  @Test
  void anEmptyFileIsReportedWithItsPath() throws Exception {
    final Path empty = Files.writeString(directory.resolve("empty"), "\n");

    assertThatThrownBy(() -> MCPStdioServer.resolveRootPassword(configuration(null, empty))).isInstanceOf(
        ServerSecurityException.class).hasMessageContaining(empty.toString()).hasMessageContaining("empty");
  }

  /** The message names both settings, so an operator who set neither knows the file is an option. */
  @Test
  void theMissingPasswordMessageNamesBothSettings() {
    assertThat(MCPStdioServer.MISSING_ROOT_PASSWORD_MESSAGE).contains(GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey())
        .contains(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.getKey());
  }

  private static ContextConfiguration configuration(final String password, final Path passwordPath) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, password);
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordPath != null ? passwordPath.toString() : null);
    return configuration;
  }
}
