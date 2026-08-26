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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6297: {@code TestServerHelper.deleteDatabaseFolders} resolves every path it deletes from the live
 * configuration, so a caller that resets first does not merely fail to clean up - it asks for a recursive delete of
 * absolute root-level paths.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6297TeardownPathResolutionTest {

  /**
   * The trap itself, pinned so it cannot be rediscovered: {@code SERVER_DATABASE_DIRECTORY} defaults to
   * {@code ${arcadedb.server.rootPath}/databases} over a root path that defaults to {@code null}, and the resolver
   * substitutes an empty string for an unknown variable rather than failing.
   */
  @Test
  void aResetConfigurationResolvesTheDatabaseDirectoryToTheFilesystemRoot() {
    GlobalConfiguration.resetAll();
    final String resolved = GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString();

    assertThat(resolved).isEqualTo("/databases");
    assertThat(TestServerHelper.isResolvedTestPath(resolved)).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath(resolved + 0 + File.separator)).isFalse();
  }

  @Test
  void theGuardAcceptsEveryShapeATestActuallyConfigures(@TempDir final Path tempDir) {
    assertThat(TestServerHelper.isResolvedTestPath("./target/databases0" + File.separator)).isTrue();
    assertThat(TestServerHelper.isResolvedTestPath("target/databases")).isTrue();
    assertThat(TestServerHelper.isResolvedTestPath(tempDir.resolve("databases").toString())).isTrue();
    assertThat(TestServerHelper.isResolvedTestPath(tempDir.resolve("databases") + "0" + File.separator)).isTrue();

    assertThat(TestServerHelper.isResolvedTestPath(null)).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("")).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("   ")).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("/")).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("/databases")).isFalse();

    // The suffix is appended before the check, not after it, so these are the strings actually asked about: a
    // folder the collapsed placeholder named stays refused once the per-server digit and separator are on it, and
    // a root path that itself collapsed to "/" is refused for its replication folder too.
    assertThat(TestServerHelper.isResolvedTestPath("/databases" + 0 + File.separator)).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("/" + File.separator + "replication")).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("./target" + File.separator + "replication")).isTrue();
  }

  /**
   * The two orderings, end to end: with the configuration live the per-server folder goes, and with it reset the
   * same call touches nothing at all instead of walking off to {@code /databases0}.
   */
  @Test
  void deleteDatabaseFoldersCleansWhenResolvedAndRefusesWhenNot(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path server0 = root.resolve("databases0");
    Files.createDirectories(server0);
    Files.writeString(server0.resolve("marker.txt"), "issue-6297");

    try {
      GlobalConfiguration.SERVER_ROOT_PATH.setValue(root.toString());
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(root.resolve("databases").toString());
      TestServerHelper.deleteDatabaseFolders(1);
      assertThat(server0).as("the folder the live configuration names is the one that gets cleaned").doesNotExist();

      Files.createDirectories(server0);
      Files.writeString(server0.resolve("marker.txt"), "issue-6297");

      GlobalConfiguration.resetAll();
      TestServerHelper.deleteDatabaseFolders(1);
      assertThat(server0).as("a cleanup running after the reset must delete nothing rather than delete /databases0")
          .exists();
    } finally {
      GlobalConfiguration.resetAll();
    }
  }
}
