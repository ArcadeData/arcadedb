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
import com.arcadedb.InstanceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class InstanceIdResolverTest {
  private static final String VALID = "adb-123e4567-e89b-12d3-a456-426614174000";

  @TempDir
  Path dir;

  private static ContextConfiguration config(final String setting) {
    final ContextConfiguration cfg = new ContextConfiguration();
    if (setting != null)
      cfg.setValue(GlobalConfiguration.INSTANCE_ID, setting);
    return cfg;
  }

  @Test
  void generatesAndPersistsOnFirstStartThenReuses() throws IOException {
    final String first = InstanceIdResolver.resolve(config(null), dir);

    assertThat(InstanceId.isValid(first)).isTrue();
    assertThat(Files.readString(dir.resolve(InstanceIdResolver.FILE_NAME)).trim()).isEqualTo(first);
    assertThat(InstanceIdResolver.resolve(config(null), dir)).isEqualTo(first);
    try (final var files = Files.list(dir)) {
      assertThat(files.count()).isEqualTo(1);
    }
  }

  @Test
  void validSettingWinsOverTheFile() throws IOException {
    Files.writeString(dir.resolve(InstanceIdResolver.FILE_NAME), InstanceId.generate());

    assertThat(InstanceIdResolver.resolve(config(VALID.toUpperCase()), dir)).isEqualTo(VALID);
  }

  @Test
  void malformedSettingIsIgnored() {
    final String id = InstanceIdResolver.resolve(config("not-an-id"), dir);

    assertThat(InstanceId.isValid(id)).isTrue();
    assertThat(dir.resolve(InstanceIdResolver.FILE_NAME)).exists();
  }

  @Test
  void invalidFileIsKeptAsInvalidAndRegenerated() throws IOException {
    Files.writeString(dir.resolve(InstanceIdResolver.FILE_NAME), "garbage");

    final String id = InstanceIdResolver.resolve(config(null), dir);

    assertThat(InstanceId.isValid(id)).isTrue();
    assertThat(Files.readString(dir.resolve(InstanceIdResolver.INVALID_FILE_NAME))).isEqualTo("garbage");
    assertThat(Files.readString(dir.resolve(InstanceIdResolver.FILE_NAME)).trim()).isEqualTo(id);
  }

  @Test
  void readOnlyConfigDirectoryFallsBackWithoutThrowing() {
    assumeThat(dir.toFile().setWritable(false)).isTrue();
    try {
      assumeThat(dir.toFile().canWrite()).as("running as a user that ignores permissions").isFalse();

      final String id = InstanceIdResolver.resolve(config(null), dir);

      assertThat(InstanceId.isValid(id)).isTrue();
      assertThat(InstanceIdResolver.resolve(config(null), dir)).isEqualTo(id);
      assertThat(dir.resolve(InstanceIdResolver.FILE_NAME)).doesNotExist();
    } finally {
      dir.toFile().setWritable(true);
    }
  }

  @Test
  void missingConfigDirectoryFallsBackWithoutThrowing() {
    assertThat(InstanceId.isValid(InstanceIdResolver.resolve(config(null), dir.resolve("missing")))).isTrue();
  }
}
