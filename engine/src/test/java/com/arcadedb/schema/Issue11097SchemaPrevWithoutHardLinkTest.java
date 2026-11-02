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
package com.arcadedb.schema;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mockStatic;

/**
 * #11097: schema.prev.json must be a plain copy, not a hard link, when arcadedb.schema.hardLinkPrevious is false.
 */
class Issue11097SchemaPrevWithoutHardLinkTest extends TestHelper {
  @Test
  void settingDefaultsToHardLink() {
    assertThat(GlobalConfiguration.SCHEMA_HARD_LINK_PREVIOUS.getDefValue()).isEqualTo(true);
  }

  @Test
  void schemaSaveWritesAPlainCopyWhenHardLinkIsDisabled() throws Exception {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("unix"));
    database.getConfiguration().setValue(GlobalConfiguration.SCHEMA_HARD_LINK_PREVIOUS, false);
    database.getSchema().createDocumentType("First");
    final Path schema = Path.of(database.getDatabasePath(), "schema.json");
    final Path previous = Path.of(database.getDatabasePath(), "schema.prev.json");
    final String before = Files.readString(schema);

    try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
      if (invocation.getMethod().getName().equals("createLink"))
        throw new AssertionError("schema.prev.json must not be a hard link");
      return invocation.callRealMethod();
    })) {
      database.getSchema().createDocumentType("Second");
    }

    assertThat(Files.readString(previous)).isEqualTo(before);
    assertThat(Files.getAttribute(previous, "unix:ino")).isNotEqualTo(Files.getAttribute(schema, "unix:ino"));
  }

  @Test
  void schemaSaveStillLinksByDefault() throws Exception {
    database.getSchema().createDocumentType("First");
    final Path previous = Path.of(database.getDatabasePath(), "schema.prev.json");
    final int[] links = { 0 };

    try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
      if (invocation.getMethod().getName().equals("createLink"))
        links[0]++;
      return invocation.callRealMethod();
    })) {
      database.getSchema().createDocumentType("Second");
    }

    assertThat(links[0]).as("the default keeps the hard link fast path").isEqualTo(1);
    assertThat(Files.exists(previous)).isTrue();
  }
}
