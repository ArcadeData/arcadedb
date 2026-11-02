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
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

/**
 * Issue #11097: {@code schema.prev.json} was always published as a hard link to {@code schema.json}. A file system
 * that accepts the link but loses the data once the original name is replaced (SeaweedFS FUSE) left the fallback copy
 * unreadable. {@link GlobalConfiguration#SCHEMA_HARD_LINK_PREVIOUS} turns the link off in favor of a byte copy.
 */
class Issue11097SchemaPrevWithoutHardLinkTest extends TestHelper {

  @Test
  void schemaSaveLinksPreviousGenerationByDefault() {
    assertThat(linkAttemptsDuringSchemaChange("DefaultLink")).isPositive();
  }

  @Test
  void schemaSaveNeverLinksWhenTheSettingIsOff() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.SCHEMA_HARD_LINK_PREVIOUS, false);

    assertThat(linkAttemptsDuringSchemaChange("NoLink")).isZero();

    final Path dir = Path.of(database.getDatabasePath());
    final Path schema = dir.resolve("schema.json");
    final Path prev = dir.resolve("schema.prev.json");
    assertThat(Files.isSameFile(schema, prev)).isFalse();

    // Second change: schema.prev.json must hold the complete previous generation, readable on its own.
    final String generationBefore = Files.readString(schema);
    database.getSchema().createDocumentType("NoLink2");
    assertThat(Files.readString(prev)).isEqualTo(generationBefore);
  }

  private int linkAttemptsDuringSchemaChange(final String typeName) {
    final AtomicInteger attempts = new AtomicInteger();
    try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
      if (invocation.getMethod().getName().equals("createLink"))
        attempts.incrementAndGet();
      return invocation.callRealMethod();
    })) {
      database.getSchema().createDocumentType(typeName);
    }
    return attempts.get();
  }
}
