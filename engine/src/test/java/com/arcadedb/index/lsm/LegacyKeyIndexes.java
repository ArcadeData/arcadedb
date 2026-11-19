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
package com.arcadedb.index.lsm;

import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test support for the indexes an older build wrote (issues #12160 and #12233): their schema entry has no
 * {@link LSMTreeIndex#CANONICAL_KEYS} marker, so a load reads them as indexes that {@link LSMTreeIndex#mayHoldLegacyKeys() may hold}
 * keys in an old spelling.
 */
public final class LegacyKeyIndexes {
  private LegacyKeyIndexes() {
  }

  /**
   * Rewrites the schema file of a CLOSED database as a build before #12160 left it: no index entry carries the marker of an index whose
   * keys are all canonical. The next open reads every LSM_TREE index as one that may hold old spellings.
   */
  public static void forgetCanonicalKeys(final String databasePath) throws IOException {
    final File schemaFile = new File(databasePath, LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath()));
    final JSONObject types = root.getJSONObject("types", null);
    int removed = 0;
    if (types != null)
      for (final String typeName : types.keySet()) {
        final JSONObject indexes = types.getJSONObject(typeName).getJSONObject("indexes", null);
        if (indexes != null)
          for (final String indexName : indexes.keySet())
            if (indexes.getJSONObject(indexName).remove(LSMTreeIndex.CANONICAL_KEYS) != null)
              ++removed;
      }
    // the scan reached the entries the marker is written into
    assertThat(removed).as("LSM_TREE schema entries carrying '%s'", LSMTreeIndex.CANONICAL_KEYS).isPositive();
    Files.writeString(schemaFile.toPath(), root.toString());
    // the fallback copy must not stand in for the edited primary
    final File prev = new File(databasePath, LocalSchema.SCHEMA_PREV_FILE_NAME);
    if (prev.exists())
      Files.writeString(prev.toPath(), root.toString());
  }
}
