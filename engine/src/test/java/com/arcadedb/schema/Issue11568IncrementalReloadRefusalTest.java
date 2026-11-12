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

import com.arcadedb.TestHelper;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11568, incremental reload: a refused definition of an index whose file the reload re-instantiates (a touched file id)
 * leaves its name published until the publication barrier, and a reload that aborts after the refusal leaves it published for
 * good. Withdrawing the name while the load was still staging made the previous generation, which readers keep using until the
 * barrier, lose the index, and an aborted load never put it back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11568IncrementalReloadRefusalTest extends TestHelper {
  private static final String VECTOR_INDEX = "Vec[vector]";

  @Override
  protected void beginTest() {
    database.command("sqlscript", """
        CREATE DOCUMENT TYPE Vec BUCKETS 1;
        CREATE PROPERTY Vec.vector ARRAY_OF_FLOATS;
        CREATE INDEX ON Vec (vector) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE', quantization: 'PRODUCT', pqClusters: 16};
        CREATE DOCUMENT TYPE Other BUCKETS 1;""");
    database.transaction(() -> database.newDocument("Vec").set("vector", new float[] { 1, 0, 0, 0 }).save());
    reopenDatabase();
  }

  @Test
  void aTouchedIndexWhoseDefinitionIsRefusedStaysPublishedUntilThePublication() throws IOException {
    final LocalSchema schema = database.getSchema().getEmbedded();
    final TypeIndex typeIndex = (TypeIndex) schema.getIndexByName(VECTOR_INDEX);
    final IndexInternal bucketIndex = typeIndex.getIndexesOnBuckets()[0];
    final int vectorFileId = bucketIndex.getFileId();
    final String valid = readSchemaFile();

    // the reload re-instantiates the index file and refuses its definition, then fails on a section it reads after the indexes
    final JSONObject broken = new JSONObject(valid.replace("\"pqClusters\":16", "\"pqClusters\":512"));
    broken.put("triggers", "not an object");
    writeSchemaFile(broken.toString());
    assertThatThrownBy(() -> schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(vectorFileId)))
        .as("the fixture must actually break this load").isInstanceOf(SchemaException.class);
    assertThat(schema.getIndexByName(VECTOR_INDEX)).as("an aborted load leaves the previous generation's index published")
        .isSameAs(typeIndex);
    assertThat(schema.getIndexByName(bucketIndex.getName())).isSameAs(bucketIndex);

    // the same reload, published: the refused index goes, with its type index
    writeSchemaFile(valid.replace("\"pqClusters\":16", "\"pqClusters\":512"));
    assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(vectorFileId))).isTrue();
    assertThat(schema.existsIndex(VECTOR_INDEX)).isFalse();
    assertThat(schema.existsIndex(bucketIndex.getName())).isFalse();
    assertThat(schema.existsType("Other")).isTrue();
  }

  private String readSchemaFile() throws IOException {
    return Files.readString(new File(getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8);
  }

  private void writeSchemaFile(final String content) throws IOException {
    Files.writeString(new File(getDatabasePath(), "schema.json").toPath(), content, StandardCharsets.UTF_8);
  }
}
