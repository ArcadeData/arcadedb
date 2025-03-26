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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class LSMTreeIndexCompositeTest extends TestHelper {
  private static final int TOT = 100;

  @Test
  public void testGetAbsoluteId() {
    database.transaction(() -> {
      final TypeIndex index = database.getSchema().getType("File").getIndexesByProperties("absoluteId").get(0);
      for (int i = 0; i < TOT * TOT; ++i) {
        final IndexCursor value = index.get(new Object[] { i });
        assertThat(value.hasNext()).isTrue();
        assertThat(value.next().asVertex().get("absoluteId")).isEqualTo(i);
      }
    });
  }

  @Test
  public void testGetRelative() {
    database.transaction(() -> {
      final TypeIndex index = database.getSchema().getType("File").getIndexesByProperties("directoryId", "fileId").get(0);
      for (int i = 0; i < TOT; ++i) {
        for (int k = 0; k < TOT; ++k) {
          final IndexCursor value = index.get(new Object[] { i, k });
          assertThat(value.hasNext()).withFailMessage("id[" + i + "," + k + "]").isTrue();

          final Vertex v = value.next().asVertex();
          assertThat(v.get("directoryId")).isEqualTo(i);
          assertThat(v.get("fileId")).isEqualTo(k);
        }
      }
    });
  }

  @Test
  public void testPartialNullGet() {
    database.transaction(() -> {
      final TypeIndex index = database.getSchema().getType("File").getIndexesByProperties("directoryId", "fileId").get(0);
      for (int i = 0; i < TOT; ++i) {
        final IndexCursor value = index.get(new Object[] { i, null });
        assertThat(value.hasNext()).withFailMessage( "id[" + i + "]").isTrue();

        final Vertex v = value.next().asVertex();
        assertThat(v.get("directoryId")).isEqualTo(i);
      }
    });
  }

  protected void beginTest() {
    // CREATE SIMPLE GRAPH OF 2 LEVELS DIRECTORY FILE SYSTEM
    database.transaction(() -> {
      assertThat(database.getSchema().existsType("File")).isFalse();
      final DocumentType file = database.getSchema().createVertexType("File");
      file.createProperty("absoluteId", Integer.class);
      file.createProperty("directoryId", Integer.class);
      file.createProperty("fileId", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "File", "absoluteId");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "File", "directoryId", "fileId");

      file.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy());

      assertThat(database.getSchema().existsType("HasChildren")).isFalse();
      database.getSchema().createEdgeType("HasChildren");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "HasChildren", "@out", "@in");

      int fileId = 0;
      for (int i = 0; i < TOT; ++i) {
        final MutableVertex v = database.newVertex("File");
        v.set("absoluteId", fileId++);
        v.set("directoryId", i);
        v.set("name", UUID.randomUUID().toString());
        v.set("lastUpdated", System.currentTimeMillis());
        v.save();

        for (int k = 0; k < TOT; ++k) {
          final MutableVertex c = database.newVertex("File");
          c.set("absoluteId", fileId++);
          c.set("directoryId", i);
          c.set("fileId", k);
          c.set("name", UUID.randomUUID().toString());
          c.set("lastUpdated", System.currentTimeMillis());
          c.save();

          v.newEdge("HasChildren", c);
        }
      }
    });
  }
}
