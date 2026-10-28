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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The hierarchy of a COLUMNAR index METADATA: parsed, persisted and validated against the schema at CREATE INDEX. */
class HierarchyDefinitionTest extends TestHelper {
  private static final String HIERARCHY = "{ \"hierarchy\": { \"vertexType\": \"Tag\", \"key\": \"path\", \"parentEdge\": \"ParentOf\" } }";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Tag");
    database.command("sql", "CREATE PROPERTY Tag.path STRING");
    database.command("sql", "CREATE EDGE TYPE ParentOf");
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    database.command("sql", "CREATE PROPERTY Asset.tags LIST OF STRING");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE PROPERTY Asset.fileSize LONG");
    database.command("sql", "CREATE DOCUMENT TYPE Folder");
  }

  @Test
  void createIndexStoresHierarchyAndSurvivesReopen() {
    database.command("sql", "CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR METADATA " + HIERARCHY);
    assertThat(metadataOf("Asset[tagsbyitem]_columnar").getHierarchyDefinition())
        .isEqualTo(new HierarchyDefinition("Tag", "path", "ParentOf", Vertex.DIRECTION.IN));

    reopenDatabase();

    assertThat(metadataOf("Asset[tagsbyitem]_columnar").getHierarchyDefinition())
        .isEqualTo(new HierarchyDefinition("Tag", "path", "ParentOf", Vertex.DIRECTION.IN));
  }

  @Test
  void stringPropertyAndOutDirectionAreAccepted() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA "
        + "{ \"hierarchy\": { \"vertexType\": \"Tag\", \"key\": \"path\", \"parentEdge\": \"ParentOf\", \"parentDirection\": \"OUT\" } }");

    assertThat(metadataOf("Asset[brand]_columnar").getHierarchyDefinition().parentDirection()).isEqualTo(Vertex.DIRECTION.OUT);
  }

  @Test
  void absentHierarchyGivesNullDefinition() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    assertThat(metadataOf("Asset[brand]_columnar").getHierarchyDefinition()).isNull();
    assertThat(HierarchyDefinition.fromJSON(null)).isNull();
  }

  @Test
  void toJsonWritesTheDirection() {
    final JSONObject json = new HierarchyDefinition("Tag", "path", "ParentOf", Vertex.DIRECTION.OUT).toJSON();

    assertThat(json.getString("parentDirection")).isEqualTo("OUT");
    assertThat(HierarchyDefinition.fromJSON(json)).isEqualTo(new HierarchyDefinition("Tag", "path", "ParentOf", Vertex.DIRECTION.OUT));
  }

  @Test
  void rejectsUnknownVertexType() {
    assertRefused("Asset (tags BY ITEM)", "Nope", "path", "ParentOf", null, "hierarchy.vertexType 'Nope' is not a vertex type");
  }

  @Test
  void rejectsDocumentTypeAsVertexType() {
    assertRefused("Asset (tags BY ITEM)", "Folder", "path", "ParentOf", null, "hierarchy.vertexType 'Folder' is not a vertex type");
  }

  @Test
  void rejectsUnknownKeyProperty() {
    assertRefused("Asset (tags BY ITEM)", "Tag", "nope", "ParentOf", null, "hierarchy.key 'nope' is not a property of Tag");
  }

  @Test
  void rejectsNonEdgeParentEdge() {
    assertRefused("Asset (tags BY ITEM)", "Tag", "path", "Tag", null, "hierarchy.parentEdge 'Tag' is not an edge type");
  }

  @Test
  void rejectsNumericIndexedProperty() {
    assertRefused("Asset (fileSize)", "Tag", "path", "ParentOf", null, "hierarchy requires a STRING or LIST OF STRING property");
  }

  @Test
  void rejectsBadDirection() {
    assertRefused("Asset (tags BY ITEM)", "Tag", "path", "ParentOf", "BOTH", "hierarchy.parentDirection must be IN or OUT");
  }

  @Test
  void rejectsUnidirectionalEdgeWithInDirection() {
    database.command("sql", "CREATE EDGE TYPE UniParent UNIDIRECTIONAL");

    assertRefused("Asset (tags BY ITEM)", "Tag", "path", "UniParent", null,
        "hierarchy.parentEdge 'UniParent' is unidirectional: use parentDirection OUT with child-to-parent edges");
    database.command("sql", "CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR METADATA "
        + "{ \"hierarchy\": { \"vertexType\": \"Tag\", \"key\": \"path\", \"parentEdge\": \"UniParent\", \"parentDirection\": \"OUT\" } }");
    assertThat(metadataOf("Asset[tagsbyitem]_columnar").getHierarchyDefinition().parentDirection()).isEqualTo(Vertex.DIRECTION.OUT);
  }

  private void assertRefused(final String target, final String vertexType, final String key, final String edge, final String direction,
      final String message) {
    final String json = "{ \"hierarchy\": { \"vertexType\": \"" + vertexType + "\", \"key\": \"" + key + "\", \"parentEdge\": \"" + edge + "\""
        + (direction == null ? "" : ", \"parentDirection\": \"" + direction + "\"") + " } }";
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON " + target + " COLUMNAR METADATA " + json))
        .hasStackTraceContaining(message);
    assertThat(database.getSchema().getIndexes()).noneMatch(i -> i.getName().startsWith("Asset[") && i.getName().endsWith("_columnar"));
  }

  private ColumnarIndexMetadata metadataOf(final String indexName) {
    final Index index = database.getSchema().getIndexByName(indexName);
    assertThat(index).isInstanceOf(TypeIndex.class);
    return ((ColumnarIndex) ((TypeIndex) index).getSubIndexes().getFirst()).getColumnarMetadata();
  }
}
