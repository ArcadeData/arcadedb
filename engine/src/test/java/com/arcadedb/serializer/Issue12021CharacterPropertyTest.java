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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12021: a {@link Character} value passed {@code Type.validateValue} (it maps to STRING) and was then skipped by the
 * serializer, so the property was missing after the commit, and a {@link Character} inside a list or a map was stored as null.
 * It is stored as the one-character STRING the type mapping names.
 */
class Issue12021CharacterPropertyTest extends TestHelper {

  private Document writeAndReload(final String typeName, final String property, final Object value) {
    if (!database.getSchema().existsType(typeName))
      database.getSchema().createDocumentType(typeName);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set(property, value).set("k", 1).save().getIdentity());
    return database.lookupByRID(rid[0], true).asDocument();
  }

  @Test
  void undeclaredCharacterPropertyIsStoredAsString() {
    final Document doc = writeAndReload("Chr", "c", 'x');
    assertThat(doc.getPropertyNames()).contains("c", "k");
    assertThat(doc.get("c")).isEqualTo("x");
    assertThat(database.query("sql", "SELECT FROM Chr WHERE c = 'x'").stream().count()).isEqualTo(1L);
  }

  @Test
  void declaredStringPropertyKeepsACharacter() {
    database.getSchema().createDocumentType("ChrDeclared").createProperty("c", Type.STRING);
    assertThat(writeAndReload("ChrDeclared", "c", 'y').get("c")).isEqualTo("y");
  }

  @Test
  void characterInsideACollectionIsNotStoredAsNull() {
    final List<Object> list = new ArrayList<>();
    list.add('a');
    list.add("b");
    assertThat(writeAndReload("ChrList", "l", list).get("l")).isEqualTo(List.of("a", "b"));

    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("k", 'v');
    assertThat(writeAndReload("ChrMap", "m", map).get("m")).isEqualTo(Map.of("k", "v"));

    // A SET IS STORED AS A LIST
    assertThat(writeAndReload("ChrSet", "s", Set.of('z')).get("s")).isEqualTo(List.of("z"));
  }

  @Test
  void vertexCharacterPropertySurvivesTheCommit() {
    database.getSchema().createVertexType("ChrV");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex("ChrV").set("c", 'q').save().getIdentity());
    assertThat(database.lookupByRID(rid[0], true).asVertex().get("c")).isEqualTo("q");
  }
}
