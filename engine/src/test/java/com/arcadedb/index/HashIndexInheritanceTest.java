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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test that creating an index on a subtype property that is already indexed via a supertype
 * correctly raises an error, since inheritance applies to indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HashIndexInheritanceTest extends TestHelper {

  @Test
  void duplicateIndexOnInheritedPropertyShouldFail() {
    database.transaction(() -> {
      final VertexType typeA = database.getSchema().buildVertexType().withName("A").create();
      typeA.createProperty("id", String.class);
      typeA.createTypeIndex(Schema.INDEX_TYPE.HASH, true, "id");

      final VertexType typeB = database.getSchema().buildVertexType().withName("B").withSuperType("A").create();

      assertThatThrownBy(() -> typeB.createTypeIndex(Schema.INDEX_TYPE.HASH, true, "id"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("existent index");
    });
  }

  @Test
  void duplicateIndexOnDeepInheritanceChainShouldFail() {
    database.transaction(() -> {
      // A -> B -> C -> D -> E (5 levels)
      final VertexType typeA = database.getSchema().buildVertexType().withName("A").create();
      typeA.createProperty("id", String.class);
      typeA.createTypeIndex(Schema.INDEX_TYPE.HASH, true, "id");

      final VertexType typeB = database.getSchema().buildVertexType().withName("B").withSuperType("A").create();
      final VertexType typeC = database.getSchema().buildVertexType().withName("C").withSuperType("B").create();
      final VertexType typeD = database.getSchema().buildVertexType().withName("D").withSuperType("C").create();
      final VertexType typeE = database.getSchema().buildVertexType().withName("E").withSuperType("D").create();

      // Every subtype should fail when trying to create the same index
      for (final VertexType subType : new VertexType[] { typeB, typeC, typeD, typeE })
        assertThatThrownBy(() -> subType.createTypeIndex(Schema.INDEX_TYPE.HASH, true, "id"))
            .as("Creating index on %s.id should fail because A.id is already indexed", subType.getName())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("existent index");
    });
  }
}
