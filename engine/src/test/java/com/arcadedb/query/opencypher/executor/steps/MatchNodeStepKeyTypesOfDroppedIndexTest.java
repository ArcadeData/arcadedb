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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The index seek of an unlabelled node lists the indexes of every type and then reads the types of their keys: a
 * concurrent DDL that drops an index in between must make the index answer nothing, so another index or the walk
 * answers the pattern, instead of failing the statement (issue #12522).
 */
class MatchNodeStepKeyTypesOfDroppedIndexTest extends TestHelper {

  @Test
  void anIndexDroppedAfterItWasListedHasNoKeyTypes() {
    final TypeIndex[] index = new TypeIndex[1];
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Entity");
      type.createProperty("uuid", Type.STRING);
      index[0] = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
    });

    assertThat(MatchNodeStep.keyTypesOf(index[0])).containsExactly(Type.STRING);

    database.getSchema().dropIndex(index[0].getName());

    assertThatThrownBy(() -> index[0].getKeyTypes())
        .as("the dropped index refuses to say what its key holds")
        .isInstanceOf(IndexException.class);
    assertThat(MatchNodeStep.keyTypesOf(index[0]))
        .as("a dropped index is not a candidate for the seek")
        .isNull();
  }
}
