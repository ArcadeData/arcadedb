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
package com.arcadedb.server.http.handler.batch;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11801: a temporary id declared by two vertices of one payload is refused at its second declaration, whether the first
 * vertex is still waiting in the chunk or already created, and an id declared but not created yet is never handed out.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TempIdVertexRefResolverTest {

  @Test
  void aSecondDeclarationInTheSameChunkNamesTheLineOfTheFirst() {
    final TempIdVertexRefResolver resolver = new TempIdVertexRefResolver(0);
    resolver.checkVertexId("a", 0, 1);
    assertThatThrownBy(() -> resolver.checkVertexId("a", 1, 2)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Vertex at line 2 declares @id 'a'").hasMessageContaining("the vertex at line 1");
  }

  @Test
  void aSecondDeclarationAfterTheFlushNamesTheVertexCreated() {
    final TempIdVertexRefResolver resolver = new TempIdVertexRefResolver(0);
    resolver.checkVertexId("a", 0, 1);
    resolver.put("a", 0, new RID(3, 7));
    assertThatThrownBy(() -> resolver.checkVertexId("a", 1, 2)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("created as #3:7");
  }

  @Test
  void verticesWithoutAnIdAreCountedAndNeverCollide() {
    final TempIdVertexRefResolver resolver = new TempIdVertexRefResolver(0);
    resolver.checkVertexId(null, 0, 1);
    resolver.checkVertexId(null, 1, 2);
    assertThat(resolver.unreferenceableVertices()).isEqualTo(2);
    assertThat(resolver.size()).isZero();
  }

  @Test
  void aDeclaredIdIsNeitherResolvedNorMappedBeforeItsVertexIsCreated() {
    final TempIdVertexRefResolver resolver = new TempIdVertexRefResolver(0);
    resolver.checkVertexId("a", 0, 1);
    resolver.checkVertexId("b", 1, 2);
    resolver.put("b", 1, new RID(1, 1));

    assertThatThrownBy(() -> resolver.get("a", 5)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown temporary ID 'a'");
    assertThat(resolver.get("b", 5)).isEqualTo(new RID(1, 1));

    final Map<String, RID> mapped = new HashMap<>();
    resolver.forEach(mapped::put);
    assertThat(mapped).containsOnly(Map.entry("b", new RID(1, 1)));
  }
}
