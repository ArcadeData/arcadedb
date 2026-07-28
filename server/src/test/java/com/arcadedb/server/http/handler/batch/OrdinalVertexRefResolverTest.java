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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Referencing vertices by their position in the payload is what lets a bulk load of millions of vertices keep its
 * mapping in two primitive arrays instead of a hash map of ids (issue #5470). It must resolve exactly, and reject
 * anything it cannot resolve rather than silently pointing an edge at the wrong vertex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OrdinalVertexRefResolverTest {

  @Test
  void resolvesByPositionInBothAcceptedForms() {
    final OrdinalVertexRefResolver resolver = new OrdinalVertexRefResolver(0, 0);

    resolver.put(null, 0, new RID(3, 100));
    resolver.put(null, 1, new RID(4, 200));

    assertThat(resolver.size()).isEqualTo(2);
    assertThat(resolver.isEmpty()).isFalse();
    assertThat(resolver.get("0", 1)).isEqualTo(new RID(3, 100));
    assertThat(resolver.get("1", 1)).isEqualTo(new RID(4, 200));
    // The form RemoteGraphBatch generates.
    assertThat(resolver.get("v0", 1)).isEqualTo(new RID(3, 100));
    assertThat(resolver.get("V1", 1)).isEqualTo(new RID(4, 200));
    assertThat(new OrdinalVertexRefResolver(0, 0).isEmpty()).isTrue();
  }

  @Test
  void growsBeyondTheHintedCapacity() {
    final OrdinalVertexRefResolver resolver = new OrdinalVertexRefResolver(4, 0);

    for (int i = 0; i < 100_000; i++)
      resolver.put(null, i, new RID(i % 8, i));

    assertThat(resolver.size()).isEqualTo(100_000);
    for (int i = 0; i < 100_000; i += 997)
      assertThat(resolver.get(Integer.toString(i), 1)).isEqualTo(new RID(i % 8, i));
  }

  @Test
  void rejectsAReferenceThatIsNotAPosition() {
    final OrdinalVertexRefResolver resolver = new OrdinalVertexRefResolver(0, 0);
    resolver.put(null, 0, new RID(1, 1));

    assertThatThrownBy(() -> resolver.get("__dk/address/abc", 42)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at line 42").hasMessageContaining("refMode=ordinal");
    assertThatThrownBy(() -> resolver.get("v", 42)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> resolver.get("", 42)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> resolver.get("1x", 42)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsAReferenceToAVertexThatHasNotBeenLoadedYet() {
    final OrdinalVertexRefResolver resolver = new OrdinalVertexRefResolver(0, 0);
    resolver.put(null, 0, new RID(1, 1));

    assertThatThrownBy(() -> resolver.get("7", 9)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("only 1 vertices were loaded");
  }

  /**
   * A payload numbered differently from the order it is streamed in would resolve edges to the wrong vertices, so it
   * is refused on the vertex that breaks it - with the line number the client needs to fix its generator.
   */
  @Test
  void rejectsAVertexNumberedOutOfOrder() {
    final OrdinalVertexRefResolver resolver = new OrdinalVertexRefResolver(0, 0);

    resolver.checkVertexId(null, 0, 1);     // no @id at all is the normal case
    resolver.checkVertexId("0", 0, 1);
    resolver.checkVertexId("v1", 1, 2);

    assertThatThrownBy(() -> resolver.checkVertexId("7", 2, 3)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at line 3").hasMessageContaining("expected '2'");
    assertThatThrownBy(() -> resolver.checkVertexId("__dk/address/abc", 2, 3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void iteratesByPosition() {
    final OrdinalVertexRefResolver resolver = new OrdinalVertexRefResolver(0, 0);
    resolver.put(null, 0, new RID(1, 10));
    resolver.put(null, 1, new RID(2, 20));

    final Map<String, RID> seen = new LinkedHashMap<>();
    resolver.forEach(seen::put);

    assertThat(seen).containsExactly(Map.entry("0", new RID(1, 10)), Map.entry("1", new RID(2, 20)));
  }

  /**
   * A client that splits one load into several requests keeps a single counter across all of them, so the payload of
   * the second request does not start at 0. Everything below the base belongs to an earlier request and is the
   * client's job to reference by RID.
   */
  @Test
  void numbersThePayloadFromTheDeclaredBase() {
    final OrdinalVertexRefResolver resolver = new OrdinalVertexRefResolver(0, 50_000);

    resolver.checkVertexId("v50000", 0, 1);
    resolver.put("v50000", 0, new RID(3, 100));
    resolver.put("v50001", 1, new RID(3, 101));

    assertThat(resolver.get("50000", 1)).isEqualTo(new RID(3, 100));
    assertThat(resolver.get("v50001", 1)).isEqualTo(new RID(3, 101));

    assertThatThrownBy(() -> resolver.get("49999", 7)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("earlier request").hasMessageContaining("50000");
    assertThatThrownBy(() -> resolver.get("50002", 7)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("only 2 vertices");
    assertThatThrownBy(() -> resolver.checkVertexId("0", 0, 3)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected '50000'");

    final Map<String, RID> seen = new LinkedHashMap<>();
    resolver.forEach(seen::put);
    assertThat(seen).containsExactly(Map.entry("50000", new RID(3, 100)), Map.entry("50001", new RID(3, 101)));
  }

  /**
   * The reason this resolver exists: no id is stored, so a vertex costs one int plus one long instead of the ~87
   * bytes an arbitrary temporary id needs.
   */
  @Test
  void costsTwelveBytesPerVertex() {
    final OrdinalVertexRefResolver resolver = new OrdinalVertexRefResolver(1_000_000, 0);

    for (int i = 0; i < 1_000_000; i++)
      resolver.put(null, i, new RID(1, i));

    assertThat(resolver.retainedBytes() / resolver.size()).isEqualTo(12);
  }
}
