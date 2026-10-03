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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #9019 (the bulk edge path skipped the declared property's conversion, constraints and defaults) and
 * #9020 (a {@link Map} holding the properties was dropped by the bulk edge path).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9019GraphBatchEdgeSchemaTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("N9019");
      database.getSchema().createEdgeType("F9019");
      database.getSchema().createEdgeType("E9020");
    });
  }

  private RID[] vertices() {
    final RID[] v = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < v.length; i++)
        v[i] = database.newVertex("N9019").save().getIdentity();
    });
    return v;
  }

  private void declare(final String type, final String declaration) {
    database.command("sql", "CREATE EDGE TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".p " + declaration);
  }

  private Edge edgeOf(final RID from, final String type) {
    return database.lookupByRID(from, true).asVertex().getEdges(Vertex.DIRECTION.OUT, type).iterator().next();
  }

  private void batchEdge(final RID[] v, final String type, final boolean mixed, final Object... props) {
    try (final GraphBatch b = database.batch().build()) {
      b.newEdge(v[0], type, v[1], props);
      if (mixed)
        b.newEdge(v[0], "F9019", v[1]);
    }
  }

  @Test
  void declaredPropertyIsConvertedAndDefaulted() {
    declare("E_INT9019", "INTEGER");
    declare("E_BYTE9019", "BYTE");
    declare("E_DFLT9019", "STRING (default 'dflt')");
    declare("E_MICROS9019", "DATETIME_MICROS");

    for (final boolean mixed : new boolean[] { false, true }) {
      RID[] v = vertices();
      batchEdge(v, "E_BYTE9019", mixed, "p", 12);
      assertThat(edgeOf(v[0], "E_BYTE9019").get("p")).isEqualTo((byte) 12);

      v = vertices();
      batchEdge(v, "E_INT9019", mixed, "p", "42");
      assertThat(edgeOf(v[0], "E_INT9019").get("p")).isEqualTo(42);

      v = vertices();
      batchEdge(v, "E_DFLT9019", mixed, "other", 1);
      assertThat(edgeOf(v[0], "E_DFLT9019").get("p")).isEqualTo("dflt");

      // the same value must come back as Vertex.newEdge() stores it
      final RID[] api = vertices();
      database.transaction(() -> api[0].asVertex().modify().newEdge("E_MICROS9019", api[1], "p", 1791000000000L));
      v = vertices();
      batchEdge(v, "E_MICROS9019", mixed, "p", 1791000000000L);
      assertThat(edgeOf(v[0], "E_MICROS9019").get("p")).isEqualTo(edgeOf(api[0], "E_MICROS9019").get("p"));
    }
  }

  @Test
  void valuesOutOfRangeAndBrokenConstraintsAreRefused() {
    declare("E_SHORT9019", "SHORT");
    declare("E_LONGINT9019", "INTEGER");
    declare("E_MANDATORY9019", "STRING (mandatory true)");
    declare("E_MIN9019", "INTEGER (min 10)");
    declare("E_REGEXP9019", "STRING (regexp '[a-z]+')");

    for (final boolean mixed : new boolean[] { false, true }) {
      assertRefused("E_SHORT9019", mixed, "p", 40000);
      assertRefused("E_LONGINT9019", mixed, "p", 3000000000L);
      assertRefused("E_MANDATORY9019", mixed, "other", 1);
      assertRefused("E_MIN9019", mixed, "p", 5);
      assertRefused("E_REGEXP9019", mixed, "p", "ABC");
    }
  }

  private void assertRefused(final String type, final boolean mixed, final Object... props) {
    final RID[] v = vertices();
    assertThatThrownBy(() -> batchEdge(v, type, mixed, props)).as(type + " mixed=" + mixed).isInstanceOf(RuntimeException.class);
    if (database.isTransactionActive())
      database.rollback();
    assertThat(database.lookupByRID(v[0], true).asVertex().getEdges(Vertex.DIRECTION.OUT, type).iterator().hasNext()).as(type).isFalse();
  }

  @Test
  void mapOfPropertiesIsStoredWhateverElseTheBatchHolds() {
    final Map<String, Object> props = new LinkedHashMap<>();
    props.put("weight", 5);
    props.put("label", "x");

    for (final boolean mixed : new boolean[] { false, true }) {
      final RID[] v = vertices();
      try (final GraphBatch b = database.batch().build()) {
        b.newEdge(v[0], "E9020", v[1], props);
        if (mixed)
          b.newEdge(v[0], "F9019", v[1]);
      }
      final Edge edge = edgeOf(v[0], "E9020");
      assertThat(edge.get("weight")).isEqualTo(5);
      assertThat(edge.get("label")).isEqualTo("x");
    }
  }

  @Test
  void legacyLightEdgeOverrideStillAppliesDefaultsAndMandatory() {
    declare("E_LDFLT9019", "STRING (default 'dflt')");
    declare("E_LMAND9019", "STRING (mandatory true)");

    final RID[] v = vertices();
    try (final GraphBatch b = database.batch().withLightEdges(true).build()) {
      b.newEdge(v[0], "E_LDFLT9019", v[1]);
    }
    assertThat(edgeOf(v[0], "E_LDFLT9019").get("p")).isEqualTo("dflt");

    final RID[] w = vertices();
    assertThatThrownBy(() -> {
      try (final GraphBatch b = database.batch().withLightEdges(true).build()) {
        b.newEdge(w[0], "E_LMAND9019", w[1]);
      }
    }).isInstanceOf(RuntimeException.class);
  }

  @Test
  void malformedPropertyArgumentsAreRefused() {
    final RID[] v = vertices();
    try (final GraphBatch b = database.batch().build()) {
      assertThatThrownBy(() -> b.newEdge(v[0], "E9020", v[1], "odd")).isInstanceOf(IllegalArgumentException.class);
      final Map<String, Object> nullKey = new LinkedHashMap<>();
      nullKey.put(null, 1);
      assertThatThrownBy(() -> b.newEdge(v[0], "E9020", v[1], nullKey)).isInstanceOf(IllegalArgumentException.class);
    }
  }
}
