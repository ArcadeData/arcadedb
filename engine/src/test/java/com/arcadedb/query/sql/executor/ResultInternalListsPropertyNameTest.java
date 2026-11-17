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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ResultInternal#listsPropertyName(String)} is {@code getPropertyNames().contains(name)} without the set. The oracle is
 * that expression, asked of every shape of row a query makes, for names the row has, names it lacks and names it hides.
 */
class ResultInternalListsPropertyNameTest extends TestHelper {

  private static final List<String> PROBES = List.of("a", "b", "nullValue", "x", "y", "removed", "hidden", "missing", "");

  @Test
  void aProjectedRowAnswersFromItsColumns() {
    final ResultInternal row = new ResultInternal();
    row.setProperty("a", 1);
    row.setProperty("b", "text");
    row.setProperty("nullValue", null);
    assertAgreesWithTheSet(row);
    assertThat(row.listsPropertyName("a")).isTrue();
    assertThat(row.listsPropertyName("nullValue")).as("a column holding null is still a column").isTrue();
    assertThat(row.listsPropertyName("x")).isFalse();
  }

  @Test
  void aRowBackedByAnElementListsTheElementsProperties() {
    database.getSchema().createVertexType("V");
    database.transaction(() -> database.newVertex("V").set("x", 1).set("y", 2).save());
    final MutableVertex vertex = database.query("sql", "SELECT FROM V").next().getVertex().get().modify();
    final ResultInternal row = new ResultInternal(vertex);
    assertAgreesWithTheSet(row);
    assertThat(row.listsPropertyName("x")).isTrue();
    assertThat(row.listsPropertyName("missing")).isFalse();
  }

  @Test
  void anElementRowWithContentListsTheContentOnly() {
    // the element's own properties are not columns once the row carries content (issue #5613)
    database.getSchema().createVertexType("V");
    database.transaction(() -> database.newVertex("V").set("x", 1).save());
    final ResultInternal row = new ResultInternal();
    row.setProperty("a", 1);
    row.setElement(database.query("sql", "SELECT FROM V").next().getVertex().get());
    assertAgreesWithTheSet(row);
    assertThat(row.listsPropertyName("a")).isTrue();
    assertThat(row.listsPropertyName("x")).isFalse();
  }

  @Test
  void aRemovedPropertyAndAnExcludedColumnAreNotListed() {
    database.getSchema().createVertexType("V");
    database.transaction(() -> database.newVertex("V").set("x", 1).set("removed", 2).set("hidden", 3).save());
    final ResultInternal row = new ResultInternal(database.query("sql", "SELECT FROM V").next().getVertex().get());
    row.removeProperty("removed");
    row.setProjectionExcludes(Set.of("hidden"));
    assertAgreesWithTheSet(row);
    assertThat(row.listsPropertyName("x")).isTrue();
    assertThat(row.listsPropertyName("removed")).isFalse();
    assertThat(row.listsPropertyName("hidden")).isFalse();
  }

  private static void assertAgreesWithTheSet(final ResultInternal row) {
    for (final String name : PROBES)
      assertThat(row.listsPropertyName(name)).as("name '%s'", name).isEqualTo(row.getPropertyNames().contains(name));
  }
}
