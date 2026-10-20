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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10415: {@code GAVFusedChainOperator} executes as a copy of itself bound to the snapshot the statement pinned
 * ({@code readingFrom}), since the plan is cached and shared by concurrent executions. The copy is written field by field, so a
 * field added to the operator and left out of it would be silently dropped on every execution: this fills every field with a
 * value of its own and checks the copy carries each of them, the view excepted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GAVFusedChainOperatorReadingFromTest {

  @Test
  void theCopyAnExecutionRunsCarriesEveryField() throws Exception {
    final GraphTraversalProvider plannedView = new GraphAnalyticalView(null);
    final GraphTraversalProvider pinnedView = new GraphAnalyticalView(null);
    final GAVFusedChainOperator operator = new GAVFusedChainOperator(new RowsOperator(List.of()), plannedView, "a",
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT }, new String[][] { { "K" } },
        new String[] { "b" }, new int[][] { { 1 } }, new boolean[] { true, true }, 1.5, 7L);

    final List<Field> fields = instanceFields();
    for (final Field field : fields)
      if (!Modifier.isFinal(field.getModifiers()))
        field.set(operator, valueFor(field));

    final Method readingFrom = GAVFusedChainOperator.class.getDeclaredMethod("readingFrom", GraphTraversalProvider.class);
    readingFrom.setAccessible(true);
    final GAVFusedChainOperator copy = (GAVFusedChainOperator) readingFrom.invoke(operator, pinnedView);

    final List<String> lost = new ArrayList<>();
    for (final Field field : fields) {
      final Object original = field.get(operator);
      final Object copied = field.get(copy);
      if (field.getName().equals("provider")) {
        assertThat(copied).as("the copy reads the pinned view").isSameAs(pinnedView);
        continue;
      }
      assertThat(original).as("the test filled " + field.getName()).isNotNull();
      if (field.getType().isPrimitive() ? !original.equals(copied) : original != copied)
        lost.add(field.getName());
    }
    assertThat(lost).as("fields readingFrom() does not carry to the copy an execution runs: add them there").isEmpty();
  }

  private static List<Field> instanceFields() {
    final List<Field> fields = new ArrayList<>();
    for (Class<?> type = GAVFusedChainOperator.class; type != Object.class; type = type.getSuperclass())
      for (final Field field : type.getDeclaredFields())
        if (!Modifier.isStatic(field.getModifiers())) {
          field.setAccessible(true);
          fields.add(field);
        }
    return fields;
  }

  /** A value of the field's own type that no other field holds, so a field left out of the copy cannot pass by coincidence. */
  private static Object valueFor(final Field field) {
    final Class<?> type = field.getType();
    if (type == String.class)
      return field.getName();
    if (type.isArray())
      return Array.newInstance(type.getComponentType(), 1);
    if (type == List.class)
      return new ArrayList<>();
    if (type == BooleanExpression.class)
      return new BooleanExpression() {
        @Override
        public boolean evaluate(final Result result, final CommandContext context) {
          return true;
        }

        @Override
        public String getText() {
          return field.getName();
        }
      };
    if (type == PhysicalOperator.class)
      return new RowsOperator(List.of());
    throw new AssertionError("The test cannot fill field '" + field.getName() + "' of type " + type.getName()
        + ": teach valueFor() its type");
  }
}
