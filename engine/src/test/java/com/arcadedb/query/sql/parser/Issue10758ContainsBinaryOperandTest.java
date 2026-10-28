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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10758: {@code CONTAINSALL} expanded a {@code byte[]} right-hand operand into its bytes, so a list holding a BINARY value did not
 * contain all of it, where {@code CONTAINS} and {@code CONTAINSANY} found it. The right-hand side of a containment operator is the
 * value searched FOR, and a {@code byte[]} is ArcadeDB's BINARY scalar: one value, never a sequence.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10758ContainsBinaryOperandTest extends TestHelper {

  private static final byte[] BINARY = { 1, 2 };

  @Test
  void everyContainmentOperatorFindsABinaryValueInAList() {
    database.command("sql", "CREATE DOCUMENT TYPE Blob").close();
    database.transaction(() -> database.newDocument("Blob").set("vals", new ArrayList<>(List.of(new byte[] { 1, 2 }))).save());

    for (final String operator : new String[] { "CONTAINS", "CONTAINSANY", "CONTAINSALL" }) {
      assertThat(count("SELECT count(*) AS n FROM Blob WHERE vals " + operator + " :b", Map.of("b", BINARY))).as(operator)
          .isEqualTo(1L);
      assertThat(count("SELECT count(*) AS n FROM Blob WHERE vals " + operator + " :b", Map.of("b", List.of(BINARY)))).as(
          operator + " [binary]").isEqualTo(1L);
      // the bytes of the value are no elements of the list
      assertThat(count("SELECT count(*) AS n FROM Blob WHERE vals " + operator + " :b", Map.of("b", List.of((byte) 1, (byte) 2))))
          .as(operator + " [bytes]").isEqualTo(0L);
    }
  }

  /**
   * The census: every containment condition of the parser package with a value form ({@code execute(Object, Object)}) reads a
   * {@code byte[]} operand, alone or as the one element of a list, as {@code CONTAINS} reads it. A condition added later is in the
   * census without anyone listing it.
   */
  @Test
  void everyContainmentConditionReadsAByteArrayOperandAsOneValue() throws Exception {
    final List<Class<?>> conditions = containmentConditions();
    assertThat(conditions).extracting(Class::getSimpleName)
        .contains("ContainsCondition", "ContainsAnyCondition", "ContainsAllCondition");

    final ContainsCondition reference = new ContainsCondition();
    final List<Object> container = List.of(new byte[] { 1, 2 });
    final List<Object> containerOfBytes = List.of((byte) 1, (byte) 2);

    for (final Class<?> type : conditions) {
      final Object condition = type.getDeclaredConstructor().newInstance();
      final Method execute = type.getMethod("execute", Object.class, Object.class);

      for (final Object operand : new Object[] { BINARY, List.of(BINARY) }) {
        assertThat(execute.invoke(condition, container, operand)).as(type.getSimpleName() + " over [binary]")
            .isEqualTo(reference.execute(container, operand)).isEqualTo(true);
        assertThat(execute.invoke(condition, containerOfBytes, operand)).as(type.getSimpleName() + " over [bytes]")
            .isEqualTo(reference.execute(containerOfBytes, operand)).isEqualTo(false);
      }
    }
  }

  private static List<Class<?>> containmentConditions() throws Exception {
    // the directory the parser classes were loaded from, not the first one of the package on the class path: the test classes of
    // this very package come first
    final URL root = ContainsCondition.class.getProtectionDomain().getCodeSource().getLocation();
    final File packageDir = new File(new File(root.toURI()), ContainsCondition.class.getPackageName().replace('.', File.separatorChar));
    final File[] files = packageDir.listFiles((dir, name) -> name.startsWith("Contains") && name.endsWith("Condition.class"));
    assertThat(files).isNotNull();

    final List<Class<?>> conditions = new ArrayList<>();
    for (final File file : files) {
      final String name = file.getName();
      final Class<?> type = Class.forName(
          ContainsCondition.class.getPackageName() + "." + name.substring(0, name.length() - ".class".length()));
      try {
        type.getMethod("execute", Object.class, Object.class);
        conditions.add(type);
      } catch (final NoSuchMethodException e) {
        // no value form: a condition over a text or a map, not over a sequence
      }
    }
    return conditions;
  }

  private long count(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
