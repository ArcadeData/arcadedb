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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9495: the {@code copy()} of {@link SelectStatement}, {@link CreateVertexStatement},
 * {@link MatchPathItem}, {@link FieldMatchPathItem} and {@link MathExpression} created the copy with
 * {@code getClass().getConstructor().newInstance()}. A GraalVM native image registers no parser constructor for
 * reflection, so every UPDATE/DELETE (whose plan-cache key copies a synthetic SELECT) failed there with
 * {@code NoSuchMethodException: SelectStatement.<init>()}. The copies now come from an overridable {@code newInstance()}
 * factory, which every subclass must override to keep the runtime type of the copy. That no class of the code base asks for the
 * constructor of its own runtime type is held by {@code ReflectionFreeCopyCensusTest} (census module).
 */
class Issue9495ReflectionFreeCopyTest extends TestHelper {

  private static final String     SQL_PACKAGE   = "com/arcadedb/query/sql";
  private static final Class<?>[] FACTORY_BASES = { SelectStatement.class, CreateVertexStatement.class, MatchPathItem.class,
      MathExpression.class };

  /**
   * The factory replaced {@code getClass()}, so a subclass that inherits {@code copy()} and forgets to override
   * {@code newInstance()} silently copies itself into its base class. Every concrete subclass, current and future, is
   * discovered from the compiled classes of the SQL packages and checked.
   */
  @Test
  void copyKeepsTheRuntimeTypeOfEverySubclass() throws Exception {
    int checked = 0;
    for (final String className : sqlClassNames()) {
      final Class<?> clazz = Class.forName(className);
      if (Modifier.isAbstract(clazz.getModifiers()) || !Modifier.isPublic(clazz.getModifiers()) || !isFactoryBased(clazz))
        continue;
      if (!hasPublicNoArgConstructor(clazz)) {
        // such a subclass cannot inherit the factory-based copy() (newInstance() could not create it): it must override copy()
        assertThat(clazz.getMethod("copy").getDeclaringClass()).as("copy() of " + clazz.getSimpleName()).isEqualTo(clazz);
        continue;
      }

      final Object original = clazz.getConstructor().newInstance();
      final Object copy = clazz.getMethod("copy").invoke(original);
      assertThat(copy).as("copy() of " + clazz.getSimpleName()).isNotSameAs(original);
      assertThat(copy.getClass()).as("copy() of " + clazz.getSimpleName()).isEqualTo(clazz);
      checked++;
    }
    // the four bases plus their subclasses: a discovery that finds nothing would pass for the wrong reason
    assertThat(checked).isGreaterThanOrEqualTo(20);
  }

  @Test
  void copiedStatementsPreserveTheirContent() {
    final SelectStatement select = (SelectStatement) parse("SELECT name FROM V WHERE age > 3 LIMIT 5");
    final SelectStatement selectCopy = select.copy();
    assertThat(selectCopy.getClass()).isEqualTo(SelectStatement.class);
    assertThat(selectCopy.toString()).isEqualTo(select.toString());
    assertThat(selectCopy).isEqualTo(select);

    final SelectStatement noTarget = (SelectStatement) parse("SELECT 1 + 2 AS sum");
    assertThat(noTarget.copy().getClass()).isEqualTo(noTarget.getClass());
    assertThat(noTarget.copy().toString()).isEqualTo(noTarget.toString());

    final Statement createVertex = parse("CREATE VERTEX V SET name = 'a'");
    assertThat(createVertex.copy().getClass()).isEqualTo(createVertex.getClass());
    assertThat(createVertex.copy().toString()).isEqualTo(createVertex.toString());

    final MatchStatement match = (MatchStatement) parse(
        "MATCH {type: V, as: a}.out('E'){as: b}.(in('E')){as: c}, {as: a}.field{as: d}, {as: a}-E->{as: e}<-E-{as: f}-E-{as: g} RETURN a, b");
    assertThat(pathItemTypes((MatchStatement) match.copy())).isEqualTo(pathItemTypes(match));
    assertThat(pathItemTypes(match)).contains(MatchPathItem.class, MultiMatchPathItem.class, FieldMatchPathItem.class);
  }

  /** The end-to-end shape of the report: UPDATE, DELETE and UNION ALL all copy a SELECT on their way to a plan. */
  @Test
  void dmlAndUnionExecuteThroughTheCopiedSource() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE TmpProbe");
      database.command("sql", "INSERT INTO TmpProbe SET id = 1, v = 0");
      database.command("sql", "INSERT INTO TmpProbe SET id = 2, v = 0");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE TmpProbe SET v = 1 WHERE id = 1")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
      try (final ResultSet rs = database.command("sql", "DELETE FROM TmpProbe WHERE id = 2")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });

    try (final ResultSet rs = database.query("sql",
        "SELECT unionAll($a, $b) AS u LET $a = (SELECT v FROM TmpProbe WHERE id = 1), $b = (SELECT v FROM TmpProbe WHERE id = 1)")) {
      assertThat(rs.next().<List<?>>getProperty("u")).hasSize(2);
    }
  }

  private Statement parse(final String sql) {
    return ((DatabaseInternal) database).getStatementCache().get(sql);
  }

  private static List<Class<?>> pathItemTypes(final MatchStatement match) {
    final List<Class<?>> types = new ArrayList<>();
    for (final MatchExpression expression : match.getMatchExpressions())
      for (final MatchPathItem item : expression.getItems())
        types.add(item.getClass());
    return types;
  }

  private static boolean isFactoryBased(final Class<?> clazz) {
    for (final Class<?> base : FACTORY_BASES)
      if (base.isAssignableFrom(clazz))
        return true;
    return false;
  }

  private static boolean hasPublicNoArgConstructor(final Class<?> clazz) {
    try {
      clazz.getConstructor();
      return true;
    } catch (final NoSuchMethodException e) {
      return false;
    }
  }

  /** The top-level classes compiled from the SQL packages, read from the class root that holds {@link SelectStatement}. */
  private static List<String> sqlClassNames() throws IOException, URISyntaxException {
    final Path root = Path.of(SelectStatement.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    final Path sql = root.resolve(SQL_PACKAGE);
    assertThat(sql).as("the compiled SQL classes").isDirectory();
    try (final Stream<Path> files = Files.walk(sql)) {
      return files.map(p -> root.relativize(p).toString().replace('\\', '/')).filter(p -> p.endsWith(".class") && !p.contains("$") && !p.endsWith("package-info.class"))
          .map(p -> p.substring(0, p.length() - ".class".length()).replace('/', '.')).sorted().toList();
    }
  }
}
