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
package com.arcadedb.postgres;

import com.arcadedb.query.sql.parser.CreateDocumentTypeStatement;
import com.arcadedb.query.sql.parser.DDLStatement;
import com.arcadedb.query.sql.parser.InsertStatement;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.UpdateStatement;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #11312 and #11313: the CommandComplete tag of a parsed statement comes from its class, every schema change is answered
 * with no result set, and the text of an unparsed statement is read past its comments. The census ranges over every concrete
 * {@link Statement} class the engine ships, read from the engine's class root, so a statement class added later is held to the
 * same rules without an entry here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCommandTagTest {

  @Test
  void everyStatementClassHasATagAndEverySchemaChangeIsRowless() throws Exception {
    final List<Class<? extends Statement>> statements = concreteStatementClasses();
    // the scan still reaches the authority: the engine's own statement classes, the schema changes among them
    assertThat(statements).as("the scan of the engine's class root no longer reaches Statement's subclasses")
        .contains(SelectStatement.class, InsertStatement.class, CreateDocumentTypeStatement.class);
    assertThat(statements.stream().filter(DDLStatement.class::isAssignableFrom).count())
        .as("the scan of the engine's class root no longer reaches the DDLStatement subclasses").isGreaterThan(25);

    for (final Class<? extends Statement> type : statements) {
      final Statement statement = instantiate(type);
      final String tag = PostgresCommandTag.ofStatement(statement, 3);
      assertThat(tag).as("%s has no command tag", type.getSimpleName()).isNotBlank();
      if (statement instanceof DDLStatement) {
        assertThat(PostgresNetworkExecutor.isRowlessWrite(statement)).as("%s answers a result set", type.getSimpleName()).isTrue();
        // a tag that ends with a number is read as a count by a driver: a schema change changes no row
        assertThat(Character.isDigit(tag.charAt(tag.length() - 1))).as("%s is tagged '%s'", type.getSimpleName(), tag).isFalse();
      }
    }
  }

  @Test
  void theCountedFamilyCarriesItsCountAndEverythingElseItsKeywords() {
    assertThat(PostgresCommandTag.ofStatement(new InsertStatement(), 2)).isEqualTo("INSERT 0 2");
    assertThat(PostgresCommandTag.ofStatement(new UpdateStatement(), 5)).isEqualTo("UPDATE 5");
    assertThat(PostgresCommandTag.ofStatement(new SelectStatement(), 0)).isEqualTo("SELECT 0");
    assertThat(PostgresCommandTag.ofStatement(new CreateDocumentTypeStatement(), 1)).isEqualTo("CREATE DOCUMENT TYPE");

    assertThat(PostgresCommandTag.keywordsOf("CreateDocumentTypeStatement")).isEqualTo("CREATE DOCUMENT TYPE");
    assertThat(PostgresCommandTag.keywordsOf("RebuildIndexStatement")).isEqualTo("REBUILD INDEX");
    assertThat(PostgresCommandTag.keywordsOf("AlterTypeStatement")).isEqualTo("ALTER TYPE");

    // a class whose simple name spells nothing (an anonymous subclass) is tagged from the text instead of ""
    final Statement anonymous = new Statement() {
    };
    assertThat(PostgresCommandTag.of(anonymous, "/* h */ SHOW x", 1)).isEqualTo("SHOW");
  }

  @Test
  void theTextOfAnUnparsedStatementIsReadPastItsCommentsAndWhitespace() {
    assertThat(PostgresCommandTag.leadingWords("  /* a /* nested */ hint */ -- line\n  select\t\t1")).isEqualTo("SELECT 1");
    assertThat(PostgresCommandTag.leadingWords("// cypher comment\nMATCH (n) RETURN n")).isEqualTo("MATCH (N) RETURN N");
    assertThat(PostgresCommandTag.ofText("/* h */ SHOW   transaction_isolation", 1)).isEqualTo("SHOW");
    assertThat(PostgresCommandTag.ofText("-- c\nMATCH (n) RETURN n", 4)).isEqualTo("SELECT 4");
    assertThat(PostgresCommandTag.ofText("BEGIN", 0)).isEqualTo("BEGIN");
    // a CREATE VERTEX TYPE is a schema change, not the insert CREATE VERTEX is
    assertThat(PostgresCommandTag.ofText("CREATE VERTEX TYPE V", 1)).isEmpty();
    assertThat(PostgresCommandTag.ofText("CREATE VERTEX V SET a = 1", 1)).isEqualTo("INSERT 0 1");
    // bounded: the tag only ever reads the first words
    assertThat(PostgresCommandTag.leadingWords("SELECT " + "x".repeat(10_000))).hasSize(64);
    assertThat(PostgresCommandTag.leadingWords("/* never closed")).isEmpty();
    // the 64 characters are counted after the leading comments, so a comment of any length still finds the statement
    assertThat(PostgresCommandTag.ofText("/* " + "c".repeat(500) + " */ -- " + "l".repeat(200) + "\nSHOW search_path", 1)).isEqualTo("SHOW");
    // and a comment that never ends tags nothing rather than something else
    assertThat(PostgresCommandTag.ofText("/* " + "SELECT ".repeat(50), 1)).isEmpty();
  }

  private static Statement instantiate(final Class<? extends Statement> type) throws Exception {
    final Constructor<? extends Statement> constructor = type.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  /** Every concrete subclass of {@link Statement} in the class root the engine was loaded from: a directory or a jar. */
  @SuppressWarnings("unchecked")
  private static List<Class<? extends Statement>> concreteStatementClasses() throws IOException, URISyntaxException {
    final Path root = Path.of(Statement.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    final List<String> classNames = new ArrayList<>();
    if (Files.isDirectory(root)) {
      try (final Stream<Path> files = Files.walk(root)) {
        files.filter(f -> f.toString().endsWith(".class")).forEach(f -> classNames.add(root.relativize(f).toString()));
      }
    } else
      try (final JarFile jar = new JarFile(root.toFile())) {
        for (final Enumeration<JarEntry> entries = jar.entries(); entries.hasMoreElements(); ) {
          final String name = entries.nextElement().getName();
          if (name.endsWith(".class"))
            classNames.add(name);
        }
      }

    final List<Class<? extends Statement>> statements = new ArrayList<>();
    final ClassLoader loader = Statement.class.getClassLoader();
    for (final String file : classNames) {
      final String className = file.substring(0, file.length() - ".class".length()).replace('/', '.').replace('\\', '.');
      if (!className.startsWith("com.arcadedb.") || className.contains("$"))
        continue;
      final Class<?> type;
      try {
        type = Class.forName(className, false, loader);
      } catch (final ClassNotFoundException | LinkageError e) {
        // a class of an optional dependency missing from this classpath: no statement is one
        continue;
      }
      // Statement itself is concrete, but is the base the parser never answers with
      if (Statement.class.isAssignableFrom(type) && type != Statement.class && !Modifier.isAbstract(type.getModifiers()))
        statements.add((Class<? extends Statement>) type);
    }
    assertThat(classNames.size()).as("no class read from the engine's class root " + root).isGreaterThan(1000);
    return statements;
  }
}
