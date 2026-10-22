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
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.sql.parser.SimpleNode;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * The classification of a statement - read-only or not, DDL or not - walks its parse tree by reflection
 * ({@link SqlAstInspector}). A GraalVM native image allows that only for the classes its reachability metadata registers,
 * and answers {@code getDeclaredFields()} with an empty array for any other: the walk then saw the root alone, and a write
 * nested in a SELECT was served by every read-only path. Every class the walk reads must therefore be registered, and a
 * runtime where the fields cannot be read at all must fail closed rather than open.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SqlAstNativeReflectionTest extends TestHelper {
  private static final File NATIVE_METADATA = new File(
      "../native/src/main/resources/META-INF/native-image/com.arcadedb/arcadedb-native/reachability-metadata.json");

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createDocumentType("Victim"));
  }

  @Test
  void theNativeImageRegistersTheFieldsOfEveryClassTheWalkReads() throws Exception {
    assumeThat(NATIVE_METADATA).as("the native module is not next to this one").exists();

    final JSONArray reflection = new JSONObject(Files.readString(NATIVE_METADATA.toPath(), StandardCharsets.UTF_8)).getJSONArray(
        "reflection");
    final Set<String> registered = new TreeSet<>();
    for (int i = 0; i < reflection.length(); i++) {
      final JSONObject entry = reflection.getJSONObject(i);
      if (entry.get("type") instanceof String type && entry.getBoolean("allDeclaredFields", false))
        registered.add(type);
    }

    // EVERY PACKAGE OF THE QUERY ENGINE, NOT ONLY THE PARSER'S: THE ANTLR BUILDER HAS NODES OF ITS OWN (StatementExpression
    // HOLDS THE STATEMENT OF A PROJECTION), AND MISSING ONE OF THEM LEFT EXACTLY THAT SHAPE OPEN IN THE NATIVE IMAGE
    final File root = new File(SimpleNode.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    final Set<String> missing = new TreeSet<>();
    final int walked = collectUnregistered(root, new File(root, "com/arcadedb/query"), registered, missing);
    assertThat(walked).isGreaterThan(100);
    assertThat(missing).as("parse-tree classes the native image cannot read the fields of: register each in " + NATIVE_METADATA
        + " with \"allDeclaredFields\": true").isEmpty();
  }

  /**
   * Adds to {@code missing} every parse-tree class under {@code dir} the metadata does not register, and answers how many
   * there are in all. An anonymous node is skipped: its only fields are captured locals, its superclass is walked by name, and
   * its generated name is not stable enough to register.
   */
  private static int collectUnregistered(final File root, final File dir, final Set<String> registered, final Set<String> missing)
      throws Exception {
    int walked = 0;
    for (final File file : dir.listFiles()) {
      if (file.isDirectory()) {
        walked += collectUnregistered(root, file, registered, missing);
        continue;
      }
      if (!file.getName().endsWith(".class"))
        continue;
      final String relative = root.toPath().relativize(file.toPath()).toString();
      final Class<?> type = Class.forName(relative.substring(0, relative.length() - 6).replace(File.separatorChar, '.'), false,
          SimpleNode.class.getClassLoader());
      if (type.isAnonymousClass() || (!SimpleNode.class.isAssignableFrom(type) && !SqlAstInspector.AST_HOLDER_CLASSES.contains(type)))
        continue;
      ++walked;
      if (!registered.contains(type.getName()))
        missing.add(type.getName());
    }
    return walked;
  }

  @Test
  void aNestedWriteIsAWriteWhereTheFieldsCanBeRead() {
    assertThat(SqlAstInspector.astFieldsReadable).isTrue();
    assertThat(database.getQueryEngine("sql").analyze("PROFILE INSERT INTO Victim SET a = 106").isIdempotent()).isFalse();
    assertThat(database.getQueryEngine("sql").analyze("SELECT FROM Victim WHERE a IN (SELECT a FROM (INSERT INTO Victim SET a = 34))")
        .isIdempotent()).isFalse();
    assertThat(database.getQueryEngine("sql").analyze("SELECT $a AS a LET $a = (SELECT a FROM Victim)").isIdempotent()).isTrue();
  }

  @Test
  void withoutReadableFieldsEveryStatementThatMayNestOneFailsClosed() {
    SqlAstInspector.astFieldsReadable = false;
    try {
      // DISTINCT TEXTS: THE STATEMENT CACHE KEEPS THE CLASSIFICATION OF A TEXT ALREADY PARSED
      final var nested = database.getQueryEngine("sql").analyze("SELECT (INSERT INTO Victim SET a = 7001) AS p FROM Victim");
      assertThat(nested.isIdempotent()).isFalse();
      assertThat(nested.getOperationTypes()).contains(OperationType.CREATE);

      assertThatThrownBy(() -> database.query("sql", "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 7002)")).isInstanceOf(
          QueryNotIdempotentException.class);
      assertThat(database.countType("Victim", false)).isZero();
    } finally {
      SqlAstInspector.astFieldsReadable = true;
    }
  }
}
