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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.vector.TextEmbeddingsImporterLSM;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10767: a Word2Vec or GloVe import with no {@code -vectorProperty} died on a null map key inside the schema, because the
 * setting had no default while {@code -idProperty} beside it had one. Driven through {@link Importer} with no format and no option,
 * so the source reaches the vector importer through content sniffing, which is how a user reaches it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10767VectorPropertyDefaultTest {
  private static final String WORD2VEC = """
      3 4
      alpha 0.1 0.2 0.3 0.4
      beta 0.5 0.6 0.7 0.8
      gamma 0.9 1.0 1.1 1.2
      """;

  @Test
  void sniffedWord2VecImportWithNoOptionsUsesTheDefaultProperties() throws Exception {
    final File source = writeFile("importer-10767-vec.txt", WORD2VEC);
    final String databasePath = "target/databases/test-import-10767-default";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      new Importer(("-url file://" + source.getAbsolutePath() + " -database " + databasePath + " -forceDatabaseCreate true")
          .split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        final DocumentType type = db.getSchema().getType("Node");
        assertThat(type.getProperty(TextEmbeddingsImporterLSM.DEFAULT_VECTOR_PROPERTY).getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
        assertThat(type.getProperty(TextEmbeddingsImporterLSM.DEFAULT_ID_PROPERTY).getType()).isEqualTo(Type.STRING);
        assertThat(db.getSchema().existsIndex("Node[" + TextEmbeddingsImporterLSM.DEFAULT_VECTOR_PROPERTY + "]")).isTrue();
        assertThat(db.countType("Node", true)).isEqualTo(3);
        assertThat(db.query("sql", "select from Node where name = 'beta'").next().<float[]>getProperty("vector"))
            .containsExactly(0.5f, 0.6f, 0.7f, 0.8f);
      }
    } finally {
      dropDatabase(databasePath);
      source.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  @Test
  void blankPropertyNameIsRefusedNamingTheSetting() throws Exception {
    final File source = writeFile("importer-10767-vec-blank.txt", WORD2VEC);
    final String databasePath = "target/databases/test-import-10767-blank";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      for (final String setting : new String[] { "vectorProperty", "idProperty" }) {
        final Importer importer = new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), "-database", databasePath,
            "-forceDatabaseCreate", "true", "-" + setting, " " });
        assertThatThrownBy(importer::load).as(setting).hasStackTraceContaining("-" + setting);
        dropDatabase(databasePath);
      }
    } finally {
      dropDatabase(databasePath);
      source.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  @Test
  void explicitPropertyNamesStillWin() throws Exception {
    final File source = writeFile("importer-10767-vec-explicit.txt", WORD2VEC);
    final String databasePath = "target/databases/test-import-10767-explicit";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      new Importer(("-url file://" + source.getAbsolutePath() + " -database " + databasePath
          + " -forceDatabaseCreate true -vertexType Word -vectorProperty embedding -idProperty word").split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        final DocumentType type = db.getSchema().getType("Word");
        assertThat(type.existsProperty("embedding")).isTrue();
        assertThat(type.existsProperty("word")).isTrue();
        assertThat(type.existsProperty(TextEmbeddingsImporterLSM.DEFAULT_VECTOR_PROPERTY)).isFalse();
        assertThat(db.countType("Word", true)).isEqualTo(3);
      }
    } finally {
      dropDatabase(databasePath);
      source.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /** Names SQL cannot take as written: each is escaped for the context it lands in, identifier or string literal. */
  @Test
  void propertyNamesNeedingEscapeStillWork() throws Exception {
    final File source = writeFile("importer-10767-vec-escape.txt", WORD2VEC);
    final String databasePath = "target/databases/test-import-10767-escape";
    FileUtils.deleteRecursively(new File(databasePath));

    final String vectorProperty = "my`vec\\tor";
    final String idProperty = "it's";
    try {
      new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), "-database", databasePath, "-forceDatabaseCreate",
          "true", "-vertexType", "Word", "-vectorProperty", vectorProperty, "-idProperty", idProperty }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        final DocumentType type = db.getSchema().getType("Word");
        assertThat(type.getProperty(vectorProperty).getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
        assertThat(type.existsProperty(idProperty)).isTrue();
        assertThat(db.getSchema().existsIndex("Word[" + vectorProperty + "]")).isTrue();
        assertThat(db.countType("Word", true)).isEqualTo(3);
      }
    } finally {
      dropDatabase(databasePath);
      source.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  private static File writeFile(final String name, final String content) throws Exception {
    final File file = new File("target/" + name);
    Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);
    return file;
  }

  private static void dropDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    else
      FileUtils.deleteRecursively(new File(databasePath));
  }
}
