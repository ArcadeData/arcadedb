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
package com.arcadedb.gremlin.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12047: on the GraphSON route for non-RID vertex ids an edge whose endpoint is not in the file was logged on the server
 * and counted nowhere, so the import report could not show it. It is now reported as {@code skippedEdges}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12047GraphSONSkippedEdgesTest {
  private static final String DATABASE_PATH = "target/databases/issue-12047-graphson-skipped-edges";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void edgesToAMissingVertexAreCountedInTheReport() throws Exception {
    final File source = new File("target/issue12047.graphson");
    Files.writeString(source.toPath(), """
        {"id":1,"label":"p","outE":{"k":[{"id":10,"inV":2},{"id":11,"inV":99},{"id":12,"inV":1}]},"properties":{}}
        {"id":2,"label":"p","properties":{}}
        """, StandardCharsets.UTF_8);

    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    try {
      final Map<String, Object> report = new Importer(database, source.getAbsolutePath()).load();
      assertThat(report).containsEntry("createdVertices", 2L).containsEntry("createdEdges", 2L)
          .containsEntry("skippedEdges", 1L);
      assertThat(database.countType("k", true)).isEqualTo(2);
    } finally {
      database.drop();
      source.delete();
    }
  }

  /**
   * The edge pass is one all-or-nothing transaction (issue #8160): when it throws, the report goes back to what the committed
   * passes made, and an edge the pass dropped before it threw is not part of what the report says about the import.
   */
  @Test
  void aRolledBackEdgePassDoesNotReportTheEdgesItDropped() throws Exception {
    final File source = new File("target/issue12047-rolled-back.graphson");
    // THE EDGE OF "a" NAMES A VERTEX THAT IS NOT IN THE FILE (DROPPED, COUNTED), THE ONE OF "b" NAMES A DOCUMENT TYPE (THROWS)
    Files.writeString(source.toPath(), """
        {"id":"a","label":"p","outE":{"k":[{"id":10,"inV":"missing"}]},"properties":{}}
        {"id":"b","label":"p","outE":{"NotAnEdge":[{"id":11,"inV":"a"}]},"properties":{}}
        """, StandardCharsets.UTF_8);

    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    try {
      database.command("sql", "CREATE DOCUMENT TYPE NotAnEdge");

      final Importer importer = new Importer(database, source.getAbsolutePath());
      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class);

      final ImporterContext context = importer.getContext();
      assertThat(context.createdVertices.get()).as("the vertex pass committed").isEqualTo(2L);
      assertThat(context.createdEdges.get()).isEqualTo(0L);
      assertThat(context.skippedEdges.get()).as("the edge the rolled back pass dropped is not reported").isEqualTo(0L);
    } finally {
      while (database.isTransactionActive())
        database.rollback();
      database.drop();
      source.delete();
    }
  }
}
