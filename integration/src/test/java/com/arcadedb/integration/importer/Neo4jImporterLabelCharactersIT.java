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
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Neo4j accepts any string as a label once it is backtick-quoted, so a real export routinely carries labels the
 * importer used to refuse: dots, accents, colons. The engine stores those fine (a type name is percent-encoded on
 * its way to the file name, and the component-file name is parsed right-to-left so a dot in it survives), so the
 * importer only has to refuse what actually cannot round-trip.
 */
class Neo4jImporterLabelCharactersIT {
  private final static String DATABASE_PATH = "target/databases/neo4j-imported-labels";

  @Test
  void importLabelsWithDotsAccentsAndColons() throws Exception {
    final StringBuilder content = new StringBuilder();
    content.append("{\"type\":\"node\",\"id\":\"0\",\"labels\":[\"acme.Customer\"],\"properties\":{\"name\":\"Alice\"}}\n");
    content.append("{\"type\":\"node\",\"id\":\"1\",\"labels\":[\"acme.crm.Order\"],\"properties\":{\"code\":\"o1\"}}\n");
    content.append("{\"type\":\"node\",\"id\":\"2\",\"labels\":[\"Ünïcøde\"],\"properties\":{\"name\":\"Zoe\"}}\n");
    content.append("{\"type\":\"node\",\"id\":\"3\",\"labels\":[\"with space\"],\"properties\":{\"name\":\"Sam\"}}\n");
    content.append("{\"id\":\"r0\",\"type\":\"relationship\",\"label\":\"HAS.ORDER\",\"properties\":{},");
    content.append("\"start\":{\"id\":\"0\",\"labels\":[\"acme.Customer\"]},\"end\":{\"id\":\"1\",\"labels\":[\"acme.crm.Order\"]}}\n");

    runImport(content.toString());

    try (final Database db = new DatabaseFactory(DATABASE_PATH).open()) {
      assertThat(db.getSchema().existsType("acme.Customer")).isTrue();
      assertThat(db.getSchema().existsType("acme.crm.Order")).isTrue();
      assertThat(db.getSchema().existsType("Ünïcøde")).isTrue();
      assertThat(db.getSchema().existsType("with space")).isTrue();
      assertThat(db.getSchema().existsType("HAS.ORDER")).isTrue();

      assertThat(db.countType("acme.Customer", false)).isEqualTo(1L);
      assertThat(db.countType("HAS.ORDER", false)).isEqualTo(1L);

      // The dotted names have to be reachable from SQL, which means backtick-quoted.
      assertThat(db.query("sql", "select expand(out(`HAS.ORDER`)) from `acme.Customer`").stream().count()).isEqualTo(1L);
    }
  }

  /**
   * A node with no labels used to throw NullPointerException out of the schema pass, before the vertex pass could
   * reach its "skip it" branch. Such nodes now land in the reserved root type rather than being lost - the same
   * sentinel Cypher's own unlabelled nodes use ({@link Labels#NO_LABEL_TYPE}), not a literal {@code Node}: that
   * name is exactly the kind of ordinary label a real graph might also want to write, and leaked as a phantom
   * label onto every imported node - {@code labels(n)} answered {@code ["Node", "Person"]} rather than
   * {@code ["Person"]} - independently of, and before, issue #6395.
   */
  @Test
  void importNodesWithoutLabels() throws Exception {
    final StringBuilder content = new StringBuilder();
    content.append("{\"type\":\"node\",\"id\":\"0\",\"labels\":[\"Person\"],\"properties\":{\"name\":\"Alice\"}}\n");
    content.append("{\"type\":\"node\",\"id\":\"1\",\"properties\":{\"name\":\"NoLabel\"}}\n");
    content.append("{\"type\":\"node\",\"id\":\"2\",\"labels\":[],\"properties\":{\"name\":\"EmptyLabels\"}}\n");
    content.append("{\"type\":\"node\",\"id\":\"3\",\"labels\":null,\"properties\":{\"name\":\"NullLabels\"}}\n");

    runImport(content.toString());

    try (final Database db = new DatabaseFactory(DATABASE_PATH).open()) {
      assertThat(db.getSchema().existsType("Person")).isTrue();
      assertThat(db.countType("Person", false)).isEqualTo(1L);
      // The three unlabelled nodes are kept on the reserved root type instead of being dropped, and are
      // invisible to labels() - unlike a literal "Node" type, this name can never collide with a real label.
      assertThat(db.countType(Labels.NO_LABEL_TYPE, false)).isEqualTo(3L);
      final List<Object> labels = db.query("opencypher", "MATCH (n {name: 'NoLabel'}) RETURN labels(n) AS l")
          .next().getProperty("l");
      assertThat(labels).isEmpty();
    }
  }

  @Test
  void stillRejectLabelsThatCannotRoundTrip() {
    // Path separators and the directory sentinels would address a file outside the database directory.
    assertRejected("../../etc/evil", "path separator");
    assertRejected("dir/sub", "path separator");
    assertRejected("dir\\\\sub", "path separator");
    assertRejected("..", "path separator");
    assertRejected(".", "path separator");
    // '*' is not a legal file name character on Windows and is left untouched by percent-encoding.
    assertRejected("sta*r", "'*'");
    // '~' joins the labels of a multi-label node into a composite type name, so it cannot appear inside one.
    assertRejected("a~b", "reserved");
  }

  private void assertRejected(final String label, final String expectedMessageFragment) {
    final String content = "{\"type\":\"node\",\"id\":\"0\",\"labels\":[\"" + label + "\"],\"properties\":{\"name\":\"x\"}}\n";
    assertThatThrownBy(() -> new Neo4jImporter(new ByteArrayInputStream(content.getBytes()),
        (" -d " + DATABASE_PATH + " -o").split(" ")).run())
        .as("label '%s'", label)
        .isInstanceOf(ImportException.class)
        .hasMessageContaining(expectedMessageFragment);
  }

  private void runImport(final String content) throws Exception {
    new Neo4jImporter(new ByteArrayInputStream(content.getBytes()), (" -d " + DATABASE_PATH + " -o").split(" ")).run();
    TestHelper.checkActiveDatabases();
  }

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }
}
