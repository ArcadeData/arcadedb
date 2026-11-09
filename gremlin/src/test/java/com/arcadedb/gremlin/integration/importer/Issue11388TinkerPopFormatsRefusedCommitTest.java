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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11388: the two importer formats of the gremlin module resolve a commit the engine refuses before it starts - a
 * rollback-only transaction, a terminated statement, a server that stopped accepting writes - without leaving the import's
 * transaction on the caller's thread. {@code GraphSONImporterFormat}'s non-RID passes cleared their ownership flag before the
 * commit and never restored it, so the rollback that should have discarded the refused transaction skipped it, the mechanism
 * of #11046, #11208 and #11271; the passes now resolve their transaction through {@code ImporterTransactions}. The GraphML and
 * RID-format GraphSON paths commit inside TinkerPop's reader, and are pinned here against the same refusal.
 * <p>
 * The refusal is the engine's own: a listener marks the import's transaction rollback-only, and {@code commit1stPhase()}
 * refuses it before doing anything. Nothing is proxied.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11388TinkerPopFormatsRefusedCommitTest {
  private static final String DB_PATH   = "target/databases/issue-11388-refused-commit";
  private static final String DATA_PATH = "target/test-data/issue-11388-refused-commit";
  private static final String REFUSAL   = "issue 11388: refused commit";

  private static final String NON_RID_GRAPHSON = """
      {"id":"http://ex/n1","label":"Person","properties":{"name":[{"id":1,"value":"Jay"}]},\
      "outE":{"knows":[{"id":10,"inV":"http://ex/n2"}]}}
      {"id":"http://ex/n2","label":"Person","properties":{"name":[{"id":2,"value":"Kim"}]}}
      """;

  private static final String GRAPHML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <graphml xmlns="http://graphml.graphdrawing.org/xmlns">
        <key id="labelV" for="node" attr.name="labelV" attr.type="string"/>
        <key id="labelE" for="edge" attr.name="labelE" attr.type="string"/>
        <key id="name" for="node" attr.name="name" attr.type="string"/>
        <graph id="G" edgedefault="directed">
          <node id="1"><data key="labelV">Person</data><data key="name">Jay</data></node>
          <node id="2"><data key="labelV">Person</data><data key="name">Kim</data></node>
          <edge id="10" source="1" target="2"><data key="labelE">knows</data></edge>
        </graph>
      </graphml>
      """;

  private Database database;
  private int      refusals;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
    new File(DATA_PATH).mkdirs();
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("knows");
      database.getSchema().createDocumentType("Marker");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
  }

  @Test
  void aRefusedNonRidGraphSONCommitLeavesNoTransactionOnTheThread() throws IOException {
    refuseTheCommitOf("Person");
    final Importer importer = new Importer(database, write("graph.graphson", NON_RID_GRAPHSON));

    assertThatThrownBy(importer::load).hasStackTraceContaining(REFUSAL);

    assertThat(refusals).as("both vertices land in the one pass whose commit is refused").isEqualTo(2);
    assertNothingLeftOnTheThread();
    assertThat(database.countType("Person", true)).isZero();
  }

  /** What the stranded transaction did: the caller's next unit of work nested inside it and never ended. */
  @Test
  void theCallersNextUnitOfWorkAfterARefusedGraphSONImportIsItsOwn() throws IOException {
    refuseTheCommitOf("Person");
    final Importer importer = new Importer(database, write("graph.graphson", NON_RID_GRAPHSON));
    assertThatThrownBy(importer::load).hasStackTraceContaining(REFUSAL);

    database.begin();
    database.newDocument("Marker").set("name", "next").save();
    database.commit();

    assertThat(database.isTransactionActive()).as("one begin/commit pair leaves nothing active").isFalse();
    assertThat(database.countType("Marker", true)).isEqualTo(1);
  }

  /** The edge pass resolves its own transaction the same way, after the vertex pass committed. */
  @Test
  void aRefusedNonRidGraphSONEdgePassLeavesNoTransactionOnTheThread() throws IOException {
    refuseTheCommitOf("knows");
    final Importer importer = new Importer(database, write("graph.graphson", NON_RID_GRAPHSON));

    assertThatThrownBy(importer::load).hasStackTraceContaining(REFUSAL);

    assertThat(refusals).isEqualTo(1);
    assertNothingLeftOnTheThread();
    assertThat(database.countType("Person", true)).as("the vertex pass committed before the edge pass").isEqualTo(2);
    assertThat(database.countType("knows", true)).isZero();
  }

  @Test
  void aRefusedGraphMLCommitLeavesNoTransactionOnTheThread() throws IOException {
    refuseTheCommitOf("Person");
    final Importer importer = new Importer(database, write("graph.graphml", GRAPHML));

    assertThatThrownBy(importer::load).hasStackTraceContaining(REFUSAL);

    assertThat(refusals).isEqualTo(2);
    assertNothingLeftOnTheThread();
    assertThat(database.countType("Person", true)).isZero();
  }

  /**
   * Under a caller's transaction the import writes into it and commits nothing of its own: the refusal is the caller's
   * commit's, and the caller's transaction is still the one on the thread when the import returns.
   */
  @Test
  void insideACallersTransactionTheGraphSONImportLeavesItTheCallers() throws IOException {
    database.begin();
    final TransactionContext callers = ((DatabaseInternal) database).getTransaction();
    database.newDocument("Marker").set("name", "caller").save();

    new Importer(database, write("graph.graphson", NON_RID_GRAPHSON)).load();

    assertThat(((DatabaseInternal) database).getTransactionIfExists()).isSameAs(callers);
    assertThat(callers.isActive()).isTrue();
    database.commit();
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countType("Marker", true)).isEqualTo(1);
    assertThat(database.countType("Person", true)).isEqualTo(2);
  }

  /** Marks the import's transaction rollback-only from inside it: the engine then refuses the commit before it starts. */
  private void refuseTheCommitOf(final String typeName) {
    database.getSchema().getType(typeName).getEvents().registerListener((AfterRecordCreateListener) record -> {
      ++refusals;
      ((DatabaseInternal) database).getTransaction().setRollbackOnly(REFUSAL);
    });
  }

  private void assertNothingLeftOnTheThread() {
    assertThat(database.isTransactionActive()).as("the refused transaction is rolled back, not stranded").isFalse();
    final TransactionContext top = ((DatabaseInternal) database).getTransactionIfExists();
    assertThat(top == null || !top.isActive()).as("no live transaction left on the thread's stack").isTrue();
  }

  private String write(final String name, final String content) throws IOException {
    final File file = new File(DATA_PATH, name);
    Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);
    return "file://" + file.getAbsolutePath();
  }
}
