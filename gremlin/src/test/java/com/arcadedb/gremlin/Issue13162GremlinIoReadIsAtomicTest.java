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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13162: {@code io().read()} run through {@code db.command("gremlin", ...)} committed the transaction it wrote into every
 * 10000 records and once more at the end, because TinkerPop's bulk readers commit through a graph that declares transactions
 * supported. A file larger than a batch whose later part failed left the earlier batches committed although the statement failed,
 * and inside a transaction of the caller the first batch boundary published the caller's pending work and left them nothing to roll
 * back. The statement is now atomic for the transaction it runs in; the graph API, which has no statement to own a transaction, keeps
 * the commits of the readers.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13162GremlinIoReadIsAtomicTest {
  private static final String PATH    = "./target/test-gremlin-13162-io-read-atomic";
  // MORE THAN ONE BATCH OF THE READERS (10000), SO A COMMIT PER BATCH HAPPENS BEFORE THE LAST RECORD IS REACHED
  private static final int    RECORDS = 10_050;
  private static final String SEED    = "seed";

  private ArcadeGraph     graph;
  private Database        database;
  private ExecutorService otherThread;
  private File            graphson;
  private File            graphml;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    database = (Database) graph.getDatabase();
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.k STRING");
    database.command("sql", "CREATE INDEX ON N (k) UNIQUE");
    database.transaction(() -> database.newVertex("N").set("k", SEED).save());
    otherThread = Executors.newSingleThreadExecutor();
    graphson = new File("./target/test-gremlin-13162-io-read-atomic.json");
    graphml = new File("./target/test-gremlin-13162-io-read-atomic.graphml");
  }

  @AfterEach
  void teardown() {
    otherThread.shutdownNow();
    if (graph != null) {
      if (database.isOpen() && database.isTransactionActive())
        database.rollback();
      graph.drop();
    }
    FileUtils.deleteRecursively(new File(PATH));
    graphson.delete();
    graphml.delete();
  }

  @Test
  void aGraphSONImportThatFailsAfterTheFirstBatchLeavesNothing() throws Exception {
    writeGraphSON(true);

    assertThatThrownBy(() -> run("g.io('" + path(graphson) + "').read()")).isInstanceOf(RuntimeException.class);

    assertThat(database.isTransactionActive()).as("the failed import left a transaction open").isFalse();
    assertThat(count()).as("this thread").isEqualTo(1);
    assertThat(committedCount()).as("the batches before the failure were committed").isEqualTo(1);
  }

  @Test
  void aGraphMLImportThatFailsAfterTheFirstBatchLeavesNothing() throws Exception {
    writeGraphML(true);

    assertThatThrownBy(() -> run("g.io('" + path(graphml) + "').read()")).isInstanceOf(RuntimeException.class);

    assertThat(database.isTransactionActive()).as("the failed import left a transaction open").isFalse();
    assertThat(count()).isEqualTo(1);
    assertThat(committedCount()).as("the batches before the failure were committed").isEqualTo(1);
  }

  @Test
  void anEagerTerminalStepIsAtomicToo() throws Exception {
    writeGraphSON(true);

    assertThatThrownBy(() -> run("g.io('" + path(graphson) + "').read().iterate()")).isInstanceOf(RuntimeException.class);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount()).as("the batches before the failure were committed").isEqualTo(1);
  }

  /** {@code withoutStrategies()} is part of the statement: the strategy that withholds the commits is pinned, as the permission is. */
  @Test
  void aStatementCannotTakeTheStrategyAway() throws Exception {
    writeGraphSON(true);

    assertThat(graph.statementTraversal().withoutStrategies(ArcadeIoRegistrationStrategy.class).getStrategies()
        .getStrategy(ArcadeIoRegistrationStrategy.class)).containsSame(ArcadeIoRegistrationStrategy.forStatement());
    assertThat(graph.traversal().getStrategies().getStrategy(ArcadeIoRegistrationStrategy.class))
        .as("the graph API keeps the plain strategy").containsSame(ArcadeIoRegistrationStrategy.instance());

    assertThatThrownBy(() -> run("g.withoutStrategies(ArcadeIoRegistrationStrategy).io('" + path(graphson) + "').read()"))
        .isInstanceOf(RuntimeException.class);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount()).as("the batches before the failure were committed").isEqualTo(1);
  }

  @Test
  void aSuccessfulImportIsCommittedWithTheStatement() throws Exception {
    writeGraphSON(false);

    run("g.io('" + path(graphson) + "').read()");

    assertThat(database.isTransactionActive()).as("the import left a transaction open").isFalse();
    assertThat(committedCount()).isEqualTo(1 + RECORDS);
  }

  @Test
  void anImportInsideTheTransactionOfTheCallerIsRolledBackWithIt() throws Exception {
    writeGraphSON(false);

    database.begin();
    database.newVertex("N").set("k", "mine").save();
    run("g.io('" + path(graphson) + "').read()");

    assertThat(database.isTransactionActive()).as("the import ended the transaction of the caller").isTrue();
    assertThat(count()).as("the caller reads its own import").isEqualTo(2 + RECORDS);
    assertThat(committedCount()).as("the import published the pending work of the caller").isEqualTo(1);

    database.rollback();

    assertThat(count()).isEqualTo(1);
    assertThat(committedCount()).isEqualTo(1);
  }

  @Test
  void anImportInsideTheTransactionOfTheCallerIsCommittedByTheCaller() throws Exception {
    writeGraphML(false);

    database.begin();
    database.newVertex("N").set("k", "mine").save();
    run("g.io('" + path(graphml) + "').read()");

    assertThat(database.isTransactionActive()).isTrue();
    assertThat(committedCount()).isEqualTo(1);

    database.commit();

    assertThat(committedCount()).isEqualTo(2 + RECORDS);
  }

  /**
   * The graph API has no statement: the readers own the commit there, as in every TinkerPop graph, also when a read made earlier
   * through the graph left the transaction of its AUTO model open. Withholding the commit would lose the import at the close of the
   * graph, which rolls back.
   */
  @Test
  void theGraphApiKeepsTheCommitOfTheReader() throws Exception {
    writeGraphSON(false);
    final GraphTraversalSource g = graph.traversal();

    assertThat(g.V().count().next()).isEqualTo(1L);
    assertThat(database.isTransactionActive()).as("the AUTO model opened a transaction for the read").isTrue();

    g.io(graphson.getAbsolutePath()).read().iterate();

    assertThat(committedCount()).isEqualTo(1 + RECORDS);
  }

  @Test
  void theGraphApiKeepsTheCommitOfTheReaderWithNoTransactionOpen() throws Exception {
    writeGraphML(false);

    assertThat(database.isTransactionActive()).isFalse();
    graph.traversal().io(graphml.getAbsolutePath()).read().iterate();

    assertThat(committedCount()).isEqualTo(1 + RECORDS);
  }

  private void run(final String statement) {
    try (final ResultSet rs = database.command("gremlin", statement)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private static String path(final File file) {
    return file.getAbsolutePath().replace('\\', '/');
  }

  /** One vertex per line. With {@code failing}, the last one repeats the unique key of the vertex the database already holds. */
  private void writeGraphSON(final boolean failing) throws IOException {
    try (final BufferedWriter writer = Files.newBufferedWriter(graphson.toPath(), StandardCharsets.UTF_8)) {
      for (int i = 1; i <= RECORDS; i++) {
        final String key = failing && i == RECORDS ? SEED : "v" + i;
        writer.write("{\"id\":{\"@type\":\"g:Int64\",\"@value\":" + i + "},\"label\":\"N\",\"properties\":{\"k\":[{\"id\":{\"@type\":\"g:Int64\",\"@value\":"
            + i + "},\"value\":\"" + key + "\"}]}}");
        writer.newLine();
      }
    }
  }

  private void writeGraphML(final boolean failing) throws IOException {
    try (final BufferedWriter writer = Files.newBufferedWriter(graphml.toPath(), StandardCharsets.UTF_8)) {
      writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      writer.write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">");
      writer.write("<key id=\"labelV\" for=\"node\" attr.name=\"labelV\" attr.type=\"string\"/>");
      writer.write("<key id=\"k\" for=\"node\" attr.name=\"k\" attr.type=\"string\"/>");
      writer.write("<graph id=\"G\" edgedefault=\"directed\">");
      for (int i = 1; i <= RECORDS; i++) {
        final String key = failing && i == RECORDS ? SEED : "v" + i;
        writer.write("<node id=\"" + i + "\"><data key=\"labelV\">N</data><data key=\"k\">" + key + "</data></node>");
        writer.newLine();
      }
      writer.write("</graph></graphml>");
    }
  }

  private long count() {
    // A SCAN: IT READS WHAT THE TRANSACTION OF THE THREAD WROTE TOO
    long count = 0;
    for (final Iterator<Record> it = database.iterateType("N", false); it.hasNext(); it.next())
      ++count;
    return count;
  }

  /** What another thread reads: only what is committed. */
  private long committedCount() throws Exception {
    return otherThread.submit(this::count).get(30, TimeUnit.SECONDS);
  }
}
