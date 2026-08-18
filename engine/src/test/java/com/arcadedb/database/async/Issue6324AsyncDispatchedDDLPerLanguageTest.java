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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6324, item 5: the routing that sends dispatched DDL off the async workers stopped being
 * SQL's private knowledge.
 * <p>
 * #6303 gave back {@code CREATE INDEX} / {@code REBUILD INDEX} sent with {@code awaitResponse=false} by routing
 * statements that parse to DDL onto {@code AsyncCommandPool}, whose threads are not workers of any executor and can
 * therefore satisfy the barrier a scan-based index build needs. The classification lived in the dispatcher and knew
 * about {@code sql} and {@code sqlscript} only, so the same statement in Cypher still ran on a worker and was still
 * refused there by #6281's guard - a refusal rather than the old hang, but an asymmetry a user meets without warning.
 * <p>
 * The question is now asked of the LANGUAGE, through {@link QueryEngine#classifyDDL(String)}. Cypher can answer it
 * for the same reason SQL can - the parse is a statement-cache lookup the execution repeats - and an engine that
 * cannot answer cheaply says {@code UNKNOWN} and keeps the behaviour it has always had.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6324AsyncDispatchedDDLPerLanguageTest extends TestHelper {

  /** The reported shape in Cypher: {@code CREATE INDEX} sent without waiting for the response. */
  @Test
  @Timeout(180)
  void cypherCreateIndexDispatchedAsynchronouslyBuildsTheIndex() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER));
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newVertex("V").set("id", i).save();
    });

    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<String> ranOn = new AtomicReference<>();
    final AtomicReference<Exception> failure = new AtomicReference<>();
    database.async().command("cypher", "CREATE INDEX FOR (n:V) ON (n.id)", new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
        ranOn.set(Thread.currentThread().getName());
        done.countDown();
      }

      @Override
      public void onError(final Exception exception) {
        failure.compareAndSet(null, exception);
        done.countDown();
      }
    });

    assertThat(done.await(120, TimeUnit.SECONDS)).as("the command must run to an answer, not park").isTrue();
    assertThat(failure.get()).as("the barrier is satisfiable off the workers, so there is nothing to refuse").isNull();
    assertThat(ranOn.get()).as("Cypher DDL goes to the same pool SQL DDL does").startsWith("ArcadeDB-AsyncCommand-");

    database.async().waitCompletion();

    assertThat(database.getSchema().getIndexByName("V[id]")).isNotNull();
    assertThat(database.getSchema().getIndexByName("V[id]").countEntries())
        .as("and it indexed the records that were already there").isEqualTo(50);
  }

  /**
   * The other half, and the half widening the fix would break: an ordinary Cypher WRITE keeps running on a worker,
   * where its batch transaction and its pinned bucket are. {@code CREATE (n:V ...)} carries the word the cheap filter
   * looks for, so this is also the case that proves the decision is the parse and not the keyword.
   */
  @Test
  @Timeout(180)
  void anOrdinaryCypherCreateStillRunsOnAWorker() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER));

    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<String> ranOn = new AtomicReference<>();
    database.async().command("cypher", "CREATE (n:V {id: 1})", new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
        ranOn.set(Thread.currentThread().getName());
        done.countDown();
      }

      @Override
      public void onError(final Exception exception) {
        ranOn.set("error: " + exception);
        done.countDown();
      }
    });

    assertThat(done.await(120, TimeUnit.SECONDS)).isTrue();
    assertThat(ranOn.get()).as("a Cypher write is not DDL and must keep its worker").doesNotStartWith("ArcadeDB-AsyncCommand-");

    database.async().waitCompletion();
    assertThat(database.countType("V", false)).isEqualTo(1);
  }

  /** The hook itself, asked directly, for the three languages that answer it and one that does not. */
  @Test
  @Timeout(60)
  void theLanguagesThatCanClassifyDoAndTheOthersSaySo() {
    database.transaction(() -> database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER));

    final QueryEngineManager engines = QueryEngineManager.getInstance();

    assertThat(engines.getEngine("sql", (DatabaseInternal) database)
        .classifyDDL("CREATE INDEX ON V (id) UNIQUE")).isEqualTo(QueryEngine.DDLClassification.DDL);
    assertThat(engines.getEngine("sql", (DatabaseInternal) database)
        .classifyDDL("INSERT INTO V SET id = 1")).isEqualTo(QueryEngine.DDLClassification.NOT_DDL);

    assertThat(engines.getEngine("sqlscript", (DatabaseInternal) database)
        .classifyDDL("INSERT INTO V SET id = 1; CREATE INDEX ON V (id) UNIQUE;"))
        .as("a script routes as a whole, on whether ANY statement in it is DDL")
        .isEqualTo(QueryEngine.DDLClassification.DDL);

    assertThat(engines.getEngine("cypher", (DatabaseInternal) database)
        .classifyDDL("CREATE INDEX FOR (n:V) ON (n.id)")).isEqualTo(QueryEngine.DDLClassification.DDL);
    assertThat(engines.getEngine("cypher", (DatabaseInternal) database)
        .classifyDDL("CREATE (n:V {id: 1})"))
        .as("the word CREATE is not the decision: the parse is").isEqualTo(QueryEngine.DDLClassification.NOT_DDL);

    // An engine that cannot classify without paying for a parse execution will not reuse says UNKNOWN rather than
    // guessing. Asked of a language that ships with the engine module so the assertion does not depend on which
    // optional modules are on the test classpath.
    assertThat(engines.getEngine("java", (DatabaseInternal) database).classifyDDL("anything"))
        .isEqualTo(QueryEngine.DDLClassification.UNKNOWN);
  }
}
