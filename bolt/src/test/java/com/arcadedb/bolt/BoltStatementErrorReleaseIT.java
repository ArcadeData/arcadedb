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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A statement that ends in a JVM {@link Error} while it runs, over BOLT: the statement's resources (its admission slot, its entry in the
 * list of running statements, its result set) go back whatever ended it, a statement that overflows the thread's stack is answered with
 * a FAILURE like any other failed statement and leaves the connection usable, and any other {@link Error} ends the connection with
 * the statement's resources released.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class BoltStatementErrorReleaseIT extends BaseBoltServerTest {
  private static final String FUNCTION = "boltraiseerror";
  // The explicit SQL namespace: an undotted name has to be one of the functions the Cypher grammar knows
  private static final String CALL     = "sql." + FUNCTION;

  // The engine evaluates the rows of a result in batches of 100: the RUN reads the first batch, a PULL or a DISCARD the ones after it
  private static final int    LATER_BATCH_ROW = 150;
  private static final String MANY_ROWS       = "UNWIND range(1, 250) AS x RETURN " + CALL + "(x) AS v";

  // Shared by the tests of the class and set by each one: sound because the methods of one class run sequentially
  private static volatile Supplier<Error> failure;
  private static volatile long            failOn;

  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  /** Returns its argument, except for the one value a test chose, for which it raises the error the test installed. */
  private static final class RaiseOnValue extends SQLFunctionAbstract {
    RaiseOnValue() {
      super(FUNCTION);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      if (params[0] instanceof Number number && number.longValue() == failOn)
        throw failure.get();
      return params[0];
    }

    @Override
    public int getMinArgs() {
      return 1;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getSyntax() {
      return FUNCTION + "(<value>)";
    }
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @BeforeEach
  void registerFunction() {
    // ONE SLOT AND NO WAITING: A SLOT THAT IS NOT GIVEN BACK REFUSES THE NEXT STATEMENT OF ANY CONNECTION
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);
    sqlEngine().getFunctionFactory().register(new RaiseOnValue());
  }

  @AfterEach
  @Override
  public void endTest() {
    sqlEngine().getFunctionFactory().unregister(FUNCTION);
    failure = null;
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private SQLQueryEngine sqlEngine() {
    return (SQLQueryEngine) getServerDatabase(0, getDatabaseName()).getQueryEngine("sql");
  }

  @Test
  void aStatementThatOverflowsTheStackWhenRunIsAnsweredAFailureAndGivesItsResourcesBack() throws Exception {
    failure = StackOverflowError::new;
    failOn = 1;

    try (final BoltWireConnection wire = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      wire.setReadTimeout(30_000);

      wire.run("UNWIND [1] AS x RETURN " + CALL + "(x) AS v");
      assertFailed(wire.readSummary());
      assertResourcesGivenBack();

      assertTheConnectionStillServes(wire);
    }
  }

  @Test
  void aStatementThatOverflowsTheStackWhenPulledIsAnsweredAFailureAndGivesItsResourcesBack() throws Exception {
    failure = StackOverflowError::new;
    failOn = LATER_BATCH_ROW;

    try (final BoltWireConnection wire = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      wire.setReadTimeout(30_000);

      // THE RUN EVALUATES THE FIRST BATCH OF ROWS, THE PULL THE ONES AFTER IT
      wire.run(MANY_ROWS);
      final BoltWireConnection.Summary run = wire.readSummary();
      assertThat(run.signature()).as("RUN: %s", run.message()).isEqualTo(BoltMessage.SUCCESS);

      wire.pull(-1, -1);
      assertFailed(wire.readSummary());
      assertResourcesGivenBack();

      assertTheConnectionStillServes(wire);
    }
  }

  @Test
  void aStatementThatOverflowsTheStackWhenDiscardedIsAnsweredAFailureAndGivesItsResourcesBack() throws Exception {
    failure = StackOverflowError::new;
    failOn = LATER_BATCH_ROW;

    try (final BoltWireConnection wire = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      wire.setReadTimeout(30_000);

      wire.run(MANY_ROWS);
      final BoltWireConnection.Summary run = wire.readSummary();
      assertThat(run.signature()).as("RUN: %s", run.message()).isEqualTo(BoltMessage.SUCCESS);

      wire.discard(-1, -1);
      assertFailed(wire.readSummary());
      assertResourcesGivenBack();

      assertTheConnectionStillServes(wire);
    }
  }

  @Test
  void aStatementThatOverflowsTheStackInATransactionLeavesItRolledBackByTheReset() throws Exception {
    failure = StackOverflowError::new;
    failOn = 1;

    try (final BoltWireConnection wire = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      wire.setReadTimeout(30_000);
      wire.begin(getDatabaseName());

      wire.run("UNWIND [1] AS x RETURN " + CALL + "(x) AS v");
      assertFailed(wire.readSummary());
      assertResourcesGivenBack();

      assertTheConnectionStillServes(wire);
    }
  }

  @Test
  void aStatementThatEndsInAnotherErrorGivesItsResourcesBackAndEndsTheConnection() throws Exception {
    failure = () -> new LinkageError("simulated");
    failOn = 1;

    try (final BoltWireConnection wire = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      wire.setReadTimeout(30_000);

      wire.run("UNWIND [1] AS x RETURN " + CALL + "(x) AS v");
      // THE CONNECTION'S THREAD ENDS WITH THE ERROR: THERE IS NO ANSWER, THE SOCKET IS CLOSED
      final Object[] answer = new Object[1];
      assertThat(catchThrowable(() -> answer[0] = wire.readSummary())).as("answer: %s", answer[0]).isInstanceOf(IOException.class);

      assertResourcesGivenBack();
    }

    // THE SLOT IS FREE FOR ANOTHER CONNECTION
    try (final BoltWireConnection other = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      other.setReadTimeout(30_000);
      assertTheConnectionStillServes(other);
    }
  }

  /** The summary is a FAILURE that is the one the test caused: its text names no problem with the statement itself. */
  private static void assertFailed(final BoltWireConnection.Summary summary) {
    assertThat(summary.signature()).as("a FAILURE, got %s", summary.metadata()).isEqualTo(BoltMessage.FAILURE);
    assertThat(String.valueOf(summary.message())).as("the statement itself is valid").doesNotContain("nknown function", "arity", "Failed to parse");
  }

  /** The slot, the entry in the list of running statements and the result set are given back: waited for, as a dying thread does it last. */
  private void assertResourcesGivenBack() {
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(gate.getRunning()).as("admission slots in use").isZero();
      assertThat(getServer(0).getRunningQueries().getRunning()).as("running statements listed").noneMatch(q -> "bolt".equals(q.getProtocol()));
    });
  }

  /** What a client does after a FAILURE: RESET, then run a statement, which the one slot there is must admit. */
  private void assertTheConnectionStillServes(final BoltWireConnection wire) throws IOException {
    wire.sendNoFields(BoltMessage.RESET);
    assertThat(wire.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

    wire.run("RETURN 1 AS one");
    assertThat(wire.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    wire.pull(-1, -1);
    final BoltWireConnection.Summary summary = wire.readSummary();
    assertThat(summary.signature()).isEqualTo(BoltMessage.SUCCESS);
    assertThat(summary.records()).hasSize(1);
    assertResourcesGivenBack();
  }
}
