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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.procedure.Procedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5627: a wrong argument count on a procedure read differently from the same mistake on a
 * function, and reached the client with a different HTTP status.
 * <p>
 * #5602 reduced {@code Function.validateArgs} to {@code Function.checkArity}, phrased through {@code FunctionArity}
 * and raised as a {@code CommandSemanticException} (400). {@code Procedure} kept a hand-written check of its own
 * raising {@code IllegalArgumentException}, which {@code CallStep.executeProcedure} then wrapped in a
 * {@code CommandExecutionException} - a 500 for what is entirely the caller's mistake.
 */
class ProcedureArityIssue5627Test extends TestHelper {

  /** The one sentence both callable kinds use, differing only in the noun. */
  private static final Pattern CANONICAL = Pattern.compile("^(Function|Procedure) '[^']+' expects .+ but got \\d+$");

  @Test
  void aWrongArgumentCountOnAProcedureIsAClientError() {
    // algo.sameCommunity declares 3 arguments exactly. Before the fix this arrived as CommandExecutionException
    // ("Error executing procedure: algo.sameCommunity") wrapping an IllegalArgumentException, i.e. a 500.
    assertThatThrownBy(() -> consume("CALL algo.sameCommunity(1)"))
        .isInstanceOf(CommandSemanticException.class)
        .isNotInstanceOf(CommandExecutionException.class)
        .hasMessage("Procedure 'algo.sameCommunity' expects 3 arguments but got 1");
  }

  @Test
  void aProcedureWithARangeOfAcceptedCountsPhrasesItAsARange() {
    // algo.dijkstra accepts 4 or 5. "4-5 arguments" is what FunctionArity.describe() produces, the same phrasing a
    // function with a range gets.
    assertThatThrownBy(() -> consume("CALL algo.dijkstra(1, 2)"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("Procedure 'algo.dijkstra' expects 4-5 arguments but got 2");
  }

  @Test
  void tooManyArgumentsIsRejectedTheSameWayTooFewIs() {
    assertThatThrownBy(() -> consume("CALL algo.dijkstra(1, 2, 3, 4, 5, 6)"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("Procedure 'algo.dijkstra' expects 4-5 arguments but got 6");
  }

  @Test
  void aProcedureAndAFunctionDescribeTheMistakeTheSameWay() {
    // The point of the issue: one sentence, one exception type, whichever kind of callable the name resolved to.
    final Throwable onAProcedure = catchFrom("CALL algo.sameCommunity(1)");
    final Throwable onAFunction = catchFrom("CALL text.hammingDistance('a')");

    assertThat(onAProcedure).isInstanceOf(CommandSemanticException.class);
    assertThat(onAFunction).isInstanceOf(CommandSemanticException.class);
    assertThat(onAProcedure.getMessage()).matches(CANONICAL);
    assertThat(onAFunction.getMessage()).matches(CANONICAL);
  }

  @Test
  void optionalCallDoesNotSwallowAWrongArgumentCount() {
    // OPTIONAL suppresses "no rows", not "your call is malformed" - the rule #5602 settled for functions, which the
    // procedure path only honours once the arity error is a CommandParsingException.
    assertThatThrownBy(() -> consume("OPTIONAL CALL algo.sameCommunity(1)"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("algo.sameCommunity");
  }

  @Test
  void aCorrectArgumentCountIsStillAccepted() {
    // The guard must reject counts, not calls: algo.degree accepts 0-2, so a no-argument CALL has to run.
    consume("CALL algo.degree()");
  }

  /**
   * Every registered procedure, not only the two called above: a procedure added later cannot reintroduce a wording
   * of its own without failing here.
   */
  @Test
  void everyRegisteredProcedureReportsTheCanonicalSentence() {
    final Collection<CypherProcedure> procedures = CypherProcedureRegistry.getAllProcedures();
    assertThat(procedures).as("the procedure registry is empty, so this sweep proves nothing").isNotEmpty();

    for (final Procedure procedure : procedures) {
      if (procedure.getMinArgs() > 0)
        assertRejects(procedure, procedure.getMinArgs() - 1);
      if (procedure.getMaxArgs() >= 0 && procedure.getMaxArgs() < Integer.MAX_VALUE)
        assertRejects(procedure, procedure.getMaxArgs() + 1);
    }
  }

  @Test
  void aNullArgumentArrayCountsAsNoArguments() {
    // args.length on a null array raised NullPointerException, which CallStep would have wrapped as a 500 too.
    final Procedure procedure = CypherProcedureRegistry.get("algo.sameCommunity");
    assertThat(procedure).isNotNull();
    assertThatThrownBy(() -> procedure.validateArgs(null))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("Procedure 'algo.sameCommunity' expects 3 arguments but got 0");
  }

  private void assertRejects(final Procedure procedure, final int argCount) {
    assertThatThrownBy(() -> procedure.validateArgs(new Object[argCount])).as("%s with %d arguments",
            procedure.getName(), argCount)
        .isInstanceOf(CommandSemanticException.class)
        .satisfies(e -> assertThat(e.getMessage()).matches(CANONICAL));
  }

  private Throwable catchFrom(final String query) {
    try {
      consume(query);
    } catch (final Exception e) {
      return e;
    }
    throw new AssertionError(query + " did not fail");
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
