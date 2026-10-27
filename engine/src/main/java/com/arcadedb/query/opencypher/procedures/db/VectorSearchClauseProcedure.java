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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * What the Cypher 25 {@code SEARCH} subclause of a {@code MATCH} runs (issue #10776):
 * <pre>
 * MATCH (d:Doc)
 * SEARCH d IN (VECTOR INDEX `Doc[embedding]` FOR $q WHERE d.lang = 'en' LIMIT 10) SCORE AS s
 * RETURN d.name, s
 * </pre>
 * The {@code k} nearest records of the index as rows of {@code node} and {@code score}, exactly as
 * {@code db.index.vector.queryNodes} answers them, with the clause's own {@code WHERE} applied while the index is searched
 * rather than to its top k, so {@code LIMIT 10} answers 10 matching rows when the index holds them.
 * <p>
 * <b>Cost of a selective {@code WHERE}.</b> A rejected candidate is skipped and the search asked again with twice the
 * candidates, until k rows match, the index has handed over everything it holds, or the budget reaches
 * {@link com.arcadedb.index.vector.VectorUtils#MAX_FETCH_CANDIDATES} (issue #10782). Every candidate up to there is loaded
 * and evaluated, so a filter that matches few records walks a large part of the index; the command's deadline still bounds
 * it.
 * <p>
 * Not registered in the procedure registry: one instance is built per {@code SEARCH} clause by the parser, since it carries
 * that clause's binding variable and its {@code WHERE}. Arguments, evaluated per input row: the index name, the query
 * vector, k.
 */
public class VectorSearchClauseProcedure implements CypherProcedure {
  public static final String NAME = "SEARCH VECTOR INDEX";

  private final String            variable;
  private final BooleanExpression filter;
  private final String            scoreVariable;

  /**
   * @param variable      the binding variable of the clause, which its {@code WHERE} reads the candidate record as
   * @param filter        the clause's {@code WHERE}, or null when it has none
   * @param scoreVariable the {@code SCORE AS} variable, or null when the clause has none
   */
  public VectorSearchClauseProcedure(final String variable, final BooleanExpression filter, final String scoreVariable) {
    this.variable = variable;
    this.filter = filter;
    this.scoreVariable = scoreVariable;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "The k nearest neighbors of a vector in a vector index, for the Cypher 25 SEARCH clause";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  public BooleanExpression getFilter() {
    return filter;
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);
    if (args[0] == null)
      throw new CommandSQLParsingException(NAME + ": the index name is null");
    // SCORE AS declares a new variable: rebinding one an earlier clause bound would silently overwrite it
    if (scoreVariable != null && inputRow != null && inputRow.hasProperty(scoreVariable))
      throw new CommandSemanticException(
          "SEARCH: the score variable '" + scoreVariable + "' is already defined by an earlier clause");
    final Stream<Result> rows = DbIndexVectorQueryNodes.search(args[0].toString(), args[2], args[1],
        buildFilter(inputRow, context), context, NAME).stream();

    // A binding variable an earlier clause already bound (MATCH (d) WITH d MATCH (d) SEARCH d ...) is restricted to the k
    // nearest, the way a MATCH restricts a bound node to its pattern: its row stays when it is one of them and goes when
    // it is not, instead of the search rebinding it to every neighbor
    if (inputRow == null || !inputRow.hasProperty(variable))
      return rows;
    final Object bound = inputRow.getProperty(variable);
    if (!(bound instanceof Identifiable boundRecord))
      return Stream.empty();
    final RID boundRid = boundRecord.getIdentity();
    return rows.filter(row -> row.<Identifiable>getProperty("node").getIdentity().equals(boundRid));
  }

  /**
   * The clause's {@code WHERE} as a predicate on a candidate record, read with the input row's bindings and the binding
   * variable set to the candidate. Only {@code true} passes: a {@code null} answer rejects the candidate, as {@code WHERE}
   * rejects a row.
   */
  private Predicate<Document> buildFilter(final Result inputRow, final CommandContext context) {
    if (filter == null)
      return null;
    // One row per search, rebound to each candidate in turn instead of copied once per candidate. The predicate is
    // therefore not thread-safe: it relies on the search testing its candidates one at a time, as searchAndLoad does
    final ResultInternal row = inputRow != null ? ResultInternal.copyBindings(inputRow, 1) : new ResultInternal();
    return record -> {
      row.setProperty(variable, record);
      return Boolean.TRUE.equals(filter.evaluateTernary(row, context));
    };
  }
}
