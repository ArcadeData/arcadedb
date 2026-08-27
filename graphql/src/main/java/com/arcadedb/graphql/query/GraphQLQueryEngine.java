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
package com.arcadedb.graphql.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.graphql.parser.Definition;
import com.arcadedb.graphql.parser.Document;
import com.arcadedb.graphql.parser.GraphQLParser;
import com.arcadedb.graphql.parser.OperationDefinition;
import com.arcadedb.graphql.parser.ParseException;
import com.arcadedb.graphql.parser.TokenMgrException;
import com.arcadedb.graphql.schema.GraphQLSchema;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GraphQLQueryEngine implements QueryEngine {
  public static final String        ENGINE_NAME = "graphql";
  private final       GraphQLSchema graphQLSchema;

  protected GraphQLQueryEngine(final GraphQLSchema graphQLSchema) {
    this.graphQLSchema = graphQLSchema;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    final Set<OperationType> ops = classify(query).ops();
    return new AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return ops.size() == 1 && ops.contains(OperationType.READ);
      }

      @Override
      public boolean isDDL() {
        return false;
      }

      @Override
      public Set<OperationType> getOperationTypes() {
        return ops;
      }
    };
  }

  /**
   * The operation-type classification, plus - when classification fell back to "assume the worst" because the
   * document could not be parsed/lexed - the exception that caused it, so a caller can report the real syntax
   * error instead of just "not idempotent". {@code parseFailure} is {@code null} whenever classification
   * completed normally, including the case where a genuine mutation was recognized.
   */
  private record Classification(Set<OperationType> ops, Exception parseFailure) {
  }

  private static Classification classify(final String query) {
    try {
      final Document doc = GraphQLParser.parse(query);
      for (final Definition def : doc.getDefinitions())
        if (def instanceof OperationDefinition op && !op.isQuery())
          return new Classification(Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE), null);
    } catch (final ParseException | TokenMgrException e) {
      // Cannot classify: assume the worst so an idempotency gate denies rather than admits. Execution
      // still re-parses and reports the real syntax error; this only changes the answer given to a
      // caller asking "is this read-only?" before execution runs. TokenMgrException (unchecked) is the
      // lexical-error counterpart to ParseException: the nesting-depth pre-scan in GraphQLParser.parse()
      // drives the generated token manager directly and a malformed token (e.g. an unterminated string
      // literal) surfaces there as a TokenMgrException rather than a ParseException. See issue #5853.
      return new Classification(Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE), e);
    }
    return new Classification(CollectionUtils.singletonSet(OperationType.READ), null);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    assertIdempotent(query);
    return command(query, null, parameters);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Object... parameters) {
    assertIdempotent(query);
    return command(query, null, parameters);
  }

  private static void assertIdempotent(final String query) {
    final Classification c = classify(query);
    if (c.ops().size() == 1 && c.ops().contains(OperationType.READ))
      return;
    if (c.parseFailure() != null)
      // The document could not be parsed/lexed at all - surface the real cause rather than just "not
      // idempotent", since this is reached for any malformed query submitted through the read-only entry
      // point, not only the pathological-nesting case the depth guard exists for. See issue #5853.
      throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent: " + c.parseFailure().getMessage(), c.parseFailure());
    throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");
  }

  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      // The parameters are the GraphQL variable values of the operation: see issue #6834, before they were dropped
      // and every `$variable` silently resolved to null.
      return graphQLSchema.execute(query, parameters);
    } catch (final CommandParsingException | CommandExecutionException e) {
      // An execution failure raised by the delegated statement is not a syntax problem with the GraphQL document.
      // Narrower than ArcadeDBException on purpose - see the note in GraphQLSchema.executeQuery. Issue #5628.
      throw e;
    } catch (final Exception e) {
      throw new CommandParsingException("Error on executing GraphQL query:\n" + FileUtils.printWithLineNumbers(query), e);
    }
  }

  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Object... parameters) {
    if (parameters.length % 2 != 0)
      throw new IllegalArgumentException("Command parameters must be as pairs `<key>, <value>`");

    final Map<String, Object> map = new HashMap<>(parameters.length / 2);
    for (int i = 0; i < parameters.length; i += 2)
      map.put((String) parameters[i], parameters[i + 1]);
    return command(query, null, map);
  }
}
