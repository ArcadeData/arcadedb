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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graphql.parser.Definition;
import com.arcadedb.graphql.parser.Document;
import com.arcadedb.graphql.parser.GraphQLParser;
import com.arcadedb.graphql.parser.OperationDefinition;
import com.arcadedb.graphql.parser.ParseException;
import com.arcadedb.graphql.schema.GraphQLSchema;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import java.util.*;

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
    final Set<OperationType> ops = detectGraphQLOperationTypes(query);
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

  private static Set<OperationType> detectGraphQLOperationTypes(final String query) {
    try {
      final Document doc = GraphQLParser.parse(query);
      for (final Definition def : doc.getDefinitions())
        if (def instanceof OperationDefinition op && !op.isQuery())
          return Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
    } catch (final ParseException e) {
      // Fall through to default READ — execution will report the parse error
    }
    return CollectionUtils.singletonSet(OperationType.READ);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    return command(query, null, parameters);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Object... parameters) {
    return command(query, null, parameters);
  }

  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      final ResultSet resultSet = graphQLSchema.execute(query);
      return resultSet;
    } catch (final CommandParsingException e) {
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
