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
 */
package com.arcadedb.query.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import java.lang.reflect.*;
import java.util.*;
import java.util.logging.*;

public class GraphQLQueryEngine implements QueryEngine {
  private static final String ENGINE_NAME = "graphql-engine";
  private final        Object graphQLSchema;

  public static class GraphQLQueryEngineFactory implements QueryEngineFactory {
    private static Boolean  available = null;
    private static Class<?> graphQLSchemaClass;

    @Override
    public boolean isAvailable() {
      if (available == null) {
        try {
          graphQLSchemaClass = Class.forName("com.arcadedb.graphql.schema.GraphQLSchema");
          available = true;
        } catch (ClassNotFoundException e) {
          available = false;
        }
      }
      return available;
    }

    @Override
    public String getLanguage() {
      return "graphql";
    }

    @Override
    public QueryEngine getInstance(final DatabaseInternal database) {
      Object engine = database.getWrappers().get(ENGINE_NAME);
      if (engine != null)
        return (GraphQLQueryEngine) engine;

      try {
        engine = new GraphQLQueryEngine(graphQLSchemaClass.getConstructor(Database.class).newInstance(database));
        database.setWrapper(ENGINE_NAME, engine);
        return (GraphQLQueryEngine) engine;

      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on initializing GraphQL query engine", e);
        throw new QueryParsingException("Error on initializing GraphQL query engine", e);
      }
    }
  }

  protected GraphQLQueryEngine(final Object graphQLSchema) {
    this.graphQLSchema = graphQLSchema;
  }

  @Override
  public AnalyzedQuery analyze(String query) {
    return new AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return false;
      }

      @Override
      public boolean isDDL() {
        return false;
      }
    };
  }

  @Override
  public ResultSet query(final String query, final Map<String, Object> parameters) {
    return command(query, parameters);
  }

  @Override
  public ResultSet query(final String query, final Object... parameters) {
    return command(query, parameters);
  }

  @Override
  public ResultSet command(final String query, final Map<String, Object> parameters) {
    try {
      final ResultSet resultSet = (ResultSet) GraphQLQueryEngineFactory.graphQLSchemaClass.getMethod("execute", String.class).invoke(graphQLSchema, query);

      return resultSet;
    } catch (InvocationTargetException e) {
      throw new CommandExecutionException("Error on executing GraphQL command:\n" + FileUtils.printWithLineNumbers(query), e.getTargetException());
    } catch (Exception e) {
      throw new QueryParsingException("Error on executing GraphQL query:\n" + FileUtils.printWithLineNumbers(query), e);
    }
  }

  @Override
  public ResultSet command(final String query, final Object... parameters) {
    final Map<String, Object> map = new HashMap<>(parameters.length / 2);
    for (int i = 0; i < parameters.length; i += 2)
      map.put((String) parameters[i], parameters[i + 1]);
    return command(query, map);
  }
}
