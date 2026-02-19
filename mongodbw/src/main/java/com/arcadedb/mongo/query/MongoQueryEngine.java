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
package com.arcadedb.mongo.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.mongo.MongoDBDatabaseWrapper;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;
import java.util.logging.*;

public class MongoQueryEngine implements QueryEngine {
  public static final String                 ENGINE_NAME = "mongo";
  private final       MongoDBDatabaseWrapper mongoDBWrapper;

  protected MongoQueryEngine(final MongoDBDatabaseWrapper mongoDBWrapper) {
    this.mongoDBWrapper = mongoDBWrapper;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    final Set<OperationType> ops = detectMongoOperationTypes(query);
    return new AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return ops.size() == 1 && ops.contains(OperationType.READ);
      }

      @Override
      public boolean isDDL() {
        return ops.contains(OperationType.SCHEMA);
      }

      @Override
      public Set<OperationType> getOperationTypes() {
        return ops;
      }
    };
  }

  private static Set<OperationType> detectMongoOperationTypes(final String query) {
    final String trimmed = query.trim();
    // MongoDB commands are JSON with a collection.method pattern or JSON with command keys
    final String upper = trimmed.toUpperCase(Locale.ENGLISH);

    if (upper.contains("\"INSERT\"") || upper.contains("\"INSERTONE\"") || upper.contains("\"INSERTMANY\""))
      return CollectionUtils.singletonSet(OperationType.CREATE);
    if (upper.contains("\"UPDATE\"") || upper.contains("\"UPDATEONE\"") || upper.contains("\"UPDATEMANY\"")
        || upper.contains("\"REPLACEONE\""))
      return CollectionUtils.singletonSet(OperationType.UPDATE);
    if (upper.contains("\"DELETE\"") || upper.contains("\"DELETEONE\"") || upper.contains("\"DELETEMANY\"")
        || upper.contains("\"REMOVE\""))
      return CollectionUtils.singletonSet(OperationType.DELETE);
    if (upper.contains("\"FIND\"") || upper.contains("\"AGGREGATE\"") || upper.contains("\"COUNT\"")
        || upper.contains("\"DISTINCT\""))
      return CollectionUtils.singletonSet(OperationType.READ);
    if (upper.contains("\"CREATEINDEX\"") || upper.contains("\"CREATECOLLECTION\"") || upper.contains("\"DROP\"")
        || upper.contains("\"DROPCOLLECTION\"") || upper.contains("\"DROPINDEX\""))
      return CollectionUtils.singletonSet(OperationType.SCHEMA);

    // Cannot classify: assume all write types for safety
    return Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      return mongoDBWrapper.query(query);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Mongo query engine", e);
      throw new CommandParsingException("Error on initializing Mongo query engine", e);
    }
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Object... parameters) {
    return query(query, null, (Map) null);
  }

  // TODO: This command method can only handle queries, a command method needs to be provided in mongoDBWrapper
  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      return mongoDBWrapper.query(query);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Mongo query engine", e);
      throw new CommandParsingException("Error on initializing Mongo query engine", e);
    }
  }

  // TODO: This command method can only handle queries, a command method needs to be provided in mongoDBWrapper
  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Object... parameters) {
    return query(query, null, (Map) null);
  }
}
