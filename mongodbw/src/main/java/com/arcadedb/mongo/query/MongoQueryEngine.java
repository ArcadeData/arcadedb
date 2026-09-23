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
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.log.LogManager;
import com.arcadedb.mongo.MongoDBDatabaseWrapper;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class MongoQueryEngine implements QueryEngine {
  public static final String                 ENGINE_NAME = "mongo";

  // TOP-LEVEL COMMAND KEYS, GROUPED BY THE OPERATION TYPE THEY DECLARE. CHECKED AGAINST THE PARSED
  // JSON'S KEYS, NEVER AGAINST THE RAW TEXT, SO A FILTER VALUE LIKE "delete" CANNOT MASQUERADE AS
  // THE COMMAND VERB (#8255)
  private static final Set<String> CREATE_KEYS = Set.of("insert", "insertone", "insertmany");
  private static final Set<String> UPDATE_KEYS = Set.of("update", "updateone", "updatemany", "replaceone");
  private static final Set<String> DELETE_KEYS = Set.of("delete", "deleteone", "deletemany", "remove");
  private static final Set<String> READ_KEYS   = Set.of("find", "aggregate", "count", "distinct", "collection");
  private static final Set<String> SCHEMA_KEYS = Set.of("createindex", "createcollection", "drop", "dropcollection", "dropindex");

  private final MongoDBDatabaseWrapper mongoDBWrapper;

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
    final Set<String> keys;
    try {
      final JSONObject json = new JSONObject(query.trim());
      keys = new HashSet<>();
      for (final String key : json.keySet())
        keys.add(key.toLowerCase(Locale.ENGLISH));
    } catch (final Exception e) {
      // Not parseable JSON: cannot classify, assume all write types for safety
      return Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
    }

    if (!Collections.disjoint(keys, CREATE_KEYS))
      return CollectionUtils.singletonSet(OperationType.CREATE);
    if (!Collections.disjoint(keys, UPDATE_KEYS))
      return CollectionUtils.singletonSet(OperationType.UPDATE);
    if (!Collections.disjoint(keys, DELETE_KEYS))
      return CollectionUtils.singletonSet(OperationType.DELETE);
    // CHECKED BEFORE READ_KEYS: "collection" IS A READ SIGNAL PRESENT ON EVERY QUERY THIS ENGINE ACCEPTS,
    // SO IT WOULD OTHERWISE ALWAYS WIN OVER AN EXPLICIT SCHEMA VERB LIKE "dropCollection" (#8255)
    if (!Collections.disjoint(keys, SCHEMA_KEYS))
      return CollectionUtils.singletonSet(OperationType.SCHEMA);
    if (!Collections.disjoint(keys, READ_KEYS))
      return CollectionUtils.singletonSet(OperationType.READ);

    // No recognized command key at the top level: cannot classify, assume all write types for safety
    return Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    if (!analyze(query).isIdempotent())
      throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");
    try {
      return mongoDBWrapper.query(query);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Mongo query engine", e);
      throw new CommandParsingException("Error on initializing Mongo query engine", e);
    }
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Object... parameters) {
    if (!analyze(query).isIdempotent())
      throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");
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
