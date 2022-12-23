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

import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.mongo.MongoDBDatabaseWrapper;
import com.arcadedb.query.QueryEngine;
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
    try {
      return mongoDBWrapper.query(query);
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Mongo query engine", e);
      throw new QueryParsingException("Error on initializing Mongo query engine", e);
    }
  }

  @Override
  public ResultSet query(final String query, final Object... parameters) {
    return query(query, (Map) null);
  }

  @Override
  public ResultSet command(final String query, final Map<String, Object> parameters) {
    return null;
  }

  @Override
  public ResultSet command(String query, Object... parameters) {
    return null;
  }
}
