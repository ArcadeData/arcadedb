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
package com.arcadedb.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Map;

public interface QueryEngine {
  interface AnalyzedQuery {
    boolean isIdempotent();

    boolean isDDL();
  }

  interface QueryEngineFactory {
    boolean isAvailable();

    String getLanguage();

    QueryEngine getInstance(DatabaseInternal database);
  }

  AnalyzedQuery analyze(String query);

  ResultSet query(String query, Map<String, Object> parameters);

  ResultSet query(String query, Object... parameters);

  ResultSet command(String query, Map<String, Object> parameters);

  ResultSet command(String query, Object... parameters);
}
