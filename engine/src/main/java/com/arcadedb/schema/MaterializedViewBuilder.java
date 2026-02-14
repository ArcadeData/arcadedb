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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;

import java.util.ArrayList;
import java.util.List;

public class MaterializedViewBuilder {
  private final DatabaseInternal database;
  private String name;
  private String query;
  private MaterializedViewRefreshMode refreshMode = MaterializedViewRefreshMode.MANUAL;
  private int buckets = 0;
  private int pageSize = 0;
  private long refreshInterval = 0;
  private boolean ifNotExists = false;

  public MaterializedViewBuilder(final DatabaseInternal database) {
    this.database = database;
  }

  public MaterializedViewBuilder withName(final String name) {
    this.name = name;
    return this;
  }

  public MaterializedViewBuilder withQuery(final String query) {
    this.query = query;
    return this;
  }

  public MaterializedViewBuilder withRefreshMode(final MaterializedViewRefreshMode mode) {
    this.refreshMode = mode;
    return this;
  }

  public MaterializedViewBuilder withTotalBuckets(final int buckets) {
    this.buckets = buckets;
    return this;
  }

  public MaterializedViewBuilder withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  public MaterializedViewBuilder withRefreshInterval(final long intervalMs) {
    this.refreshInterval = intervalMs;
    return this;
  }

  public MaterializedViewBuilder withIgnoreIfExists(final boolean ignore) {
    this.ifNotExists = ignore;
    return this;
  }

  public MaterializedView create() {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Materialized view name is required");
    if (query == null || query.isEmpty())
      throw new IllegalArgumentException("Materialized view query is required");

    final LocalSchema schema = (LocalSchema) database.getSchema();

    // Check if view already exists
    if (schema.existsMaterializedView(name)) {
      if (ifNotExists)
        return schema.getMaterializedView(name);
      throw new SchemaException("Materialized view '" + name + "' already exists");
    }

    // Check if a type with this name already exists (backing type would conflict)
    if (schema.existsType(name))
      throw new SchemaException("Cannot create materialized view '" + name +
          "': a type with the same name already exists");

    // Parse the query to validate syntax and extract source types
    final List<String> sourceTypeNames = extractSourceTypes(query);

    // Validate source types exist
    for (final String srcType : sourceTypeNames)
      if (!schema.existsType(srcType))
        throw new SchemaException("Source type '" + srcType + "' referenced in materialized view query does not exist");

    // Classify query complexity
    final boolean simple = MaterializedViewQueryClassifier.isSimple(query, database);

    // Create the backing document type (schema-less)
    final TypeBuilder<?> typeBuilder = schema.buildDocumentType().withName(name);
    if (buckets > 0)
      typeBuilder.withTotalBuckets(buckets);
    if (pageSize > 0)
      typeBuilder.withPageSize(pageSize);
    typeBuilder.create();

    // Create and register the materialized view
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, name, query, name, sourceTypeNames,
        refreshMode, simple, refreshInterval);
    schema.materializedViews.put(name, view);
    schema.saveConfiguration();

    // Perform initial full refresh
    MaterializedViewRefresher.fullRefresh(database, view);
    schema.saveConfiguration();

    return view;
  }

  private List<String> extractSourceTypes(final String sql) {
    // Parse the SQL and extract type names from the FROM clause
    final List<String> types = new ArrayList<>();
    final String upper = sql.toUpperCase();
    int fromIdx = upper.indexOf("FROM ");
    while (fromIdx >= 0) {
      final int start = fromIdx + 5;
      int i = start;
      while (i < sql.length() && sql.charAt(i) == ' ')
        i++;
      final int nameStart = i;
      while (i < sql.length() && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_'))
        i++;
      if (i > nameStart) {
        final String typeName = sql.substring(nameStart, i);
        if (!typeName.isEmpty() && !types.contains(typeName))
          types.add(typeName);
      }
      fromIdx = upper.indexOf("FROM ", i);
    }
    return types;
  }
}
