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

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Fluent builder for materialized views.
 * <p>
 * The builder varies over {@link BasicDatabase}, not over {@code DatabaseInternal}, so the same body of builder code
 * runs against an embedded database and against {@code RemoteDatabase} (issue #7688, the #7399 shape for views).
 * This class is the embedded implementation: {@link #create()} builds the view in place. A remote schema subclasses
 * it and overrides {@link #create()} to issue {@link #toSQL()} through the server instead.
 */
public class MaterializedViewBuilder {
  /**
   * The units {@code REFRESH EVERY n <unit>} accepts, largest first, paired with their millisecond value. The smallest
   * is SECOND, which is why a refresh interval that is not a whole number of seconds cannot be rendered.
   */
  private static final long[]   SQL_UNIT_MS    = { 3_600_000L, 60_000L, 1_000L };
  private static final String[] SQL_UNIT_NAMES = { "HOUR", "MINUTE", "SECOND" };

  protected final BasicDatabase               database;
  protected       String                      name;
  protected       String                      query;
  protected       MaterializedViewRefreshMode refreshMode     = MaterializedViewRefreshMode.MANUAL;
  protected       int                         buckets         = 0;
  protected       int                         pageSize        = 0;
  protected       long                        refreshInterval = 0;
  protected       boolean                     ifNotExists     = false;

  /**
   * @param database the database the view will be created in. An embedded {@code DatabaseInternal} for this class; a
   *                 subclass may accept any other {@link BasicDatabase}, {@code RemoteDatabase} included.
   */
  public MaterializedViewBuilder(final BasicDatabase database) {
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

  /**
   * The view name accumulated so far, or {@code null} when {@link #withName} has not been called. For subclasses that
   * have to name the view they just created when reading it back.
   */
  public String getName() {
    return name;
  }

  /**
   * The checks that hold wherever the view is created: they are the reason a remote {@code create()} fails on the same
   * builder states an embedded one does, instead of on the server's parse error or not at all.
   */
  protected void validate() {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Materialized view name is required");
    if (name.contains("`"))
      throw new IllegalArgumentException("Materialized view name must not contain backtick characters");
    if (query == null || query.isEmpty())
      throw new IllegalArgumentException("Materialized view query is required");
    if (refreshMode == null)
      throw new IllegalArgumentException("Materialized view refresh mode is required");
    // A negative interval is refused rather than carried: the SQL rendering has no expression for it, so the same
    // builder body would store the caller's negative number embedded and something else remotely.
    if (refreshInterval < 0)
      throw new IllegalArgumentException("Materialized view refresh interval cannot be negative, was " + refreshInterval + "ms");
  }

  /**
   * Renders the accumulated state as the single {@code CREATE MATERIALIZED VIEW} statement that creates the same view,
   * for an implementation that can only reach the schema through {@code command("sql", ...)} (issue #7688).
   * <p>
   * Throws {@link SchemaException} rather than emitting DDL the server would reject or read differently, for every
   * builder state the grammar cannot express: a refresh interval that is not a whole number of seconds (the grammar's
   * smallest unit) or does not fit its integer literal, and a refresh interval on a mode other than
   * {@link MaterializedViewRefreshMode#PERIODIC} (only {@code REFRESH EVERY} carries one).
   * <p>
   * The query is emitted verbatim after {@code AS}. The server parses it and stores its own rendering of the parsed
   * statement - exactly what an embedded {@code CREATE MATERIALIZED VIEW} does - so {@code getQuery()} on a view created
   * this way returns the normalized form, not the caller's spelling.
   */
  public String toSQL() {
    validate();
    if (refreshMode != MaterializedViewRefreshMode.PERIODIC && refreshInterval > 0)
      throw new SchemaException("A refresh interval of " + refreshInterval + "ms on a " + refreshMode
          + " materialized view has no CREATE MATERIALIZED VIEW expression: only REFRESH EVERY (PERIODIC) carries an interval");

    final StringBuilder sql = new StringBuilder(64 + query.length());
    sql.append("CREATE MATERIALIZED VIEW ");
    if (ifNotExists)
      sql.append("IF NOT EXISTS ");
    sql.append(Identifier.quote(name)).append(" AS ");
    appendQuery(sql, query);

    switch (refreshMode) {
    case MANUAL -> sql.append(" REFRESH MANUAL");
    case INCREMENTAL -> sql.append(" REFRESH INCREMENTAL");
    case PERIODIC -> sql.append(" REFRESH EVERY ").append(renderInterval(refreshInterval));
    }

    if (buckets > 0)
      sql.append(" BUCKETS ").append(buckets);
    if (pageSize > 0)
      sql.append(" PAGESIZE ").append(pageSize);

    return sql.toString();
  }

  /**
   * Appends the query so the clauses rendered after it cannot be swallowed by it (code review on PR #8727). A trailing
   * {@code ;} is dropped, since the statement would otherwise end before {@code REFRESH}; and a query carrying a
   * {@code --} line comment is terminated with a newline, since the comment would otherwise run to the end of the
   * statement and silently drop every clause after it - creating a MANUAL view with default buckets instead of failing.
   * Shared with {@link ContinuousAggregateBuilder#toSQL()} so the two render a query the same way.
   */
  static void appendQuery(final StringBuilder sql, final String query) {
    int end = query.length();
    while (end > 0 && (Character.isWhitespace(query.charAt(end - 1)) || query.charAt(end - 1) == ';'))
      --end;
    sql.append(query, 0, end);
    if (query.lastIndexOf("--", end) >= 0)
      sql.append('\n');
  }

  /**
   * {@code <count> <unit>} using the largest of HOUR/MINUTE/SECOND that divides the interval exactly. Zero renders as
   * {@code 0 SECOND}, which the statement reads as PERIODIC with no schedule - what an embedded {@code create()} with a
   * zero interval builds.
   */
  private static String renderInterval(final long millis) {
    if (millis == 0)
      return "0 SECOND";
    for (int i = 0; i < SQL_UNIT_MS.length; i++)
      if (millis % SQL_UNIT_MS[i] == 0) {
        final long count = millis / SQL_UNIT_MS[i];
        if (count > Integer.MAX_VALUE)
          break;
        return count + " " + SQL_UNIT_NAMES[i];
      }
    throw new SchemaException("A refresh interval of " + millis
        + "ms has no CREATE MATERIALIZED VIEW expression: the value must be a whole number of seconds and fit REFRESH EVERY's integer literal");
  }

  /**
   * Creates the view and returns it.
   * <p>
   * This implementation creates it in place, in the embedded database the builder was constructed with. A remote
   * schema returns a subclass that overrides this to issue {@link #toSQL()} through the server.
   */
  public MaterializedView create() {
    validate();

    if (!(database instanceof DatabaseInternal databaseInternal))
      throw new SchemaException("Cannot create the materialized view '" + name + "' in place: "
          + database.getClass().getSimpleName() + " is not an embedded database. Use the builder returned by its own Schema");

    final LocalSchema schema = (LocalSchema) databaseInternal.getSchema();

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
    final List<String> sourceTypeNames = extractSourceTypes(databaseInternal, query);

    // Validate source types exist
    for (final String srcType : sourceTypeNames)
      if (!schema.existsType(srcType))
        throw new SchemaException("Source type '" + srcType + "' referenced in materialized view query does not exist");

    // Classify query complexity
    final boolean simple = MaterializedViewQueryClassifier.isSimple(query, databaseInternal);

    // Wrap in recordFileChanges so that all schema mutations (backing type creation,
    // MV metadata registration, and initial data population) are replicated atomically
    // to HA replicas as a single schema change
    return schema.recordFileChanges(() -> {
      // Create the backing document type (schema-less)
      final TypeBuilder<?> typeBuilder = schema.buildDocumentType().withName(name);
      if (buckets > 0)
        typeBuilder.withTotalBuckets(buckets);
      if (pageSize > 0)
        typeBuilder.withPageSize(pageSize);
      typeBuilder.create();

      // Create and register the materialized view; persist BUILDING status before the
      // initial refresh so crash recovery in readConfiguration() can detect an incomplete
      // population and mark the view STALE on restart
      final MaterializedViewImpl view = new MaterializedViewImpl(
          databaseInternal, name, query, name, sourceTypeNames,
          refreshMode, simple, refreshInterval);
      view.setStatus(MaterializedViewStatus.BUILDING);
      synchronized (schema) {
        schema.materializedViews.put(name, view);
      }
      schema.saveConfiguration();

      // Perform initial full refresh; on failure clean up the orphaned view and backing type
      try {
        MaterializedViewRefresher.fullRefresh(databaseInternal, view);
      } catch (final Exception e) {
        // Remove the MV entry first (so dropType's backing-type guard won't block)
        synchronized (schema) {
          schema.materializedViews.remove(name);
        }
        try {
          schema.dropType(name);
        } catch (final Exception dropEx) {
          LogManager.instance().log(MaterializedViewBuilder.class, Level.WARNING,
              "Failed to clean up backing type '%s' after materialized view creation failure: %s",
              dropEx, name, dropEx.getMessage());
        }
        throw e;
      }
      schema.saveConfiguration();

      // Register event listeners for INCREMENTAL mode
      if (refreshMode == MaterializedViewRefreshMode.INCREMENTAL)
        registerListeners(schema, view, sourceTypeNames);

      if (refreshMode == MaterializedViewRefreshMode.PERIODIC && refreshInterval > 0)
        schema.getMaterializedViewScheduler().schedule(databaseInternal, view);

      return view;
    });
  }

  static void registerListeners(final LocalSchema schema, final MaterializedViewImpl view,
      final List<String> sourceTypeNames) {
    final MaterializedViewChangeListener listener =
        new MaterializedViewChangeListener((DatabaseInternal) schema.getDatabase(), view);
    view.setChangeListener(listener);
    for (final String srcType : sourceTypeNames) {
      final DocumentType type = schema.getType(srcType);
      type.getEvents().registerListener((AfterRecordCreateListener) listener);
      type.getEvents().registerListener((AfterRecordUpdateListener) listener);
      type.getEvents().registerListener((AfterRecordDeleteListener) listener);
    }
  }

  static void unregisterListeners(final LocalSchema schema, final MaterializedViewImpl view) {
    final MaterializedViewChangeListener listener = view.getChangeListener();
    if (listener == null)
      return;
    for (final String srcType : view.getSourceTypeNames()) {
      if (!schema.existsType(srcType))
        continue;
      final DocumentType type = schema.getType(srcType);
      type.getEvents().unregisterListener((AfterRecordCreateListener) listener);
      type.getEvents().unregisterListener((AfterRecordUpdateListener) listener);
      type.getEvents().unregisterListener((AfterRecordDeleteListener) listener);
    }
    view.setChangeListener(null);
  }

  private static List<String> extractSourceTypes(final DatabaseInternal database, final String sql) {
    final List<String> types = new ArrayList<>();
    final Statement parsed = database.getStatementCache().get(sql);
    if (parsed instanceof SelectStatement select) {
      final FromClause from = select.getTarget();
      if (from != null) {
        final FromItem item = from.getItem();
        if (item != null && item.getIdentifier() != null)
          types.add(item.getIdentifier().getStringValue());
      }
    }
    return types;
  }
}
