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
package com.arcadedb.postgres;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.DocumentType;

import java.util.List;
import java.util.Map;

public class PostgresPortal {
  public String                    query;
  public String                    language;
  public Statement                 sqlStatement;
  public List<Long>                parameterTypes;
  public List<Integer>             parameterFormats;
  public List<Object>              parameterValues;
  public List<Integer>             resultFormats;
  public List<Result>              cachedResultSet;
  public Map<String, PostgresType> columns;
  public boolean                   ignoreExecution      = false;
  public boolean                   isExpectingResult;
  public boolean                   executed             = false;
  public boolean                   rowDescriptionSent   = false;
  /**
   * True only when a {@code Describe('S')} for this exact statement text already sent the client a real
   * RowDescription (schema-sampled column OIDs, since no row has run yet) - as opposed to {@link #columns}
   * being non-null for some other reason (a catalog answer recomputed fresh on every Bind, or a stale value
   * inherited via {@code bindFrom()} from an already-executed portal picked up by {@code bindCommand()}'s
   * unknown-source-statement fallback). Statement-level Describe is a per-connection contract a client is
   * entitled to rely on for every later Bind+Execute of the same statement with different parameters (issue
   * #6725): once set, {@code executeCommand()} must keep serializing under that exact promised shape instead
   * of silently re-deriving and re-announcing a different one from whatever row this particular Execute
   * happens to return - a schemaless type's undeclared property can hold a different Postgres type per row,
   * and a client that already negotiated binary transfer off the first shape has no way to know the second
   * one ever happened.
   */
  public boolean                   columnsDescribed     = false;
  /**
   * Memoizes {@code PostgresNetworkExecutor.resolveQueryTargetType(sqlStatement)} (issue #6447): a portal can
   * be described and executed - possibly executed repeatedly, for a cursor-based fetch with a LIMIT - several
   * times over its lifetime, and the schema type its FROM target names does not change between them.
   */
  public DocumentType               queryTargetType;
  public boolean                    queryTargetTypeResolved = false;
  /**
   * Memoizes {@code PostgresNetworkExecutor.resolveAliasToSourceProperty(sqlStatement)} (issue #6473), for the
   * same reason and on the same lifecycle as {@link #queryTargetType}.
   */
  public Map<String, String>        aliasToSourceProperty;
  public boolean                    aliasToSourcePropertyResolved = false;
  /**
   * True when the query is about the emulated system catalog and could not be answered at Parse time because
   * its filters are bound parameters, whose values only arrive with the Bind message (issue #6412).
   */
  public boolean                   catalogQuery         = false;
  /**
   * The complete materialized result of this portal's statement (issue #6458), set once - by whichever of a
   * Describe('P') or the first Execute runs the statement first - and read by every Execute after that to
   * hand out {@code limit}-sized slices via {@link #resultCursor}. {@link #cachedResultSet} holds only the
   * current slice (what the in-flight Execute is about to write to the wire), matching what every existing
   * reader of that field already expects; this field is what makes a second slice possible without re-running
   * the statement or losing the rows a Describe already had to materialize to discover the row's columns.
   * <p>
   * <b>Known limitation</b> (tracked as issue #6659): this is eager, in-memory pagination over a fully
   * materialized list, not a true streaming cursor - the whole result is pulled into memory (and retained for
   * the portal's whole lifetime) before the first row goes out, regardless of the client's row-limit. A client
   * that always Describes first (pgjdbc, notably) already got this treatment before #6458, since
   * {@code describeCommand()} has to read every row to discover a sparse document's full column set - but a
   * raw wire-protocol client that skips Describe and relies on Execute's row-limit to bound server-side memory
   * over a large result no longer gets that bound. Real streaming would need the source {@code ResultSet} kept
   * open across Execute calls instead of closed immediately by {@code browseAndCacheResultSet}.
   */
  public List<Result>              fullResultSet;
  /**
   * How many rows of {@link #fullResultSet} have already been handed to the client across every Execute so
   * far (issue #6458). The next Execute's slice starts here.
   */
  public int                       resultCursor         = 0;
  /**
   * True when the most recently computed slice of {@link #fullResultSet} stopped because Execute's row-limit
   * was hit while rows remained - i.e. the wire must send PortalSuspended for that slice, not CommandComplete.
   * The protocol allows exactly one of the two (issue #6458).
   */
  public boolean                   suspended            = false;

  public PostgresPortal(final String query, String language) {
    this.query = query;
    this.language = language;
    this.isExpectingResult = true;
  }

  /**
   * Creates a fresh, independent portal for one Bind of the prepared statement {@code template} (issue
   * #6660 / CodeRabbit review on #6658). PARSE builds one {@code PostgresPortal} per prepared statement and
   * it must stay a read-only template from then on: two portal names bound from the same statement, or the
   * same portal name re-bound without a new Parse (asyncpg's/pgjdbc's statement caching, both), are logically
   * independent portals and must never share mutable per-execution state - sharing it let a Bind on one
   * portal name silently reset or overwrite another already-bound (possibly suspended) portal from the same
   * statement, since both names pointed at the same object.
   * <p>
   * This copies only what PARSE already fixed for the statement for good (query text/language/parameter
   * types, the parsed {@code sqlStatement}, and - for BEGIN/COMMIT/ROLLBACK and a resolved catalog answer -
   * the response PARSE precomputed into {@code executed}/{@code cachedResultSet}/{@code columns}) and leaves
   * every per-Bind field (parameter values, {@code fullResultSet}, {@code resultCursor}, {@code suspended},
   * {@code rowDescriptionSent}, ...) at its fresh default, so each returned portal starts its own independent
   * lifecycle.
   */
  public static PostgresPortal bindFrom(final PostgresPortal template) {
    final PostgresPortal portal = new PostgresPortal(template.query, template.language);
    portal.sqlStatement = template.sqlStatement;
    portal.parameterTypes = template.parameterTypes;
    portal.ignoreExecution = template.ignoreExecution;
    portal.isExpectingResult = template.isExpectingResult;
    portal.catalogQuery = template.catalogQuery;
    portal.executed = template.executed;
    portal.cachedResultSet = template.cachedResultSet;
    portal.columns = template.columns;
    portal.columnsDescribed = template.columnsDescribed;
    portal.queryTargetType = template.queryTargetType;
    portal.queryTargetTypeResolved = template.queryTargetTypeResolved;
    portal.aliasToSourceProperty = template.aliasToSourceProperty;
    portal.aliasToSourcePropertyResolved = template.aliasToSourcePropertyResolved;
    return portal;
  }

  @Override
  public String toString() {
    return query;
  }
}
