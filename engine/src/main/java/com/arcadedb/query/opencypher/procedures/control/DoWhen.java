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
package com.arcadedb.query.opencypher.procedures.control;

import com.arcadedb.database.Database;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: do.when(condition, ifQuery, elseQuery, params)
 * <p>
 * Runs {@code ifQuery} when {@code condition} is true, {@code elseQuery} otherwise, both given as
 * Cypher query strings, with {@code params} bound into the sub-query as named parameters. Each row
 * the sub-query produces is yielded as a map under {@code value}. The sub-query is dispatched to
 * {@link Database#query} or {@link Database#command} depending on whether it is read-only, so both
 * read and write sub-queries work without the caller having to say which.
 * </p>
 * <p>
 * {@code condition} must be a literal {@code Boolean} - unlike real APOC, which coerces other
 * truthy/falsy values. Cypher boolean expressions (comparisons, {@code AND}/{@code OR}, {@code IS
 * NULL}, ...) already evaluate to {@code Boolean}, so this only rejects a caller passing something
 * else entirely (a string, a number, a list), which is intentional: a coerced non-boolean would
 * silently pick a branch rather than surfacing the caller's mistake.
 * </p>
 * <p>
 * {@code ifQuery} is type-checked unconditionally, even on a call where {@code condition} is false and
 * {@code ifQuery} never runs - a malformed call fails the same way regardless of which branch is taken,
 * rather than only failing once someone happens to trigger the true branch. {@code elseQuery} differs
 * only in that {@code null} is accepted, meaning "no else branch": the call yields zero rows rather than
 * running anything.
 * </p>
 * <p>
 * {@code ifQuery}/{@code elseQuery} run as Cypher against this database with whatever privileges the
 * caller already has - same as real APOC's {@code apoc.do.when}/{@code apoc.cypher.doIt}, and no more
 * of a new risk than any other place in the engine that runs a caller-supplied command string. As with
 * those, never build either argument by concatenating untrusted input.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL apoc.do.when(size($list) > 0, 'RETURN $list[0] AS first', 'RETURN null AS first', {list: $list})
 * YIELD value
 * RETURN value.first
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DoWhen implements CypherProcedure {
  public static final String NAME = "do.when";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Runs ifQuery when condition is true, elseQuery otherwise, binding params into the sub-query.";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("value");
  }

  @Override
  public boolean isWriteProcedure() {
    return true;
  }

  /**
   * Classifies one specific {@code CALL apoc.do.when(...)} site from the branch queries it was written with.
   * {@link #isWriteProcedure()} has to answer "writes" for every call - at registration time the branch strings
   * do not exist yet - but at a call site that spells them out as literals the answer is knowable, and the call
   * is a write only if a branch it could actually run is one.
   * <p>
   * Both branches are weighed, never only the one the condition selects: {@code condition} is an arbitrary
   * expression evaluated per row, so which branch runs is not a parse-time fact even where it happens to be
   * written here as a literal.
   * <p>
   * A call that cannot get as far as running a branch cannot write, so it is not classified as a write: a wrong
   * argument count, or a branch argument that is a literal of the wrong type, is rejected by {@link #execute}'s
   * own checks before anything runs. Answering "writes" for those would replace their actionable error
   * (<em>"ifQuery must be a string"</em>) with {@code QueryNotIdempotentException} for every caller using
   * {@link Database#query}, which is a worse answer to the same mistake and tells the caller nothing.
   * <p>
   * What stays conservative is genuine ignorance: a branch supplied as {@code $param} (or a literal
   * {@code null}, which is indistinguishable here), and a branch string this method cannot parse. The latter is
   * conservative rather than "cannot run" on purpose - {@code OpenCypherQueryEngine} strips an
   * {@code EXPLAIN}/{@code PROFILE} prefix before parsing and {@code PROFILE} does execute, so a
   * string that fails a bare parse here is not proof that nothing runs. A wrong "read-only" would route the
   * statement to the raw database instance on HA and let {@link Database#query} run it, which is exactly the bug
   * this classification exists to prevent (issue #6094).
   */
  @Override
  public boolean isWriteProcedure(final Object[] literalArguments) {
    // Rejected by validateArgs() at execution before a branch can run; let that error surface.
    if (literalArguments.length < getMinArgs() || literalArguments.length > getMaxArgs())
      return false;
    return branchMayWrite(literalArguments[1]) || branchMayWrite(literalArguments[2]);
  }

  /**
   * Whether a branch argument could run something that writes. Only two answers are "yes": a query string that
   * parses into a non-read-only statement, and an argument the parser could not resolve to a literal at all.
   * A blank branch runs nothing ({@link #execute} returns an empty stream for it) and a non-string literal is
   * rejected by {@code extractString} before any branch runs.
   * <p>
   * The parse here is deliberately recursive and it terminates. A branch string may itself contain
   * {@code CALL apoc.do.when(...)}, so this reaches {@code SimpleCypherStatement}'s constructor, its
   * {@code anyWriteProcedureCall}, and back into this method. Each step strictly consumes one level of the
   * literal nesting written into the query text, which is finite and in practice shallow - every level has to
   * escape the quoting of the level around it, so the text grows exponentially in the depth. The parser used is
   * a fresh instance and holds no state shared with the parse in progress around it.
   */
  private static boolean branchMayWrite(final Object branchQuery) {
    if (branchQuery == null)
      // Dynamic, or a literal null - the two are indistinguishable here, so assume the worst.
      return true;
    if (!(branchQuery instanceof String query))
      return false;
    if (query.isBlank())
      return false;
    try {
      return !new Cypher25AntlrParser().parse(query).isReadOnly();
    } catch (final Exception e) {
      // Unparseable as written. Not proof that nothing runs (see the EXPLAIN/PROFILE note above), so assume it writes.
      return true;
    }
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final boolean condition = extractBoolean(args[0]);
    final String ifQuery = extractString(args[1], "ifQuery");
    final String elseQuery = args[2] == null ? "" : extractString(args[2], "elseQuery");
    final Map<String, Object> params = extractMap(args[3]);

    final String query = condition ? ifQuery : elseQuery;
    if (query == null || query.isBlank())
      return Stream.empty();

    final Database database = context.getDatabase();
    final QueryEngine queryEngine = database.getQueryEngine("opencypher");
    final boolean idempotent = queryEngine.analyze(query).isIdempotent();

    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = idempotent ?
        database.query("opencypher", query, params) :
        database.command("opencypher", query, params)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final ResultInternal wrapped = new ResultInternal();
        wrapped.setProperty("value", row.toMap());
        rows.add(wrapped);
      }
    }

    return rows.stream();
  }

  private boolean extractBoolean(final Object arg) {
    if (arg instanceof Boolean b)
      return b;
    throw new IllegalArgumentException(getName() + "(): condition must be a boolean, got " +
        (arg == null ? "null" : arg.getClass().getSimpleName()));
  }

  private String extractString(final Object arg, final String paramName) {
    if (!(arg instanceof String s))
      throw new IllegalArgumentException(getName() + "(): " + paramName + " must be a string, got " +
          (arg == null ? "null" : arg.getClass().getSimpleName()));
    return s;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> extractMap(final Object arg) {
    if (arg == null)
      return Collections.emptyMap();
    if (!(arg instanceof Map))
      throw new IllegalArgumentException(getName() + "(): params must be a map, got " + arg.getClass().getSimpleName());
    return (Map<String, Object>) arg;
  }
}
