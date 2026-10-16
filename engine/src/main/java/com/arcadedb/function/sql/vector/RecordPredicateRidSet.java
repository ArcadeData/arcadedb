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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.QueryOperatorEquals;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The allow-list of the {@code where} option of {@code vector.neighbors} / {@code vector.sparseNeighbors} (issue #10169): the
 * RIDs whose record satisfies a predicate, decided one candidate at a time as the search asks about it. Restricting a search by
 * a property no longer means materializing the set of every RID in scope up front (a {@code filter} subquery): only the
 * candidates the search actually considers are loaded and tested, each once per call.
 * <p>
 * The predicate is either a map of property to value, every entry of which must hold with the SQL {@code =} semantics
 * ({@code where: { embed_space: :space }}), or a SQL condition evaluated on the record ({@code where: "year >= :minYear"}),
 * whose parameters must be named: a positional {@code ?} is refused, since the condition is parsed on its own and could not tell
 * which of the query's positional values it meant. A
 * {@code filter} given as well restricts the candidates further, and is checked first, so a candidate outside it is never
 * loaded.
 * <p>
 * The indexes consume an allow-list through {@link #contains} and {@link #isEmpty}, and enumerate it only when it is small
 * enough to scan its members instead of the graph. This set cannot be enumerated without the {@code filter}: its size is then
 * reported as unbounded, which never qualifies for that plan, and {@link #iterator()} refuses. With a {@code filter} its size is
 * the filter's, an upper bound, and enumerating it tests each member of the filter.
 * <p>
 * Safe for the concurrent searches of a sparse fan-out: the memo is concurrent and the records are loaded outside any lock,
 * so a slow load holds no other worker back. Only a SQL condition is evaluated under this set's lock, because it evaluates in
 * the caller's {@link CommandContext}, which is not meant to be shared between threads. Two workers meeting the same candidate
 * may both test it; the answer is the same. The memo holds one entry per candidate the search met, which the search itself
 * bounds (its beam, its fetch budget), and lives as long as the call.
 * <p>
 * The cost follows the predicate's selectivity. A predicate that keeps a fair share of the records is met within the graph walk,
 * after testing a few candidates per result. One that keeps few or none leaves the walk short of {@code k}, and the dense index
 * then completes the answer by testing every indexed vector, each record once: an exact answer, at the cost of the full load a
 * {@code filter} subquery pays on every call anyway. Capping that walk would answer fewer rows than exist, so it is not capped;
 * for a small, known set of RIDs the {@code filter} option remains the plan that scans only them.
 * <p>
 * Records are read with the caller's permissions, as every other read of the search: a candidate the caller may not read is
 * not accepted, so the predicate can neither reveal it nor fail the search over it.
 * <p>
 * Only {@link #contains}, {@link #isEmpty}, {@link #size} and, with a {@code filter}, {@link #iterator} are meaningful.
 * {@link #equals}, {@link #hashCode} and {@link #toString} are identity-based and never enumerate the set.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RecordPredicateRidSet extends AbstractSet<RID> {
  private final BasicDatabase                   database;
  private final CommandContext                  context;
  private final Map<String, Object>             equalities;
  private final WhereClause                     condition;
  private final Set<RID>                        restrictTo;
  private final ConcurrentHashMap<RID, Boolean> decided = new ConcurrentHashMap<>();

  private RecordPredicateRidSet(final CommandContext context, final Map<String, Object> equalities, final WhereClause condition,
      final Set<RID> restrictTo) {
    this.database = context.getDatabase();
    this.context = context;
    this.equalities = equalities;
    this.condition = condition;
    this.restrictTo = restrictTo;
  }

  /**
   * Builds the allow-list of a {@code where} option, or answers {@code filter} as it is when there is none.
   *
   * @param where        the option's value: a map of property to value, or a SQL condition; null when the option is absent
   * @param filter       the RIDs of the {@code filter} option, or null when it is absent
   * @param functionName the function the option belongs to, for the error messages
   */
  static Set<RID> of(final Object where, final Set<RID> filter, final String functionName, final CommandContext context) {
    if (where == null)
      return filter;
    if (filter != null && filter.isEmpty())
      // nothing can satisfy the filter: there is nothing to test
      return filter;

    if (where instanceof Map<?, ?> map) {
      if (map.isEmpty())
        throw new CommandSQLParsingException(functionName + " 'where' must name at least one property");
      final LinkedHashMap<String, Object> equalities = new LinkedHashMap<>(map.size());
      for (final Map.Entry<?, ?> e : map.entrySet())
        equalities.put(String.valueOf(e.getKey()), e.getValue());
      return new RecordPredicateRidSet(context, equalities, null, filter);
    }
    if (where instanceof String text) {
      if (text.isBlank())
        throw new CommandSQLParsingException(functionName + " 'where' condition is empty");
      // The condition is parsed on its own, so a `?` in it would be numbered from the first positional parameter of the
      // QUERY and read the wrong value without any error: only named parameters bind to what the caller meant
      if (hasPositionalParameter(text))
        throw new CommandSQLParsingException(functionName + " 'where' condition cannot use positional parameters ('?'): use named "
            + "parameters such as ':space', or pass a map of property to value");
      final WhereClause condition;
      try {
        condition = new SQLAntlrParser(context.getDatabase()).parseCondition(text);
      } catch (final CommandSQLParsingException e) {
        throw e;
      } catch (final Exception e) {
        throw new CommandSQLParsingException(functionName + " 'where' is not a valid condition: " + text, e);
      }
      return new RecordPredicateRidSet(context, null, condition, filter);
    }
    throw new CommandSQLParsingException(
        functionName + " 'where' must be a map of property to value or a SQL condition, got " + where.getClass().getSimpleName());
  }

  /**
   * Whether {@code condition} holds a {@code ?} outside a string literal or a quoted identifier.
   */
  static boolean hasPositionalParameter(final String condition) {
    char quote = 0;
    for (int i = 0; i < condition.length(); i++) {
      final char c = condition.charAt(i);
      if (quote != 0) {
        if (c == '\\')
          ++i; // an escaped character never closes the literal
        else if (c == quote)
          quote = 0;
      } else if (c == '\'' || c == '"' || c == '`')
        quote = c;
      else if (c == '?')
        return true;
    }
    return false;
  }

  @Override
  public boolean contains(final Object o) {
    if (!(o instanceof RID rid))
      return false;
    if (restrictTo != null && !restrictTo.contains(rid))
      return false;
    final Boolean known = decided.get(rid);
    if (known != null)
      return known;
    final boolean accepted = test(rid);
    decided.putIfAbsent(rid, accepted);
    return accepted;
  }

  private boolean test(final RID rid) {
    final Document record;
    try {
      // A RID of an internal record (an edge segment, ...) has no properties to test
      if (!(database.lookupByRID(rid, true) instanceof Document document))
        return false;
      record = document;
    } catch (final RecordNotFoundException | SecurityException e) {
      // gone, or not readable by the caller: not a candidate the caller may be answered
      return false;
    }
    if (condition != null)
      synchronized (this) {
        return Boolean.TRUE.equals(condition.matchesFilters(record, context));
      }
    for (final Map.Entry<String, Object> e : equalities.entrySet())
      if (!QueryOperatorEquals.equals(SQLFunctionVectorAbstract.readNestedField(record, e.getKey()), e.getValue()))
        return false;
    return true;
  }

  @Override
  public boolean equals(final Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public String toString() {
    return "where(" + (condition != null ? condition : equalities) + (restrictTo != null ? ", filter of " + restrictTo.size() : "")
        + ", " + decided.size() + " decided)";
  }

  /** Never empty: an empty allow-list reads as "no filter" to the indexes. */
  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public int size() {
    return restrictTo != null ? restrictTo.size() : Integer.MAX_VALUE;
  }

  @Override
  public Iterator<RID> iterator() {
    if (restrictTo == null)
      throw new UnsupportedOperationException("The RIDs a 'where' predicate accepts are decided one candidate at a time and cannot be listed");
    final Iterator<RID> members = restrictTo.iterator();
    return new Iterator<>() {
      private RID next = advance();

      private RID advance() {
        while (members.hasNext()) {
          final RID candidate = members.next();
          if (contains(candidate))
            return candidate;
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public RID next() {
        if (next == null)
          throw new NoSuchElementException();
        final RID current = next;
        next = advance();
        return current;
      }
    };
  }
}
