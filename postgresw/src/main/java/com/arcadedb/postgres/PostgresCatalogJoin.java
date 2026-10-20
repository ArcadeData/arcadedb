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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A nested-loop join over the rows of the relations a catalog query names (issue #10473).
 * <p>
 * The catalog's usual row model is one row per thing the query is about, carrying the columns of every relation that thing can be
 * joined to. That cannot represent a statement that reads one relation <b>twice</b> - psql's {@code \d} reads {@code pg_class} as
 * the table and again as each of its indexes, and psqlodbc reads {@code pg_attribute} as a table column and as an index column -
 * because the two aliases are two different rows. Here each FROM entry has its own rows and the statement's conditions pair them up.
 * <p>
 * The entries are bound in FROM order. A condition is evaluated as soon as the last entry it reads is bound, so a row that
 * fails it is dropped before the entries after it are tried; and when a condition equates a column of the entry being bound
 * with something the entries already bound decide - {@code c.oid = i.indrelid} - the rows are looked up by that column instead of
 * scanned, so a table's indexes are found without reading every index of the database.
 * <p>
 * An outer join keeps the row on its left when nothing on its right matches its ON condition, and reads NULL from the right then.
 * That ON condition only decides the match: a condition of the WHERE clause on the same entry is applied afterwards, to the row
 * the outer join produced, as SQL says.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresCatalogJoin {
  /**
   * What keeping one binding costs the statement, in the units its budget counts: a binding holds its rows and flags until the
   * statement is projected, so a join of large relations that nothing narrows (a cross join) runs out of budget long before it
   * fills the heap.
   */
  private static final int BINDING_COST = 8;

  private final Source[]                               sources;
  private final List<List<PostgresCatalogExpression>>  conditionsAtLevel;
  private final boolean                                tolerant;
  private final Evaluator                              evaluator;
  private final Budget                                 budget;
  private final Map<String, Object>[]                  bound;
  private final boolean[]                              nulled;
  private final List<Binding>                          bindings = new ArrayList<>();
  private final Map<Integer, Map<String, ColumnIndex>> indexes  = new HashMap<>();

  /** The statement cannot be answered honestly: a condition it depends on could not be evaluated. */
  static final class Declined extends RuntimeException {
    Declined() {
      super(null, null, false, false);
    }
  }

  /** Evaluates an expression against the entries bound so far; an entry not bound yet reads {@code UNKNOWN}. */
  interface Evaluator {
    Object evaluate(PostgresCatalogExpression expression, Map<String, Object>[] bound, boolean[] nulled);
  }

  /** Charges the work a statement spends, and stops it when it has spent what it may. */
  interface Budget {
    void charge(int units);
  }

  /** A column of an entry that a condition equates with an expression the entries before it decide. */
  record Access(String column, PostgresCatalogExpression key) {
  }

  /**
   * The rows one FROM entry can be bound to.
   *
   * @param nullRow  what the entry reads when an outer join matched nothing: the same columns, all NULL
   * @param outer    whether the entry is the right side of a LEFT JOIN
   * @param matching an outer join's own ON conditions, which only decide whether a row matches
   * @param accesses the equalities that find the rows of the entry by one of its columns: those of {@code matching} for an outer
   *                 join, any condition of the statement otherwise
   */
  record Source(List<Map<String, Object>> rows, Map<String, Object> nullRow, boolean outer,
                List<PostgresCatalogExpression> matching, List<Access> accesses) {
  }

  /** One way of binding every entry: the row of each, and which entries an outer join left without one. */
  record Binding(Map<String, Object>[] rows, boolean[] nulled) {
  }

  private PostgresCatalogJoin(final Source[] sources, final List<List<PostgresCatalogExpression>> conditionsAtLevel,
      final boolean tolerant, final Evaluator evaluator, final Budget budget) {
    this.sources = sources;
    this.conditionsAtLevel = conditionsAtLevel;
    this.tolerant = tolerant;
    this.evaluator = evaluator;
    this.budget = budget;
    this.bound = newBound(sources.length);
    this.nulled = new boolean[sources.length];
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object>[] newBound(final int size) {
    return new Map[size];
  }

  /**
   * Binds the entries of a statement.
   *
   * @param conditionsAtLevel for each entry, the conditions whose last entry is that one: the WHERE clause's, and the ON conditions
   *                          of the inner joins
   * @param tolerant          whether a condition that cannot be evaluated keeps the row, as the catalog's own rows do, rather than
   *                          declining the statement
   *
   * @throws Declined when a condition could not be evaluated and the rows do not tolerate that
   */
  static List<Binding> join(final Source[] sources, final List<List<PostgresCatalogExpression>> conditionsAtLevel,
      final boolean tolerant, final Evaluator evaluator, final Budget budget) {
    final PostgresCatalogJoin join = new PostgresCatalogJoin(sources, conditionsAtLevel, tolerant, evaluator, budget);
    join.bind(0);
    return join.bindings;
  }

  private void bind(final int level) {
    if (level == sources.length) {
      budget.charge(BINDING_COST);
      bindings.add(new Binding(bound.clone(), nulled.clone()));
      return;
    }

    final Source source = sources[level];
    final List<Map<String, Object>> candidates = candidates(level, source);

    final List<Map<String, Object>> matches = new ArrayList<>();
    for (final Map<String, Object> row : candidates) {
      budget.charge(1);
      if (source.matching().isEmpty()) {
        matches.add(row);
        continue;
      }
      bound[level] = row;
      nulled[level] = false;
      if (holds(source.matching()))
        matches.add(row);
    }

    final boolean unmatched = matches.isEmpty() && source.outer();
    if (unmatched)
      matches.add(source.nullRow());

    for (final Map<String, Object> row : matches) {
      bound[level] = row;
      nulled[level] = unmatched;
      if (holds(conditionsAtLevel.get(level)))
        bind(level + 1);
    }
    bound[level] = null;
    nulled[level] = false;
  }

  /** Whether every condition holds: FALSE or NULL drops the row, a condition that cannot be evaluated keeps it when tolerated. */
  private boolean holds(final List<PostgresCatalogExpression> conditions) {
    for (final PostgresCatalogExpression condition : conditions) {
      final Object value = evaluator.evaluate(condition, bound, nulled);
      if (value == PostgresCatalogExpression.UNKNOWN) {
        if (!tolerant)
          throw new Declined();
      } else if (!PostgresCatalogExpression.isTrue(value))
        return false;
    }
    return true;
  }

  /** The rows of an entry worth trying: those an equality finds by the value the bound entries decide, else every row. */
  private List<Map<String, Object>> candidates(final int level, final Source source) {
    for (final Access access : source.accesses()) {
      final Object key = evaluator.evaluate(access.key(), bound, nulled);
      if (key == PostgresCatalogExpression.UNKNOWN)
        continue;
      if (key == null)
        // nothing equals NULL
        return List.of();

      final Map<String, ColumnIndex> byColumn = indexes.computeIfAbsent(level, l -> new HashMap<>());
      ColumnIndex index = byColumn.get(access.column());
      if (index == null) {
        budget.charge(source.rows().size());
        index = new ColumnIndex(source.rows(), access.column());
        byColumn.put(access.column(), index);
      }
      final List<Map<String, Object>> found = index.lookup(key);
      if (found != null)
        return found;
    }
    return source.rows();
  }

  /**
   * The rows of an entry grouped by the value of one column, to find the rows an equality names without reading them all.
   * <p>
   * A lookup returns every row the catalog's own {@code =} could call equal to the key, and only ever more: the condition is
   * evaluated on what it returns anyway. A column whose values are of more than one kind, or of a kind with no exact key (a
   * fraction), is not indexed, and neither is a key of a kind the column's does not compare with by an exact key; the entry is then
   * scanned.
   */
  private static final class ColumnIndex {
    private enum Kind {
      INTEGRAL, TEXT, BOOLEAN, UNUSABLE
    }

    private final Kind                                 kind;
    private final Map<Object, List<Map<String, Object>>> byKey = new HashMap<>();

    ColumnIndex(final List<Map<String, Object>> rows, final String column) {
      Kind kind = null;
      for (final Map<String, Object> row : rows) {
        final Object value = row.get(column);
        if (value == null)
          continue;
        final Kind valueKind = kindOf(value);
        if (kind == null)
          kind = valueKind;
        else if (kind != valueKind)
          kind = Kind.UNUSABLE;
        if (kind == Kind.UNUSABLE)
          break;
      }
      this.kind = kind == null ? Kind.INTEGRAL : kind;

      if (this.kind != Kind.UNUSABLE)
        for (final Map<String, Object> row : rows) {
          final Object value = row.get(column);
          if (value != null)
            byKey.computeIfAbsent(keyOf(value), k -> new ArrayList<>()).add(row);
        }
    }

    private static Kind kindOf(final Object value) {
      if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte)
        return Kind.INTEGRAL;
      if (value instanceof String)
        return Kind.TEXT;
      if (value instanceof Boolean)
        return Kind.BOOLEAN;
      return Kind.UNUSABLE;
    }

    private Object keyOf(final Object value) {
      return kind == Kind.INTEGRAL ? (Object) ((Number) value).longValue() : value;
    }

    /** @return the rows equal to {@code key}, null when this index cannot tell and the caller must scan */
    List<Map<String, Object>> lookup(final Object key) {
      final Object exact;
      switch (kind) {
      case INTEGRAL:
        if (key instanceof Integer || key instanceof Long || key instanceof Short || key instanceof Byte)
          exact = ((Number) key).longValue();
        else if (key instanceof Double || key instanceof Float) {
          final double d = ((Number) key).doubleValue();
          if (d != Math.rint(d) || Double.isInfinite(d))
            return List.of();
          exact = (long) d;
        } else if (key instanceof String text) {
          try {
            exact = Long.parseLong(text.trim());
          } catch (final NumberFormatException e) {
            // text that is no integer is not equal to one
            return List.of();
          }
        } else
          return null;
        break;
      case TEXT:
        if (key instanceof String)
          exact = key;
        else if (key instanceof Integer || key instanceof Long || key instanceof Short || key instanceof Byte)
          exact = String.valueOf(key);
        else
          return null;
        break;
      case BOOLEAN:
        if (key instanceof Boolean || key instanceof String)
          exact = PostgresCatalogExpression.truth(key);
        else
          return null;
        break;
      default:
        return null;
      }
      final List<Map<String, Object>> rows = byKey.get(exact);
      return rows == null ? List.of() : rows;
    }
  }
}
