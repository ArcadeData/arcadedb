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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * The structure of a catalog query, read off its tokens alone (issue #10228): the clauses of a SELECT, the
 * relations and join conditions of its FROM, the arms of a set operation, the conjuncts of a condition, and the
 * plain forms of the spellings that mean the same thing. Nothing here knows what a relation holds; that is
 * {@link PostgresCatalog}'s to decide.
 * <p>
 * <b>Read:</b> {@code SELECT [DISTINCT]} with or without FROM; FROM items that are relations, derived tables or
 * parenthesised inner joins, joined by commas, {@code CROSS}, {@code [INNER]}, {@code LEFT}, {@code RIGHT} or
 * {@code FULL JOIN ... ON | USING}; {@code WHERE}, {@code GROUP BY}, {@code ORDER BY}, {@code LIMIT n | ALL};
 * {@code UNION}, {@code INTERSECT} and {@code EXCEPT} chains with a trailing ORDER BY and LIMIT; the
 * {@code OPERATOR(schema.op)} and {@code COLLATE} spellings.
 * <p>
 * <b>Declined</b> (the statement is not a shape this catalog answers): {@code DISTINCT ON}, {@code HAVING},
 * {@code WINDOW}, {@code OFFSET}, {@code FETCH}, {@code INTO}, an outer join to a parenthesised join, an alias on one,
 * a FROM item that is a function call or a VALUES list, and anything nested deeper than {@link #MAX_NESTING}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresCatalogSyntax {
  /**
   * How deeply statements may nest inside one another - derived tables, sub-selects, set-operation arms,
   * parenthesised joins - before the query is declined. A client's catalog query nests two or three deep; the
   * bound is what keeps a query nested thousands deep, which any authenticated client can send, from recursing
   * until the stack gives out: a StackOverflowError is an Error, and would take the connection thread with it.
   */
  static final int MAX_NESTING = 32;

  private PostgresCatalogSyntax() {
  }

  /**
   * How a token moves the bracket depth of the clause around it: +1 for {@code (} and {@code [}, -1 for {@code )}
   * and {@code ]}, 0 for anything else. Every top-level scan here tracks that depth, and a clause keyword means
   * what it says only at depth 0.
   */
  static int nestingStep(final PostgresCatalogToken token) {
    if (token.type != PostgresCatalogToken.Type.SYMBOL)
      return 0;
    return switch (token.text) {
      case "(", "[" -> 1;
      case ")", "]" -> -1;
      default -> 0;
    };
  }

  /**
   * Rewrites the spellings that mean the same as a plainer one, so that nothing past this point has to know
   * both (issue #10228):
   * <ul>
   * <li>{@code OPERATOR(pg_catalog.=)} is {@code =}: psqlodbc writes every exact-name match this way, and psql
   * its regular-expression lookups ({@code OPERATOR(pg_catalog.~)}). Left as written it parsed as nothing, and a
   * WHERE that does not parse keeps every row - so asking for one table answered all of them;</li>
   * <li>{@code ~~}, {@code ~~*} and their negations are LIKE and ILIKE, which is what PostgreSQL itself calls
   * those operators;</li>
   * <li>{@code COLLATE <name>} is dropped: every name in this catalog compares the same under any collation a
   * client names, and psql appends {@code COLLATE pg_catalog.default} to its name lookup. An ORDER BY with one
   * sorts in this catalog's own order - by code point, as {@code COLLATE "C"} does - whatever collation it names.
   * The rewrite only touches tokens this catalog evaluates: a statement it hands on keeps its own text.</li>
   * </ul>
   */
  static List<PostgresCatalogToken> plainSpellings(final List<PostgresCatalogToken> tokens) {
    List<PostgresCatalogToken> result = null;

    for (int i = 0; i < tokens.size(); i++) {
      final PostgresCatalogToken token = tokens.get(i);
      int end = -1;
      List<PostgresCatalogToken> replacement = null;

      if (token.isKeyword("OPERATOR") && i + 1 < tokens.size() && tokens.get(i + 1).isSymbol("(")) {
        int j = i + 2;
        while (j + 1 < tokens.size() && tokens.get(j).type == PostgresCatalogToken.Type.IDENTIFIER
            && tokens.get(j + 1).isSymbol("."))
          j += 2;
        final StringBuilder operator = new StringBuilder();
        while (j < tokens.size() && tokens.get(j).type == PostgresCatalogToken.Type.SYMBOL && !tokens.get(j).isSymbol(")"))
          operator.append(tokens.get(j++).text);
        if (!operator.isEmpty() && j < tokens.size() && tokens.get(j).isSymbol(")")) {
          end = j + 1;
          replacement = switch (operator.toString()) {
            case "~~" -> List.of(PostgresCatalogToken.keyword("LIKE"));
            case "~~*" -> List.of(PostgresCatalogToken.keyword("ILIKE"));
            case "!~~" -> List.of(PostgresCatalogToken.keyword("NOT"), PostgresCatalogToken.keyword("LIKE"));
            case "!~~*" -> List.of(PostgresCatalogToken.keyword("NOT"), PostgresCatalogToken.keyword("ILIKE"));
            default -> List.of(PostgresCatalogToken.symbol(operator.toString()));
          };
        }
      } else if (token.isKeyword("COLLATE") && i + 1 < tokens.size() && isName(tokens.get(i + 1))) {
        int j = i + 2;
        while (j + 1 < tokens.size() && tokens.get(j).isSymbol(".") && isName(tokens.get(j + 1)))
          j += 2;
        end = j;
        replacement = List.of();
      }

      if (replacement == null) {
        if (result != null)
          result.add(token);
        continue;
      }

      if (result == null)
        result = new ArrayList<>(tokens.subList(0, i));
      result.addAll(replacement);
      i = end - 1;
    }

    return result == null ? tokens : result;
  }

  /** Whether the statement has a set operation anywhere - psqlodbc's is inside a derived table. */
  static boolean containsSetOperation(final List<PostgresCatalogToken> tokens) {
    for (final PostgresCatalogToken token : tokens)
      if (token.isKeyword("UNION") || token.isKeyword("INTERSECT") || token.isKeyword("EXCEPT"))
        return true;
    return false;
  }

  static boolean isName(final PostgresCatalogToken token) {
    return token.type == PostgresCatalogToken.Type.IDENTIFIER || token.type == PostgresCatalogToken.Type.QUOTED_IDENTIFIER;
  }

  /**
   * {@code (SELECT ...)} written whole in parentheses, which a set-operation arm may be, is the SELECT inside. One
   * pass finds where each of the leading parentheses closes, so peeling a statement wrapped a thousand times costs
   * one scan rather than one scan per layer - this runs before the nesting bound counts anything.
   */
  static List<PostgresCatalogToken> unwrapParentheses(final List<PostgresCatalogToken> tokens) {
    final int size = tokens.size();
    int leading = 0;
    while (leading < size && tokens.get(leading).isSymbol("("))
      ++leading;
    if (leading == 0 || !tokens.get(size - 1).isSymbol(")"))
      return tokens;

    // closes[d] is where the leading parenthesis at depth d closes: the first time the depth falls back to d.
    final int[] closes = new int[leading];
    Arrays.fill(closes, -1);
    int depth = 0;
    for (int i = 0; i < size; i++) {
      if (tokens.get(i).isSymbol("("))
        ++depth;
      else if (tokens.get(i).isSymbol(")")) {
        if (--depth < 0)
          return tokens;
        if (depth < leading && closes[depth] < 0)
          closes[depth] = i;
      }
    }

    int peeled = 0;
    while (peeled < leading && peeled < size - 1 - peeled && closes[peeled] == size - 1 - peeled)
      ++peeled;
    return peeled == 0 ? tokens : tokens.subList(peeled, size - peeled);
  }

  /**
   * Splits a condition at its top-level ANDs: not inside parentheses or a CASE, and not BETWEEN's own AND. A
   * condition with a top-level OR is one conjunct, because AND binds tighter and splitting it would regroup it.
   */
  static List<List<PostgresCatalogToken>> splitConjuncts(final List<PostgresCatalogToken> tokens) {
    final List<Integer> ands = new ArrayList<>();
    int depth = 0;
    int cases = 0;
    int betweens = 0;
    for (int i = 0; i < tokens.size(); i++) {
      final PostgresCatalogToken token = tokens.get(i);
      final int step = nestingStep(token);
      depth += step;
      if (step == 0 && depth == 0) {
        if (token.isKeyword("CASE"))
          ++cases;
        else if (token.isKeyword("END"))
          --cases;
        else if (cases == 0 && token.isKeyword("BETWEEN"))
          ++betweens;
        else if (cases == 0 && token.isKeyword("OR"))
          return List.of(tokens);
        else if (cases == 0 && token.isKeyword("AND")) {
          if (betweens > 0)
            --betweens;
          else
            ands.add(i);
        }
      }
    }

    if (ands.isEmpty())
      return List.of(tokens);

    final List<List<PostgresCatalogToken>> conjuncts = new ArrayList<>(ands.size() + 1);
    int start = 0;
    for (final int and : ands) {
      conjuncts.add(tokens.subList(start, and));
      start = and + 1;
    }
    conjuncts.add(tokens.subList(start, tokens.size()));
    return conjuncts;
  }

  /** Splits a token list at the commas that are not inside parentheses or brackets. */
  static List<List<PostgresCatalogToken>> splitTopLevel(final List<PostgresCatalogToken> tokens) {
    final List<List<PostgresCatalogToken>> items = new ArrayList<>();
    List<PostgresCatalogToken> current = new ArrayList<>();
    int depth = 0;

    for (final PostgresCatalogToken token : tokens) {
      final int step = nestingStep(token);
      depth += step;
      if (step == 0 && depth == 0 && token.isSymbol(",")) {
        items.add(current);
        current = new ArrayList<>();
        continue;
      }
      current.add(token);
    }

    if (depth != 0)
      return null;

    items.add(current);
    return items;
  }

  // ---------------------------------------------------------------- set operations

  /**
   * A UNION, INTERSECT or EXCEPT chain, split at the top level into its arms (issue #10228). ArcadeDB's SQL has
   * no set operations, so a client that sends one - psqlodbc does, for {@code SQLTables(SQL_ALL_TABLE_TYPES)} -
   * got a syntax error. The arms are answered by this catalog one by one, and combined here.
   */
  static final class SetOperation {
    final List<List<PostgresCatalogToken>> arms      = new ArrayList<>();
    /** Between arm {@code i} and arm {@code i + 1}: UNION, INTERSECT or EXCEPT. */
    final List<String>                     operators = new ArrayList<>();
    final List<Boolean>                    all       = new ArrayList<>();
    /** The trailing ORDER BY and LIMIT, which apply to the result of the whole chain, not to its last arm. */
    List<PostgresCatalogToken>             orderBy;
    Integer                                limit;

    /** @return the chain, or null when the statement has no top-level set operation or one this class cannot read */
    static SetOperation split(final List<PostgresCatalogToken> tokens) {
      SetOperation operation = null;
      int depth = 0;
      int armStart = 0;

      for (int i = 0; i < tokens.size(); i++) {
        final PostgresCatalogToken token = tokens.get(i);
        final int step = nestingStep(token);
        depth += step;
        if (step == 0 && depth == 0 && (token.isKeyword("UNION") || token.isKeyword("INTERSECT")
            || token.isKeyword("EXCEPT"))) {
          if (operation == null)
            operation = new SetOperation();
          operation.arms.add(tokens.subList(armStart, i));
          operation.operators.add(token.text.toUpperCase(Locale.ENGLISH));
          boolean all = false;
          if (i + 1 < tokens.size() && tokens.get(i + 1).isKeyword("ALL")) {
            all = true;
            ++i;
          } else if (i + 1 < tokens.size() && tokens.get(i + 1).isKeyword("DISTINCT"))
            ++i;
          operation.all.add(all);
          armStart = i + 1;
        }
      }

      if (operation == null)
        return null;

      List<PostgresCatalogToken> last = tokens.subList(armStart, tokens.size());
      depth = 0;
      for (int i = 0; i < last.size(); i++) {
        final PostgresCatalogToken token = last.get(i);
        final int step = nestingStep(token);
        depth += step;
        if (step == 0 && depth == 0 && (token.isKeyword("ORDER") || token.isKeyword("LIMIT") || token.isKeyword("OFFSET")
            || token.isKeyword("FETCH"))) {
          if (!operation.readTail(last.subList(i, last.size())))
            return null;
          last = last.subList(0, i);
          break;
        }
      }
      operation.arms.add(last);

      for (final List<PostgresCatalogToken> arm : operation.arms)
        if (arm.isEmpty())
          return null;
      return operation;
    }

    private boolean readTail(final List<PostgresCatalogToken> tail) {
      int i = 0;
      if (tail.get(0).isKeyword("ORDER")) {
        if (tail.size() < 3 || !tail.get(1).isKeyword("BY"))
          return false;
        i = 2;
        int depth = 0;
        while (i < tail.size() && !(depth == 0 && tail.get(i).isKeyword("LIMIT"))) {
          if (depth == 0 && (tail.get(i).isKeyword("OFFSET") || tail.get(i).isKeyword("FETCH")))
            return false;
          depth += nestingStep(tail.get(i));
          ++i;
        }
        orderBy = new ArrayList<>(tail.subList(2, i));
      }

      if (i == tail.size())
        return true;

      // OFFSET and FETCH are outside the slice; LIMIT takes a number or ALL, and nothing may follow it.
      if (!tail.get(i).isKeyword("LIMIT") || i + 2 != tail.size())
        return false;
      if (tail.get(i + 1).isKeyword("ALL"))
        return true;
      if (tail.get(i + 1).type != PostgresCatalogToken.Type.NUMBER)
        return false;
      try {
        limit = Integer.valueOf(tail.get(i + 1).text);
        return true;
      } catch (final NumberFormatException e) {
        return false;
      }
    }
  }

  // ---------------------------------------------------------------- statement structure

  /** The clauses of a SELECT, as token ranges. */
  static final class Statement {
    boolean                    distinct;
    List<PostgresCatalogToken> projection;
    /** Null for a SELECT with no FROM clause. */
    List<PostgresCatalogToken> from;
    List<PostgresCatalogToken> where;
    List<PostgresCatalogToken> groupBy;
    List<PostgresCatalogToken> orderBy;
    Integer                    limit;

    /**
     * Splits a token list at the top-level clause keywords. Returns null for anything with a shape this class
     * does not read: a set operation (resolved by {@link SetOperation} before this is reached), a HAVING, a
     * window clause, an OFFSET.
     */
    static Statement split(final List<PostgresCatalogToken> tokens) {
      final Statement statement = new Statement();
      int i = 1; // past SELECT

      if (i < tokens.size() && tokens.get(i).isKeyword("DISTINCT")) {
        ++i;
        if (i < tokens.size() && tokens.get(i).isKeyword("ON"))
          return null;
        statement.distinct = true;
      } else if (i < tokens.size() && tokens.get(i).isKeyword("ALL"))
        ++i;

      final int projectionStart = i;
      int depth = 0;
      for (; i < tokens.size(); i++) {
        final PostgresCatalogToken token = tokens.get(i);
        final int step = nestingStep(token);
        depth += step;
        if (step == 0 && depth == 0 && endsProjection(token))
          break;
      }

      statement.projection = new ArrayList<>(tokens.subList(projectionStart, i));
      if (statement.projection.isEmpty())
        return null;

      int fromStart = -1;
      if (i < tokens.size() && tokens.get(i).isKeyword("FROM")) {
        fromStart = i + 1;
        i = fromStart;
      }
      final int clausesStart = i;

      int end = tokens.size();
      int whereStart = -1;
      int groupStart = -1;
      int orderStart = -1;
      int limitStart = -1;
      depth = 0;

      for (; i < tokens.size(); i++) {
        final PostgresCatalogToken token = tokens.get(i);
        final int step = nestingStep(token);
        depth += step;
        if (step == 0 && depth == 0) {
          if (token.isKeyword("UNION") || token.isKeyword("INTERSECT") || token.isKeyword("EXCEPT")
              || token.isKeyword("HAVING") || token.isKeyword("WINDOW") || token.isKeyword("FETCH")
              || token.isKeyword("OFFSET") || token.isKeyword("INTO"))
            return null;
          if (token.isKeyword("WHERE") && whereStart < 0 && groupStart < 0 && orderStart < 0)
            whereStart = i + 1;
          else if (token.isKeyword("GROUP") && groupStart < 0 && orderStart < 0) {
            if (i + 1 >= tokens.size() || !tokens.get(i + 1).isKeyword("BY"))
              return null;
            groupStart = i + 2;
          } else if (token.isKeyword("ORDER") && orderStart < 0) {
            if (i + 1 >= tokens.size() || !tokens.get(i + 1).isKeyword("BY"))
              return null;
            orderStart = i + 2;
          } else if (token.isKeyword("LIMIT") && limitStart < 0)
            limitStart = i + 1;
        }
      }

      if (limitStart > 0) {
        if (limitStart >= tokens.size())
          return null;
        final PostgresCatalogToken limitToken = tokens.get(limitStart);
        // LIMIT ALL is no limit at all.
        if (!limitToken.isKeyword("ALL")) {
          if (limitToken.type != PostgresCatalogToken.Type.NUMBER)
            return null;
          try {
            statement.limit = Integer.valueOf(limitToken.text);
          } catch (final NumberFormatException e) {
            return null;
          }
        }
        end = Math.min(end, limitStart - 1);
      }

      if (orderStart > 0) {
        statement.orderBy = new ArrayList<>(tokens.subList(orderStart, end));
        end = Math.min(end, orderStart - 2);
      }

      if (groupStart > 0) {
        statement.groupBy = new ArrayList<>(tokens.subList(groupStart, end));
        if (statement.groupBy.isEmpty())
          return null;
        end = Math.min(end, groupStart - 2);
      }

      if (whereStart > 0) {
        statement.where = new ArrayList<>(tokens.subList(whereStart, end));
        end = Math.min(end, whereStart - 1);
      }

      if (fromStart < 0)
        // No FROM: the projection ran up to the first clause, so nothing can be left between the two.
        return end == clausesStart ? statement : null;

      statement.from = new ArrayList<>(tokens.subList(fromStart, end));
      return statement.from.isEmpty() ? null : statement;
    }

    private static boolean endsProjection(final PostgresCatalogToken token) {
      return token.isKeyword("FROM") || token.isKeyword("WHERE") || token.isKeyword("GROUP") || token.isKeyword("HAVING")
          || token.isKeyword("ORDER") || token.isKeyword("LIMIT") || token.isKeyword("OFFSET") || token.isKeyword("FETCH")
          || token.isKeyword("WINDOW") || token.isKeyword("UNION") || token.isKeyword("INTERSECT")
          || token.isKeyword("EXCEPT") || token.isKeyword("INTO");
    }
  }

  /** How a FROM entry is joined to the entries before it. */
  enum Join {
    FIRST, COMMA, CROSS, INNER, LEFT, RIGHT, FULL
  }

  /** One item of the FROM clause: an emulated relation, or a derived table with the tokens of its SELECT. */
  static final class FromEntry {
    final String                     relation;
    /** The name as the client wrote it, which is what a case-sensitive type lookup has to be given. */
    final String                     writtenAs;
    final String                     alias;
    final List<PostgresCatalogToken> derived;
    final Join                       join;
    /** The ON condition the entry was joined with, null without one. */
    List<PostgresCatalogToken>       on;

    FromEntry(final String relation, final String writtenAs, final String alias,
        final List<PostgresCatalogToken> derived, final Join join) {
      this.relation = relation;
      this.writtenAs = writtenAs;
      this.alias = alias;
      this.derived = derived;
      this.join = join;
    }
  }

  /**
   * Reads the relations out of a FROM clause: comma-separated items, explicit JOINs and parenthesised joins
   * alike, each with the condition it was joined on. Every emulated row already carries the columns of every
   * relation it can be joined to, so the conditions do not pair rows up; {@link #project} applies them instead,
   * an inner join's as a filter and an outer join's as what decides whether the joined relation reads NULL.
   */
  static List<FromEntry> parseFrom(final List<PostgresCatalogToken> tokens) {
    final List<FromEntry> entries = new ArrayList<>();
    return parseFrom(tokens, Join.FIRST, entries, 0) && !entries.isEmpty() ? entries : null;
  }

  private static boolean parseFrom(final List<PostgresCatalogToken> tokens, final Join firstJoin,
      final List<FromEntry> entries, final int nesting) {
    if (nesting > MAX_NESTING)
      return false;

    Join join = firstJoin;
    int i = 0;
    while (i < tokens.size()) {
      // Join decorations before the item.
      if (isJoinWord(tokens.get(i))) {
        boolean left = false;
        boolean right = false;
        boolean full = false;
        boolean cross = false;
        while (i < tokens.size() && isJoinWord(tokens.get(i))) {
          final PostgresCatalogToken word = tokens.get(i++);
          left |= word.isKeyword("LEFT");
          right |= word.isKeyword("RIGHT");
          full |= word.isKeyword("FULL");
          cross |= word.isKeyword("CROSS");
        }
        join = left ? Join.LEFT : right ? Join.RIGHT : full ? Join.FULL : cross ? Join.CROSS : Join.INNER;
        if (i >= tokens.size())
          return true;
      }

      final PostgresCatalogToken token = tokens.get(i);
      // The entry an ON condition after this item belongs to: the item itself, or the first relation of a group.
      final int item = entries.size();

      if (token.isSymbol("(")) {
        final int close = matchingParenthesis(tokens, i);
        if (close < 0)
          return false;
        final List<PostgresCatalogToken> inside = tokens.subList(i + 1, close);
        i = close + 1;

        if (startsAStatement(inside)) {
          // A derived table: read the parenthesised SELECT whole and hand it back for its own resolution.
          String derivedAlias = null;
          if (i < tokens.size() && tokens.get(i).isKeyword("AS"))
            ++i;
          if (i < tokens.size() && isName(tokens.get(i)) && !isStructuralWord(tokens.get(i))) {
            derivedAlias = tokens.get(i).text.toLowerCase(Locale.ENGLISH);
            ++i;
          }
          entries.add(new FromEntry(derivedAlias == null ? "" : derivedAlias, null, derivedAlias, new ArrayList<>(inside),
              join));
        } else {
          // A parenthesised join - (a JOIN b ON ...) - which psqlodbc writes its column list with, three deep
          // (issue #10228). Inner joins associate, so its relations join the outer ones as written. An outer
          // join to the group would have to null the whole group, and an alias would rename it: neither is a
          // shape this reads.
          if (join == Join.LEFT || join == Join.RIGHT || join == Join.FULL)
            return false;
          if (!parseFrom(inside, join, entries, nesting + 1) || entries.size() == item)
            return false;
          if (i < tokens.size() && (tokens.get(i).isKeyword("AS") || (isName(tokens.get(i))
              && !isStructuralWord(tokens.get(i)))))
            return false;
        }
      } else if (isName(token)) {
        final StringBuilder relation = new StringBuilder(token.text.toLowerCase(Locale.ENGLISH));
        final StringBuilder written = new StringBuilder(token.text);
        ++i;
        while (i + 1 < tokens.size() && tokens.get(i).isSymbol(".")) {
          final PostgresCatalogToken part = tokens.get(i + 1);
          if (!isName(part))
            return false;
          relation.append('.').append(part.text.toLowerCase(Locale.ENGLISH));
          written.setLength(0);
          written.append(part.text);
          i += 2;
        }

        String name = relation.toString();
        // pg_catalog is the schema every pg_ relation lives in, and naming it changes nothing.
        if (name.startsWith("pg_catalog."))
          name = name.substring("pg_catalog.".length());

        String alias = null;
        if (i < tokens.size() && tokens.get(i).isKeyword("AS")) {
          ++i;
          if (i >= tokens.size())
            return false;
          alias = tokens.get(i).text.toLowerCase(Locale.ENGLISH);
          ++i;
        } else if (i < tokens.size() && isName(tokens.get(i)) && !isStructuralWord(tokens.get(i))) {
          alias = tokens.get(i).text.toLowerCase(Locale.ENGLISH);
          ++i;
        }

        entries.add(new FromEntry(name, written.toString(), alias, null, join));
      } else
        // A function call, a VALUES list: not a shape this catalog reads.
        return false;

      // An ON or USING condition, up to the next comma or join at this level.
      if (i < tokens.size() && (tokens.get(i).isKeyword("ON") || tokens.get(i).isKeyword("USING"))) {
        final boolean using = tokens.get(i).isKeyword("USING");
        final int start = ++i;
        int depth = 0;
        while (i < tokens.size()) {
          final PostgresCatalogToken t = tokens.get(i);
          final int step = nestingStep(t);
          depth += step;
          if (step == 0 && depth == 0 && (t.isSymbol(",") || isJoinWord(t)))
            break;
          ++i;
        }
        // USING names columns both sides share, which every emulated row satisfies by construction.
        if (!using)
          entries.get(item).on = new ArrayList<>(tokens.subList(start, i));
      }

      if (i < tokens.size()) {
        if (tokens.get(i).isSymbol(",")) {
          join = Join.COMMA;
          ++i;
        } else if (!isJoinWord(tokens.get(i)))
          return false;
      }
    }

    return true;
  }

  /** The index of the {@code )} closing the {@code (} at {@code open}, or -1. */
  private static int matchingParenthesis(final List<PostgresCatalogToken> tokens, final int open) {
    int depth = 0;
    for (int i = open; i < tokens.size(); i++) {
      if (tokens.get(i).isSymbol("("))
        ++depth;
      else if (tokens.get(i).isSymbol(")") && --depth == 0)
        return i;
    }
    return -1;
  }

  /** Whether parenthesised FROM content is a sub-select (a derived table) rather than a parenthesised join. */
  private static boolean startsAStatement(final List<PostgresCatalogToken> inside) {
    int i = 0;
    while (i < inside.size() && inside.get(i).isSymbol("("))
      ++i;
    if (i == inside.size())
      return true;
    final PostgresCatalogToken token = inside.get(i);
    return token.isKeyword("SELECT") || token.isKeyword("VALUES") || token.isKeyword("WITH") || token.isKeyword("TABLE");
  }

  private static boolean isJoinWord(final PostgresCatalogToken token) {
    return token.isKeyword("JOIN") || token.isKeyword("LEFT") || token.isKeyword("RIGHT") || token.isKeyword("FULL")
        || token.isKeyword("INNER") || token.isKeyword("OUTER") || token.isKeyword("CROSS") || token.isKeyword("NATURAL");
  }

  private static boolean isStructuralWord(final PostgresCatalogToken token) {
    return isJoinWord(token) || token.isKeyword("ON") || token.isKeyword("USING") || token.isKeyword("WHERE")
        || token.isKeyword("ORDER") || token.isKeyword("GROUP") || token.isKeyword("LIMIT") || token.isKeyword("AS");
  }
}
