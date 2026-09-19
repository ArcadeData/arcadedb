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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.query.opencypher.grammar.Cypher25Lexer;
import com.arcadedb.query.opencypher.parser.ParserUtils;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Applies the {@code YIELD}/{@code WHERE} tail of a {@code SHOW ...} command to the table that command produced.
 * <p>
 * Every openCypher {@code SHOW} command ends in an optional {@code showCommandYield} - {@code WHERE <predicate>}, or
 * {@code YIELD <items> [ORDER BY ...] [SKIP n] [LIMIT n] [WHERE ...] [RETURN ...]} - and the rows a {@code SHOW}
 * produces are an ordinary table of values. ArcadeDB answers the {@code SHOW} commands from the server and the
 * schema rather than from a query plan, so that tail had nowhere to be applied and was parsed and then silently
 * dropped: {@code SHOW DATABASES WHERE name = $dbName} returned every database on the server, which is what issue
 * #7946 reports. That breaks the standard Neo4j bootstrap idiom, where the query is the existence check that
 * decides whether to create the database.
 * <p>
 * Rather than grow a second filter/projection/sort engine beside the one the openCypher engine already is, the
 * table is handed back to that engine: the rows become a parameter and the tail is rewritten, as written, onto an
 * {@code UNWIND} over them. {@code YIELD}'s grammar is {@code WITH}'s grammar, so the translation is one keyword
 * and the whole of {@code WHERE}, {@code ORDER BY}, {@code SKIP}, {@code LIMIT}, {@code RETURN}, every operator,
 * function and parameter binding comes from the engine and cannot drift from what the same expression means
 * anywhere else in a query.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ShowCommandTail {
  /** Name of the parameter the rewritten query reads the {@code SHOW} rows from. */
  private static final String ROWS_PARAMETER = "__showRows";
  private static final String ROW_VARIABLE   = "__showRow";

  /** The rows of a {@code SHOW} command: the column names, and one list of values per row, positionally aligned. */
  public record Table(List<String> fields, List<List<Object>> rows) {
  }

  private ShowCommandTail() {
  }

  /**
   * Whether the command carries a tail at all. Callers use it to skip the rewrite - and the second query it runs -
   * for the far more common bare {@code SHOW ...}.
   */
  public static boolean hasTail(final String query) {
    return tailOf(query) != null;
  }

  /**
   * Applies the command's {@code YIELD}/{@code WHERE} tail to the table it produced.
   *
   * @param database   the database the rewritten query runs against; the rows never touch it, but its query engine,
   *                   its configuration and its parse limits are what evaluate the tail
   * @param query      the {@code SHOW} command as the client wrote it
   * @param fields     the column names of the table
   * @param rows       the rows of the table, each positionally aligned with {@code fields}
   * @param parameters the parameters the client bound, so a {@code WHERE name = $dbName} sees {@code $dbName}
   *
   * @return the filtered and projected table, or the table unchanged when the command has no tail
   */
  public static Table apply(final BasicDatabase database, final String query, final List<String> fields,
      final List<List<Object>> rows, final Map<String, Object> parameters) {
    if (database == null)
      return new Table(fields, rows);

    final List<Token> tail = tailOf(query);
    if (tail == null)
      return new Table(fields, rows);


    // The rows travel as a parameter, under a name the client cannot already have bound: putting it in last would
    // silently shadow a client parameter of the same name, and while __showRows is not a name anyone is likely to
    // choose, "unlikely" is not a reason to let one query quietly answer with another query's data.
    final Map<String, Object> effectiveParameters = new LinkedHashMap<>();
    if (parameters != null)
      effectiveParameters.putAll(parameters);

    String rowsParameter = ROWS_PARAMETER;
    while (effectiveParameters.containsKey(rowsParameter))
      rowsParameter = rowsParameter + "_";
    effectiveParameters.put(rowsParameter, asMaps(fields, rows));

    // Same reasoning for the UNWIND variable the rows are bound to: a tail that happens to name it would silently
    // read the synthetic binding instead of failing on an unknown variable.
    String rowVariable = ROW_VARIABLE;
    while (mentions(tail, rowVariable))
      rowVariable = rowVariable + "_";

    final Rewrite rewrite = rewrite(query, tail, fields, rowsParameter, rowVariable);

    final List<List<Object>> filtered = new ArrayList<>();
    List<String> rowFields = null;
    List<String> outFields = rewrite.projected();

    try (final ResultSet resultSet = database.query("opencypher", rewrite.query(), effectiveParameters)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();

        // The row itself is what says which columns the projection produced, and in which order. Asked of the row
        // rather than re-derived from the query text so this cannot answer with a name the engine did not project
        // - which would read back as null for every row. The statically derived list is the fallback for an empty
        // result, where there is no row to ask and the column list still has to be reported.
        if (rowFields == null)
          rowFields = new ArrayList<>(row.getPropertyNames());
        outFields = rowFields;

        final List<Object> values = new ArrayList<>(outFields.size());
        for (final String field : outFields)
          values.add(row.getProperty(field));
        filtered.add(values);
      }
    }

    return new Table(outFields != null ? outFields : fields, filtered);
  }

  /** The rewritten query and the columns it returns, or null for those a {@code *} leaves to the first row. */
  private record Rewrite(String query, List<String> projected) {
  }

  private static Rewrite rewrite(final String query, final List<Token> tail, final List<String> fields,
      final String rowsParameter, final String rowVariable) {
    final StringBuilder rewritten = new StringBuilder(query.length() + 64);
    rewritten.append("UNWIND $").append(rowsParameter).append(" AS ").append(rowVariable).append(" WITH ");

    final StringJoiner bindings = new StringJoiner(", ");
    for (final String field : fields)
      bindings.add(rowVariable + ".`" + field + "` AS `" + field + "`");
    rewritten.append(bindings);

    final Token first = tail.getFirst();
    if (first.getType() == Cypher25Lexer.WHERE) {
      // WHERE alone keeps every column the command declares.
      rewritten.append(' ').append(textOf(query, tail)).append(" RETURN ").append(backticked(fields));
      return new Rewrite(rewritten.toString(), fields);
    }

    // YIELD: its items, ORDER BY, SKIP, LIMIT and WHERE are WITH's, one keyword apart.
    final int returnAt = indexOfTopLevel(tail, Cypher25Lexer.RETURN, 1);
    final List<Token> yieldClause = tail.subList(1, returnAt < 0 ? tail.size() : returnAt);
    rewritten.append(" WITH ").append(textOf(query, yieldClause));

    if (returnAt >= 0) {
      final List<Token> returnClause = tail.subList(returnAt + 1, tail.size());
      rewritten.append(" RETURN ").append(textOf(query, returnClause));
      return new Rewrite(rewritten.toString(), projectedNames(query, itemsOf(returnClause), fields));
    }

    final List<String> yielded = projectedNames(query, itemsOf(yieldClause), fields);
    rewritten.append(" RETURN ").append(yielded != null ? backticked(yielded) : "*");
    return new Rewrite(rewritten.toString(), yielded);
  }

  /**
   * The names the projection produces, in order, or null when it is a bare {@code *} - whose columns are whatever
   * is in scope, which only the engine can say.
   */
  private static List<String> projectedNames(final String query, final List<Token> items, final List<String> fields) {
    if (items.isEmpty())
      return fields;
    if (items.size() == 1 && items.getFirst().getType() == Cypher25Lexer.TIMES)
      return null;

    final List<String> names = new ArrayList<>();
    int depth = 0;
    int itemStart = 0;
    for (int i = 0; i < items.size(); i++) {
      final Token token = items.get(i);
      if (depth == 0 && token.getType() == Cypher25Lexer.COMMA) {
        names.add(nameOf(query, items.subList(itemStart, i)));
        itemStart = i + 1;
      } else
        depth = nextDepth(depth, token);
    }
    names.add(nameOf(query, items.subList(itemStart, items.size())));
    return names;
  }

  /** The column name one projection item produces: its alias when it has one, otherwise the item as written. */
  private static String nameOf(final String query, final List<Token> item) {
    int depth = 0;
    int aliasAt = -1;
    for (int i = 0; i < item.size(); i++) {
      final Token token = item.get(i);
      if (depth == 0 && token.getType() == Cypher25Lexer.AS)
        aliasAt = i;
      else
        depth = nextDepth(depth, token);
    }

    if (aliasAt >= 0 && aliasAt < item.size() - 1)
      return ParserUtils.stripBackticks(textOf(query, item.subList(aliasAt + 1, item.size())).trim());
    return ParserUtils.stripBackticks(textOf(query, item).trim());
  }

  /** The projection items of a clause, i.e. everything before its ORDER BY / SKIP / LIMIT / WHERE. */
  private static List<Token> itemsOf(final List<Token> clause) {
    int depth = 0;
    for (int i = 0; i < clause.size(); i++) {
      final Token token = clause.get(i);
      if (depth == 0)
        switch (token.getType()) {
        case Cypher25Lexer.ORDER:
        case Cypher25Lexer.SKIPROWS:
        case Cypher25Lexer.OFFSET:
        case Cypher25Lexer.LIMITROWS:
        case Cypher25Lexer.WHERE:
          return clause.subList(0, i);
        default:
        }
      depth = nextDepth(depth, token);
    }
    return clause;
  }

  /**
   * The tokens of the command's {@code YIELD}/{@code WHERE} tail, or null when it has none.
   * <p>
   * Found with the openCypher lexer rather than by searching the text, so a {@code WHERE} inside a string literal,
   * a comment or a backquoted name is a value and not a clause.
   */
  private static List<Token> tailOf(final String query) {
    if (query == null)
      return null;

    final List<Token> tokens = tokenize(query);

    // A UNION is not one command with a tail, it is several commands each with their own - the shape Neo4j Desktop
    // sends to read the schema, "CALL db.labels() YIELD label RETURN ... UNION CALL db.relationshipTypes() ...".
    // Its parts are answered one by one by whoever produced the rows, so there is no single table here for a
    // trailing clause to be applied to, and treating the first YIELD as the whole query's tail would swallow every
    // branch after it. Declining leaves such a query answered exactly as it was.
    if (indexOfTopLevel(tokens, Cypher25Lexer.UNION, 0) >= 0)
      return null;

    int depth = 0;
    for (int i = 0; i < tokens.size(); i++) {
      final Token token = tokens.get(i);
      if (depth == 0 && (token.getType() == Cypher25Lexer.YIELD || token.getType() == Cypher25Lexer.WHERE))
        return tokens.subList(i, tokens.size());
      depth = nextDepth(depth, token);
    }
    return null;
  }

  /** The first token of that type sitting outside any bracket, at or after {@code from}, or -1. */
  private static int indexOfTopLevel(final List<Token> tokens, final int type, final int from) {
    int depth = 0;
    for (int i = 0; i < tokens.size(); i++) {
      final Token token = tokens.get(i);
      if (i >= from && depth == 0 && token.getType() == type)
        return i;
      depth = nextDepth(depth, token);
    }
    return -1;
  }

  /**
   * The bracket nesting after this token. Every scan in this class carries its own running depth rather than asking
   * for one token's depth, so a long command costs one pass over its tokens and not one per token.
   */
  private static int nextDepth(final int depth, final Token token) {
    return switch (token.getType()) {
      case Cypher25Lexer.LPAREN, Cypher25Lexer.LBRACKET, Cypher25Lexer.LCURLY -> depth + 1;
      case Cypher25Lexer.RPAREN, Cypher25Lexer.RBRACKET, Cypher25Lexer.RCURLY -> depth - 1;
      default -> depth;
    };
  }

  /** Whether the tail names this identifier anywhere, asked of the tokens so a string literal does not count. */
  private static boolean mentions(final List<Token> tokens, final String identifier) {
    for (final Token token : tokens)
      if (identifier.equals(token.getText()))
        return true;
    return false;
  }

  /** The source text the tokens span, taken from the query so whitespace, case and quoting are as written. */
  private static String textOf(final String query, final List<Token> tokens) {
    if (tokens.isEmpty())
      return "";
    return query.substring(tokens.getFirst().getStartIndex(), tokens.getLast().getStopIndex() + 1);
  }

  private static List<Token> tokenize(final String query) {
    final Cypher25Lexer lexer = new Cypher25Lexer(CharStreams.fromString(query));
    lexer.removeErrorListeners();

    final CommonTokenStream stream = new CommonTokenStream(lexer);
    stream.fill();

    final List<Token> tokens = new ArrayList<>();
    for (final Token token : stream.getTokens())
      if (token.getType() != Token.EOF && token.getChannel() == Token.DEFAULT_CHANNEL)
        tokens.add(token);
    return tokens;
  }

  private static String backticked(final List<String> names) {
    final StringJoiner joiner = new StringJoiner(", ");
    for (final String name : names)
      joiner.add("`" + name + "`");
    return joiner.toString();
  }

  private static List<Map<String, Object>> asMaps(final List<String> fields, final List<List<Object>> rows) {
    final List<Map<String, Object>> maps = new ArrayList<>(rows.size());
    for (final List<Object> row : rows) {
      final Map<String, Object> map = new LinkedHashMap<>(fields.size());
      for (int i = 0; i < fields.size(); i++)
        map.put(fields.get(i), i < row.size() ? row.get(i) : null);
      maps.add(map);
    }
    return maps;
  }
}
