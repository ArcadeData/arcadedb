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

import com.arcadedb.exception.CommandParsingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/**
 * A {@code COPY ... TO STDOUT} statement, recognised ahead of the SQL engine the way {@code SET}/{@code SHOW} and
 * the system queries are (issue #7188): the wire protocol answers it itself, with a {@code CopyOutResponse}, one
 * {@code CopyData} per row and a {@code CopyDone}, and only the query inside it reaches the engine.
 * <p>
 * This is what the Apache Arrow ADBC PostgreSQL driver reads EVERY result set through by default
 * ({@code COPY (SELECT ...) TO STDOUT (FORMAT binary)}), what {@code psql}'s {@code \copy} sends, and what
 * {@code pg_dump}'s data phase and most bulk-export tooling want. Both spellings PostgreSQL accepts are
 * recognised: the option list, {@code COPY ... TO STDOUT [WITH] (FORMAT csv, HEADER, DELIMITER ';')}, and the
 * pre-9.0 keywords still emitted by older tools and by {@code \copy} users, {@code COPY ... TO STDOUT CSV HEADER
 * DELIMITER ';'}. The source is either a parenthesised query, run as it stands in the session's language, or a
 * type name with an optional column list, which is the same statement with an implicit {@code SELECT}.
 * <p>
 * The ingest direction ({@code COPY ... FROM STDIN}) and the server-side targets ({@code TO 'file'},
 * {@code TO PROGRAM}) are refused with {@code feature_not_supported}: the first is a separate statement with its
 * own failure modes, and the other two would let a wire client write files or run commands on the server host.
 * <p>
 * This class also owns the text and CSV row encodings, which are PostgreSQL's own ({@code copyto.c}): a client
 * that parses the stream - {@code \copy}, pandas, the ADBC driver in text mode - relies on the escaping rules
 * exactly, and a value that merely looks right in a terminal is not the same thing as one that round-trips.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PostgresCopyStatement {

  public enum Format {TEXT, CSV, BINARY}

  /**
   * SQLSTATE {@code feature_not_supported}, for the parts of COPY this server declines to run.
   */
  static final String SQLSTATE_FEATURE_NOT_SUPPORTED = "0A000";
  /**
   * SQLSTATE {@code syntax_error}, for a COPY this server would run but cannot read.
   */
  static final String SQLSTATE_SYNTAX_ERROR          = "42601";

  /**
   * A COPY the protocol will not run, carrying the SQLSTATE the client is told. Extends the parsing exception
   * so the existing "Syntax error on ..." arms of the executor catch it, while {@link #sqlState} lets them
   * report {@code feature_not_supported} rather than {@code syntax_error} where the statement was perfectly
   * well-formed and merely asks for something this server does not do.
   */
  public static final class CopyException extends CommandParsingException {
    public final String sqlState;

    CopyException(final String message, final String sqlState) {
      super(message);
      this.sqlState = sqlState;
    }
  }

  private final String      query;
  private final Format      format;
  private final char        delimiter;
  private final String      nullString;
  private final boolean     header;
  private final char        quote;
  private final char        escape;
  private final boolean     forceQuoteAll;
  private final Set<String> forceQuoteColumns;

  private PostgresCopyStatement(final String query, final Format format, final char delimiter, final String nullString,
      final boolean header, final char quote, final char escape, final boolean forceQuoteAll,
      final Set<String> forceQuoteColumns) {
    this.query = query;
    this.format = format;
    this.delimiter = delimiter;
    this.nullString = nullString;
    this.header = header;
    this.quote = quote;
    this.escape = escape;
    this.forceQuoteAll = forceQuoteAll;
    this.forceQuoteColumns = forceQuoteColumns;
  }

  /**
   * The statement to run for the rows: the parenthesised query as written, or the {@code SELECT} a table form
   * stands for.
   */
  public String getQuery() {
    return query;
  }

  public Format getFormat() {
    return format;
  }

  public char getDelimiter() {
    return delimiter;
  }

  public String getNullString() {
    return nullString;
  }

  public boolean isHeader() {
    return header;
  }

  public char getQuote() {
    return quote;
  }

  public char getEscape() {
    return escape;
  }

  /**
   * Whether {@code text} is a COPY statement at all: the keyword, followed by whitespace or the opening
   * parenthesis of a query. Cheap, and what the dispatch asks before {@link #parse} does any work.
   */
  public static boolean isCopy(final String text) {
    if (text == null || text.length() < 5 || !text.regionMatches(true, 0, "COPY", 0, 4))
      return false;
    final char next = text.charAt(4);
    return Character.isWhitespace(next) || next == '(';
  }

  /**
   * Parses a COPY statement, or returns null when {@code text} is not one.
   *
   * @throws CopyException when it is a COPY this server will not run - the ingest direction, a server-side
   *                       file or program target, an option that does not exist or combines with the format the
   *                       way PostgreSQL itself refuses
   */
  public static PostgresCopyStatement parse(final String text) {
    if (!isCopy(text))
      return null;

    String statement = text.trim();
    if (statement.endsWith(";"))
      statement = statement.substring(0, statement.length() - 1).trim();

    int pos = 4;
    while (pos < statement.length() && Character.isWhitespace(statement.charAt(pos)))
      pos++;
    if (pos >= statement.length())
      throw new CopyException("syntax error at end of COPY statement", SQLSTATE_SYNTAX_ERROR);

    final String query;
    final String tail;
    if (statement.charAt(pos) == '(') {
      final int close = matchingParenthesis(statement, pos);
      query = statement.substring(pos + 1, close).trim();
      if (query.isEmpty())
        throw new CopyException("COPY query must not be empty", SQLSTATE_SYNTAX_ERROR);
      tail = statement.substring(close + 1);
      return parseTail(query, tokenize(tail), 0, true);
    }

    tail = statement.substring(pos);
    final List<PostgresCatalogToken> tokens = tokenize(tail);
    if (tokens.isEmpty())
      throw new CopyException("syntax error at end of COPY statement", SQLSTATE_SYNTAX_ERROR);

    // COPY <table> [ ( column [, ...] ) ] - the name may arrive schema-qualified ("public"."t"): ArcadeDB has no
    // schemas, so the last segment is the type.
    int i = 0;
    String table = identifier(tokens, i++);
    while (i + 1 < tokens.size() && tokens.get(i).isSymbol(".")) {
      table = identifier(tokens, i + 1);
      i += 2;
    }

    final List<String> columns = new ArrayList<>();
    if (i < tokens.size() && tokens.get(i).isSymbol("(")) {
      i++;
      while (true) {
        columns.add(identifier(tokens, i++));
        if (i >= tokens.size())
          throw new CopyException("syntax error in COPY column list: missing ')'", SQLSTATE_SYNTAX_ERROR);
        if (tokens.get(i).isSymbol(","))
          i++;
        else if (tokens.get(i).isSymbol(")")) {
          i++;
          break;
        } else
          throw new CopyException("syntax error in COPY column list at '" + tokens.get(i).text + "'", SQLSTATE_SYNTAX_ERROR);
      }
    }

    final StringBuilder select = new StringBuilder("SELECT ");
    for (int c = 0; c < columns.size(); c++) {
      if (c > 0)
        select.append(", ");
      select.append('`').append(columns.get(c)).append('`');
    }
    select.append(columns.isEmpty() ? "FROM `" : " FROM `").append(table).append('`');
    return parseTail(select.toString(), tokens, i, false);
  }

  /**
   * Parses what follows the source: the direction and target, then the options in either spelling.
   */
  private static PostgresCopyStatement parseTail(final String query, final List<PostgresCatalogToken> tokens, int i,
      final boolean queryForm) {
    if (i >= tokens.size())
      throw new CopyException("syntax error in COPY: expected TO or FROM after the source", SQLSTATE_SYNTAX_ERROR);

    if (tokens.get(i).isKeyword("FROM")) {
      if (queryForm)
        throw new CopyException("COPY FROM cannot read into a query", SQLSTATE_SYNTAX_ERROR);
      throw new CopyException("COPY ... FROM STDIN is not supported by this server: use INSERT, or the HTTP import API",
          SQLSTATE_FEATURE_NOT_SUPPORTED);
    }
    if (!tokens.get(i).isKeyword("TO"))
      throw new CopyException("syntax error in COPY at '" + tokens.get(i).text + "': expected TO", SQLSTATE_SYNTAX_ERROR);
    i++;

    if (i >= tokens.size())
      throw new CopyException("syntax error in COPY: expected STDOUT after TO", SQLSTATE_SYNTAX_ERROR);
    final PostgresCatalogToken target = tokens.get(i++);
    if (target.type == PostgresCatalogToken.Type.STRING)
      throw new CopyException("COPY TO a server-side file is not supported by this server: use COPY ... TO STDOUT",
          SQLSTATE_FEATURE_NOT_SUPPORTED);
    if (target.isKeyword("PROGRAM"))
      throw new CopyException("COPY TO PROGRAM is not supported by this server: use COPY ... TO STDOUT",
          SQLSTATE_FEATURE_NOT_SUPPORTED);
    if (!target.isKeyword("STDOUT"))
      throw new CopyException("syntax error in COPY at '" + target.text + "': expected STDOUT", SQLSTATE_SYNTAX_ERROR);

    final Options options = new Options();
    if (i < tokens.size() && tokens.get(i).isKeyword("WITH"))
      i++;

    if (i < tokens.size() && tokens.get(i).isSymbol("("))
      i = parseOptionList(tokens, i + 1, options);
    else
      i = parseLegacyOptions(tokens, i, options);

    if (i < tokens.size())
      throw new CopyException("syntax error in COPY at '" + tokens.get(i).text + "'", SQLSTATE_SYNTAX_ERROR);

    return options.build(query);
  }

  /**
   * {@code ( option [value] [, ...] )}, the spelling PostgreSQL has had since 9.0. Returns the index after the
   * closing parenthesis.
   */
  private static int parseOptionList(final List<PostgresCatalogToken> tokens, int i, final Options options) {
    while (true) {
      if (i >= tokens.size())
        throw new CopyException("syntax error in COPY options: missing ')'", SQLSTATE_SYNTAX_ERROR);
      final String name = tokens.get(i++).text.toLowerCase(Locale.ENGLISH);
      // The value is optional for the boolean options, and is whatever single token follows otherwise - a
      // string literal, a bare word (FORMAT csv, HEADER true) or a number (HEADER 1).
      PostgresCatalogToken value = null;
      List<String> list = null;
      if (i < tokens.size() && tokens.get(i).isSymbol("(")) {
        list = new ArrayList<>();
        i++;
        while (true) {
          list.add(identifier(tokens, i++));
          if (i >= tokens.size())
            throw new CopyException("syntax error in COPY option " + name + ": missing ')'", SQLSTATE_SYNTAX_ERROR);
          if (tokens.get(i).isSymbol(","))
            i++;
          else if (tokens.get(i).isSymbol(")")) {
            i++;
            break;
          } else
            throw new CopyException("syntax error in COPY option " + name + " at '" + tokens.get(i).text + "'",
                SQLSTATE_SYNTAX_ERROR);
        }
      } else if (i < tokens.size() && !tokens.get(i).isSymbol(",") && !tokens.get(i).isSymbol(")"))
        value = tokens.get(i++);

      options.set(name, value, list);

      if (i >= tokens.size())
        throw new CopyException("syntax error in COPY options: missing ')'", SQLSTATE_SYNTAX_ERROR);
      if (tokens.get(i).isSymbol(",")) {
        i++;
        continue;
      }
      if (tokens.get(i).isSymbol(")"))
        return i + 1;
      throw new CopyException("syntax error in COPY options at '" + tokens.get(i).text + "'", SQLSTATE_SYNTAX_ERROR);
    }
  }

  /**
   * The keyword spelling from before 9.0, still accepted by PostgreSQL and still produced by {@code \copy}
   * users: {@code [BINARY] [DELIMITER [AS] 'x'] [NULL [AS] 'x'] [CSV [HEADER] [QUOTE [AS] 'q'] [ESCAPE [AS] 'e']
   * [FORCE QUOTE cols | *]]}. Returns the index of the first token it did not consume.
   */
  private static int parseLegacyOptions(final List<PostgresCatalogToken> tokens, int i, final Options options) {
    while (i < tokens.size()) {
      final PostgresCatalogToken token = tokens.get(i);
      if (token.type != PostgresCatalogToken.Type.IDENTIFIER)
        return i;
      final String keyword = token.text.toLowerCase(Locale.ENGLISH);
      switch (keyword) {
      case "binary" -> {
        options.set("format", PostgresCatalogToken.literal("binary"), null);
        i++;
      }
      case "csv" -> {
        options.set("format", PostgresCatalogToken.literal("csv"), null);
        i++;
      }
      case "header", "freeze" -> {
        options.set(keyword, null, null);
        i++;
      }
      case "delimiter", "null", "quote", "escape" -> {
        i++;
        if (i < tokens.size() && tokens.get(i).isKeyword("AS"))
          i++;
        if (i >= tokens.size())
          throw new CopyException("syntax error in COPY: " + token.text.toUpperCase(Locale.ENGLISH) + " needs a value",
              SQLSTATE_SYNTAX_ERROR);
        options.set(keyword, tokens.get(i++), null);
      }
      case "force" -> {
        i++;
        if (i < tokens.size() && tokens.get(i).isKeyword("QUOTE")) {
          i++;
          if (i < tokens.size() && tokens.get(i).isSymbol("*")) {
            options.set("force_quote", tokens.get(i++), null);
          } else {
            final List<String> list = new ArrayList<>();
            list.add(identifier(tokens, i++));
            while (i + 1 < tokens.size() && tokens.get(i).isSymbol(",")) {
              list.add(identifier(tokens, i + 1));
              i += 2;
            }
            options.set("force_quote", null, list);
          }
        } else
          throw new CopyException("syntax error in COPY at 'FORCE': only FORCE QUOTE applies to COPY TO", SQLSTATE_SYNTAX_ERROR);
      }
      case "oids" -> throw new CopyException("COPY OIDS is not supported", SQLSTATE_FEATURE_NOT_SUPPORTED);
      default -> {
        return i;
      }
      }
    }
    return i;
  }

  private static String identifier(final List<PostgresCatalogToken> tokens, final int i) {
    if (i >= tokens.size())
      throw new CopyException("syntax error in COPY: expected a name", SQLSTATE_SYNTAX_ERROR);
    final PostgresCatalogToken token = tokens.get(i);
    if (token.type != PostgresCatalogToken.Type.IDENTIFIER && token.type != PostgresCatalogToken.Type.QUOTED_IDENTIFIER)
      throw new CopyException("syntax error in COPY at '" + token.text + "': expected a name", SQLSTATE_SYNTAX_ERROR);
    // The table form splices its names into a SELECT between back-ticks, which ArcadeDB's SQL cannot escape inside
    // an identifier: a name holding one would end the identifier early and read the rest as SQL. No type or
    // property can be named that way, so there is nothing to lose by refusing it.
    if (token.text.indexOf('`') >= 0)
      throw new CopyException("syntax error in COPY: the name \"" + token.text + "\" cannot contain a back-tick", SQLSTATE_SYNTAX_ERROR);
    return token.text;
  }

  private static List<PostgresCatalogToken> tokenize(final String text) {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize(text);
    if (tokens == null)
      throw new CopyException("syntax error in COPY: unterminated string or comment", SQLSTATE_SYNTAX_ERROR);
    return tokens;
  }

  /**
   * The index of the {@code )} closing the {@code (} at {@code open}, read past nested parentheses, string
   * literals (with {@code ''} and, in {@code E'...'}, backslash escapes), quoted identifiers of either kind and
   * comments - anything the query inside may legitimately hold a stray parenthesis in.
   */
  static int matchingParenthesis(final String text, final int open) {
    int depth = 0;
    int i = open;
    final int length = text.length();
    while (i < length) {
      final char c = text.charAt(i);
      if (c == '(')
        depth++;
      else if (c == ')') {
        if (--depth == 0)
          return i;
      } else if (c == '\'') {
        final boolean escapes = i > 0 && (text.charAt(i - 1) == 'E' || text.charAt(i - 1) == 'e')
            && (i < 2 || !Character.isLetterOrDigit(text.charAt(i - 2)));
        i++;
        while (i < length) {
          final char ch = text.charAt(i);
          if (ch == '\\' && escapes)
            i++;
          else if (ch == '\'') {
            if (i + 1 < length && text.charAt(i + 1) == '\'')
              i++;
            else
              break;
          }
          i++;
        }
      } else if (c == '"' || c == '`') {
        final int end = text.indexOf(c, i + 1);
        i = end < 0 ? length : end;
      } else if (c == '-' && i + 1 < length && text.charAt(i + 1) == '-') {
        final int end = text.indexOf('\n', i);
        i = end < 0 ? length : end;
      } else if (c == '/' && i + 1 < length && text.charAt(i + 1) == '*') {
        final int end = text.indexOf("*/", i + 2);
        i = end < 0 ? length : end + 1;
      }
      i++;
    }
    throw new CopyException("syntax error in COPY: the query's opening '(' is never closed", SQLSTATE_SYNTAX_ERROR);
  }

  /**
   * The options as they are collected, in either spelling, and the validation PostgreSQL applies to the
   * combination ({@code ProcessCopyOptions} in {@code copy.c}) once all of them are known.
   */
  private static final class Options {
    private Format       format;
    private Character    delimiter;
    private String       nullString;
    private Boolean      header;
    private Character    quote;
    private Character    escape;
    private boolean      forceQuoteAll;
    private List<String> forceQuoteColumns;

    void set(final String name, final PostgresCatalogToken value, final List<String> list) {
      switch (name) {
      case "format" -> {
        final String v = requireValue(name, value).toLowerCase(Locale.ENGLISH);
        format = switch (v) {
          case "text" -> Format.TEXT;
          case "csv" -> Format.CSV;
          case "binary" -> Format.BINARY;
          default -> throw new CopyException("COPY format \"" + v + "\" not recognized", SQLSTATE_SYNTAX_ERROR);
        };
      }
      case "delimiter" -> delimiter = singleCharacter(name, requireValue(name, value));
      case "null" -> nullString = requireValue(name, value);
      case "quote" -> quote = singleCharacter(name, requireValue(name, value));
      case "escape" -> escape = singleCharacter(name, requireValue(name, value));
      case "header" -> {
        if (value == null)
          header = true;
        else if (value.text.equalsIgnoreCase("match"))
          throw new CopyException("cannot use \"match\" with HEADER in COPY TO", SQLSTATE_SYNTAX_ERROR);
        else
          header = bool(name, value.text);
      }
      case "force_quote" -> {
        if (list != null)
          forceQuoteColumns = list;
        else if (value != null && value.isSymbol("*"))
          forceQuoteAll = true;
        else
          throw new CopyException("syntax error in COPY option force_quote: expected * or a column list",
              SQLSTATE_SYNTAX_ERROR);
      }
      // Accepted and ignored: FREEZE has no meaning for COPY TO and ENCODING is always UTF-8 on this wire.
      case "freeze" -> {
        if (value != null)
          bool(name, value.text);
      }
      case "encoding" -> {
        final String v = requireValue(name, value).replace("-", "").replace("_", "").toLowerCase(Locale.ENGLISH);
        if (!v.equals("utf8") && !v.equals("unicode"))
          throw new CopyException("COPY encoding \"" + value.text + "\" is not supported by this server: results are always UTF-8",
              SQLSTATE_FEATURE_NOT_SUPPORTED);
      }
      case "log_verbosity" -> requireValue(name, value);
      case "default", "force_not_null", "force_null", "on_error", "reject_limit" ->
          throw new CopyException("COPY " + name.toUpperCase(Locale.ENGLISH) + " only applies to COPY FROM", SQLSTATE_SYNTAX_ERROR);
      case "oids" -> throw new CopyException("COPY OIDS is not supported", SQLSTATE_FEATURE_NOT_SUPPORTED);
      default -> throw new CopyException("option \"" + name + "\" not recognized", SQLSTATE_SYNTAX_ERROR);
      }
    }

    private static String requireValue(final String name, final PostgresCatalogToken value) {
      if (value == null)
        throw new CopyException("COPY option " + name + " requires a value", SQLSTATE_SYNTAX_ERROR);
      return value.text;
    }

    private static char singleCharacter(final String name, final String value) {
      if (value.length() != 1)
        throw new CopyException("COPY " + name + " must be a single one-byte character", SQLSTATE_SYNTAX_ERROR);
      return value.charAt(0);
    }

    private static boolean bool(final String name, final String value) {
      return switch (value.toLowerCase(Locale.ENGLISH)) {
        case "true", "on", "yes", "1", "t", "y" -> true;
        case "false", "off", "no", "0", "f", "n" -> false;
        default -> throw new CopyException(name + " requires a Boolean value", SQLSTATE_SYNTAX_ERROR);
      };
    }

    PostgresCopyStatement build(final String query) {
      final Format fmt = format != null ? format : Format.TEXT;
      if (fmt == Format.BINARY) {
        if (delimiter != null)
          throw new CopyException("cannot specify DELIMITER in BINARY mode", SQLSTATE_SYNTAX_ERROR);
        if (nullString != null)
          throw new CopyException("cannot specify NULL in BINARY mode", SQLSTATE_SYNTAX_ERROR);
        if (header != null)
          throw new CopyException("cannot specify HEADER in BINARY mode", SQLSTATE_SYNTAX_ERROR);
      }
      if (fmt != Format.CSV) {
        if (quote != null)
          throw new CopyException("COPY QUOTE requires CSV mode", SQLSTATE_SYNTAX_ERROR);
        if (escape != null)
          throw new CopyException("COPY ESCAPE requires CSV mode", SQLSTATE_SYNTAX_ERROR);
        if (forceQuoteAll || forceQuoteColumns != null)
          throw new CopyException("COPY FORCE_QUOTE requires CSV mode", SQLSTATE_SYNTAX_ERROR);
      }

      final char delim = delimiter != null ? delimiter : fmt == Format.CSV ? ',' : '\t';
      if (delim == '\r' || delim == '\n')
        throw new CopyException("COPY delimiter cannot be newline or carriage return", SQLSTATE_SYNTAX_ERROR);
      if (fmt == Format.TEXT && delim == '\\')
        throw new CopyException("COPY delimiter cannot be \"\\\" in text mode", SQLSTATE_SYNTAX_ERROR);

      final String nul = nullString != null ? nullString : fmt == Format.CSV ? "" : "\\N";
      if (nul.indexOf('\r') >= 0 || nul.indexOf('\n') >= 0)
        throw new CopyException("COPY null representation cannot use newline or carriage return", SQLSTATE_SYNTAX_ERROR);
      if (fmt == Format.TEXT && nul.indexOf(delim) >= 0)
        throw new CopyException("COPY delimiter cannot appear in the NULL specification", SQLSTATE_SYNTAX_ERROR);

      final char q = quote != null ? quote : '"';
      final char e = escape != null ? escape : q;
      if (fmt == Format.CSV && delim == q)
        throw new CopyException("COPY delimiter and quote must be different", SQLSTATE_SYNTAX_ERROR);

      final Set<String> forced;
      if (forceQuoteColumns != null) {
        forced = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        forced.addAll(forceQuoteColumns);
      } else
        forced = Collections.emptySet();

      return new PostgresCopyStatement(query, fmt, delim, nul, header != null && header, q, e, forceQuoteAll, forced);
    }
  }

  // ---- text and CSV encodings (PostgreSQL's own, copyto.c) ----

  /**
   * Appends the header line - the column names, encoded as a row of values - to {@code out}.
   */
  public void appendHeader(final StringBuilder out, final Collection<String> columnNames) {
    boolean first = true;
    for (final String name : columnNames) {
      if (!first)
        out.append(delimiter);
      first = false;
      if (format == Format.CSV)
        appendCsvValue(out, name, false, columnNames.size() == 1);
      else
        appendTextValue(out, name);
    }
    out.append('\n');
  }

  /**
   * Appends one row to {@code out}: the values in column order, null for SQL NULL, each in the text form the
   * column's Postgres type gives it.
   *
   * @param columnNames the names, in the same order, for FORCE_QUOTE
   */
  public void appendRow(final StringBuilder out, final String[] values, final String[] columnNames) {
    for (int i = 0; i < values.length; i++) {
      if (i > 0)
        out.append(delimiter);
      final String value = values[i];
      if (value == null)
        out.append(nullString);
      else if (format == Format.CSV)
        appendCsvValue(out, value, forceQuoteAll || forceQuoteColumns.contains(columnNames[i]), values.length == 1);
      else
        appendTextValue(out, value);
    }
    out.append('\n');
  }

  /**
   * Text format ({@code CopyAttributeOutText}): backslash escapes for the control characters, the backslash
   * itself and the delimiter, so the reader's unescape recovers the value exactly.
   */
  private void appendTextValue(final StringBuilder out, final String value) {
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      switch (c) {
      case '\b' -> out.append("\\b");
      case '\f' -> out.append("\\f");
      case '\n' -> out.append("\\n");
      case '\r' -> out.append("\\r");
      case '\t' -> out.append("\\t");
      case 0x0B -> out.append("\\v");
      case '\\' -> out.append("\\\\");
      default -> {
        if (c == delimiter)
          out.append('\\');
        out.append(c);
      }
      }
    }
  }

  /**
   * CSV format ({@code CopyAttributeOutCSV}): a value is quoted when it is forced, when it would otherwise read
   * back as the NULL string (which is how an empty string survives next to NULL), when it holds the delimiter,
   * the quote or a line break, or when it is {@code \.} alone on its line - the end-of-data marker. Inside the
   * quotes, the quote character is preceded by the escape character (itself, by default).
   */
  private void appendCsvValue(final StringBuilder out, final String value, final boolean forceQuote,
      final boolean singleColumn) {
    boolean useQuote = forceQuote || value.equals(nullString) || (singleColumn && value.equals("\\."));
    if (!useQuote)
      for (int i = 0; i < value.length(); i++) {
        final char c = value.charAt(i);
        if (c == delimiter || c == quote || c == '\n' || c == '\r') {
          useQuote = true;
          break;
        }
      }

    if (!useQuote) {
      out.append(value);
      return;
    }

    out.append(quote);
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      if (c == quote || c == escape)
        out.append(escape);
      out.append(c);
    }
    out.append(quote);
  }
}
