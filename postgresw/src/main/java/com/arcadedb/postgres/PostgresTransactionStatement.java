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

import com.arcadedb.database.Database;

import java.util.regex.Pattern;

/**
 * A transaction-control statement in PostgreSQL's own grammar (issue #9631), recognized by the wire protocol before
 * anything reaches the SQL engine, whose grammar only knows {@code BEGIN [ISOLATION <level>]}, {@code COMMIT [RETRY]}
 * and {@code ROLLBACK}:
 * <ul>
 *   <li>{@code BEGIN [ WORK | TRANSACTION ] [ transaction_mode [, ...] ]}</li>
 *   <li>{@code START TRANSACTION [ transaction_mode [, ...] ]}, tagged {@code START TRANSACTION}</li>
 *   <li>{@code { COMMIT | END } [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]}, tagged {@code COMMIT}</li>
 *   <li>{@code { ROLLBACK | ABORT } [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]}, tagged {@code ROLLBACK}</li>
 * </ul>
 * where {@code transaction_mode} is {@code ISOLATION LEVEL <level>}, {@code READ WRITE | READ ONLY} or
 * {@code [ NOT ] DEFERRABLE}, the same modes {@code SET TRANSACTION} takes, parsed by the same
 * {@link #isolationOfModes} so both refuse the same things the same way.
 * <p>
 * A statement PostgreSQL would accept but this server cannot honor - {@code READ ONLY}, {@code SERIALIZABLE},
 * {@code AND CHAIN} - is recognized with a {@link #refusal()} (SQLSTATE {@code 0A000}) rather than left to the SQL
 * grammar to refuse as a syntax error. Text that is none of these shapes - a comment, {@code BEGIN ISOLATION
 * READ_COMMITTED}, {@code COMMIT RETRY 3}, {@code ROLLBACK TO SAVEPOINT} - is not recognized at all, so the caller's
 * grammar fallback (issue #8273) still sees ArcadeDB's own spellings.
 */
final class PostgresTransactionStatement {
  static final String TAG_START_TRANSACTION = "START TRANSACTION";

  private static final Pattern WHITESPACE     = Pattern.compile("\\s+");
  private static final Pattern MODE_SEPARATOR = Pattern.compile("[\\s,]+");
  private static final Pattern EMPTY_MODE     = Pattern.compile(",\\s*,");

  final PostgresPortal.TransactionControl    control;
  /** The level a BEGIN / START TRANSACTION asked for, or null for the database default. */
  final Database.TRANSACTION_ISOLATION_LEVEL isolation;
  /** The command tag PostgreSQL answers this statement with. */
  final String                               tag;
  private final PostgresSessionSettings.SettingException refusal;

  private PostgresTransactionStatement(final PostgresPortal.TransactionControl control,
      final Database.TRANSACTION_ISOLATION_LEVEL isolation, final String tag, final PostgresSessionSettings.SettingException refusal) {
    this.control = control;
    this.isolation = isolation;
    this.tag = tag;
    this.refusal = refusal;
  }

  /**
   * The refusal of a recognized statement this server cannot honor, or null when it can be applied.
   */
  PostgresSessionSettings.SettingException refusal() {
    return refusal;
  }

  /**
   * Throws the refusal, if any, so a caller that dispatches on the statement answers it with an ErrorResponse.
   */
  PostgresTransactionStatement checkSupported() {
    if (refusal != null)
      throw refusal;
    return this;
  }

  /**
   * True for a COMMIT/ROLLBACK this server can apply: the statement that ends a block, the only one PostgreSQL still
   * accepts while the block is aborted.
   */
  boolean endsBlock() {
    return refusal == null && control != PostgresPortal.TransactionControl.BEGIN;
  }

  /**
   * Recognizes a transaction-control statement.
   *
   * @param upperCaseText the statement upper-cased, trimmed and without its trailing {@code ;}
   *
   * @return the statement, or null when the text is not one of the shapes above
   */
  static PostgresTransactionStatement parse(final String upperCaseText) {
    if (upperCaseText.isEmpty())
      return null;
    // Cheap rejection first: this runs on every statement the wire dispatches
    switch (upperCaseText.charAt(0)) {
    case 'A', 'B', 'C', 'E', 'R', 'S' -> {
    }
    default -> {
      return null;
    }
    }
    int keywordEnd = 0;
    while (keywordEnd < upperCaseText.length() && !Character.isWhitespace(upperCaseText.charAt(keywordEnd)))
      // No keyword below is longer than ROLLBACK
      if (++keywordEnd > 8)
        return null;
    // Matched in place, so a statement that only shares a first letter with a keyword (SELECT, CREATE, SET...)
    // allocates nothing
    if (isKeyword(upperCaseText, keywordEnd, "BEGIN"))
      return begin(upperCaseText, "BEGIN", restOf(upperCaseText, keywordEnd), true);
    if (isKeyword(upperCaseText, keywordEnd, "START"))
      return begin(upperCaseText, TAG_START_TRANSACTION, restOf(upperCaseText, keywordEnd), false);
    if (isKeyword(upperCaseText, keywordEnd, "COMMIT"))
      return end(PostgresPortal.TransactionControl.COMMIT, "COMMIT", restOf(upperCaseText, keywordEnd));
    if (isKeyword(upperCaseText, keywordEnd, "END"))
      return end(PostgresPortal.TransactionControl.COMMIT, "END", restOf(upperCaseText, keywordEnd));
    if (isKeyword(upperCaseText, keywordEnd, "ROLLBACK"))
      return end(PostgresPortal.TransactionControl.ROLLBACK, "ROLLBACK", restOf(upperCaseText, keywordEnd));
    if (isKeyword(upperCaseText, keywordEnd, "ABORT"))
      return end(PostgresPortal.TransactionControl.ROLLBACK, "ABORT", restOf(upperCaseText, keywordEnd));
    return null;
  }

  private static boolean isKeyword(final String text, final int keywordEnd, final String keyword) {
    return keywordEnd == keyword.length() && text.startsWith(keyword);
  }

  private static String restOf(final String text, final int keywordEnd) {
    return text.substring(keywordEnd).trim();
  }

  private static PostgresTransactionStatement begin(final String query, final String tag, final String rest,
      final boolean transactionKeywordOptional) {
    // PostgreSQL separates the modes with a comma OR with whitespace (transaction_mode_list : item | list ',' item |
    // list item), so both are separators here. A comma that separates nothing - leading, trailing or doubled - is a
    // syntax error in PostgreSQL, and is left to the grammar here, which refuses it the same way
    if (!rest.isEmpty() && (rest.charAt(0) == ',' || rest.charAt(rest.length() - 1) == ',' || EMPTY_MODE.matcher(rest).find()))
      return null;
    final String[] tokens = rest.isEmpty() ? new String[0] : MODE_SEPARATOR.split(rest);
    int i = 0;
    if (transactionKeywordOptional) {
      if (i < tokens.length && ("WORK".equals(tokens[i]) || "TRANSACTION".equals(tokens[i])))
        ++i;
    } else if (i < tokens.length && "TRANSACTION".equals(tokens[i]))
      ++i;
    else
      return null;
    // A comma after WORK/TRANSACTION separates nothing either (BEGIN TRANSACTION, READ WRITE)
    if (i == 1 && rest.length() > tokens[0].length() && rest.substring(tokens[0].length()).trim().startsWith(","))
      return null;

    final String isolationName;
    try {
      isolationName = isolationOfModes(query, tokens, i);
    } catch (final PostgresSessionSettings.SettingException e) {
      if (PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR.equals(e.sqlState))
        // Not PostgreSQL's spelling: left to the SQL grammar, which accepts ArcadeDB's own (BEGIN ISOLATION <level>)
        return null;
      return new PostgresTransactionStatement(PostgresPortal.TransactionControl.BEGIN, null, tag, e);
    }

    if (isolationName == null)
      return new PostgresTransactionStatement(PostgresPortal.TransactionControl.BEGIN, null, tag, null);
    return switch (isolationName) {
      case "repeatable read" ->
          new PostgresTransactionStatement(PostgresPortal.TransactionControl.BEGIN, Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ, tag,
              null);
      case "serializable" -> new PostgresTransactionStatement(PostgresPortal.TransactionControl.BEGIN, null, tag,
          new PostgresSessionSettings.SettingException(
              "transaction isolation level \"serializable\" is not supported by this server: transactions run at \"read committed\" or \"repeatable read\"",
              PostgresSessionSettings.SQLSTATE_FEATURE_NOT_SUPPORTED));
      // READ UNCOMMITTED behaves as READ COMMITTED, as in PostgreSQL
      case "read committed", "read uncommitted" ->
          new PostgresTransactionStatement(PostgresPortal.TransactionControl.BEGIN, Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED, tag,
              null);
      default -> throw new IllegalStateException("Unmapped transaction isolation level '" + isolationName + "'");
    };
  }

  private static PostgresTransactionStatement end(final PostgresPortal.TransactionControl control, final String keyword,
      final String rest) {
    final String tag = control.name();
    // The split allocates, which is fine: only a statement whose first word is a transaction keyword gets this far
    final String[] tokens = rest.isEmpty() ? new String[0] : WHITESPACE.split(rest);
    int i = 0;
    if (i < tokens.length && ("WORK".equals(tokens[i]) || "TRANSACTION".equals(tokens[i])))
      ++i;
    if (i == tokens.length)
      return new PostgresTransactionStatement(control, null, tag, null);

    if (!"AND".equals(tokens[i++]) || i >= tokens.length)
      return null;
    final boolean chain;
    if ("NO".equals(tokens[i])) {
      chain = false;
      ++i;
    } else
      chain = true;
    if (i != tokens.length - 1 || !"CHAIN".equals(tokens[i]))
      return null;

    if (!chain)
      // AND NO CHAIN is the default, spelled out
      return new PostgresTransactionStatement(control, null, tag, null);
    return new PostgresTransactionStatement(control, null, tag,
        new PostgresSessionSettings.SettingException(keyword + " AND CHAIN is not supported by this server: end the transaction, then open a new one",
            PostgresSessionSettings.SQLSTATE_FEATURE_NOT_SUPPORTED));
  }

  /**
   * The isolation level a list of transaction modes asks for, spelled as PostgreSQL reports it (e.g.
   * {@code read committed}), or null when it names none. {@code READ WRITE} and {@code [NOT] DEFERRABLE} change nothing
   * here ({@code DEFERRABLE} only matters to a SERIALIZABLE READ ONLY transaction), and {@code READ ONLY} is refused.
   *
   * @param query  the whole statement, for the syntax-error message
   * @param tokens the statement split on whitespace and commas
   * @param from   the index of the first mode
   *
   * @throws PostgresSessionSettings.SettingException {@code 42601} for a malformed mode list, {@code 0A000} for
   *                                                  {@code READ ONLY}
   */
  static String isolationOfModes(final String query, final String[] tokens, final int from) {
    String isolation = null;
    int i = from;
    while (i < tokens.length) {
      final String token = tokens[i++];
      switch (token) {
      case "ISOLATION" -> {
        if (i >= tokens.length || !"LEVEL".equals(tokens[i++]) || i >= tokens.length)
          throw syntaxError(query);
        final String first = tokens[i++];
        if ("SERIALIZABLE".equals(first))
          isolation = "serializable";
        else {
          if (i >= tokens.length)
            throw syntaxError(query);
          final String second = tokens[i++];
          if ("REPEATABLE".equals(first) && "READ".equals(second))
            isolation = "repeatable read";
          else if ("READ".equals(first) && ("COMMITTED".equals(second) || "UNCOMMITTED".equals(second)))
            isolation = "read " + ("COMMITTED".equals(second) ? "committed" : "uncommitted");
          else
            throw syntaxError(query);
        }
      }
      case "READ" -> {
        if (i >= tokens.length)
          throw syntaxError(query);
        final String access = tokens[i++];
        if ("ONLY".equals(access))
          throw new PostgresSessionSettings.SettingException(PostgresSessionSettings.READ_ONLY_NOT_SUPPORTED,
              PostgresSessionSettings.SQLSTATE_FEATURE_NOT_SUPPORTED);
        if (!"WRITE".equals(access))
          throw syntaxError(query);
      }
      case "NOT" -> {
        if (i >= tokens.length || !"DEFERRABLE".equals(tokens[i++]))
          throw syntaxError(query);
      }
      case "DEFERRABLE" -> {
      }
      default -> throw syntaxError(query);
      }
    }
    return isolation;
  }

  static PostgresSessionSettings.SettingException syntaxError(final String query) {
    return new PostgresSessionSettings.SettingException("syntax error in \"" + query + "\"", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR);
  }
}
