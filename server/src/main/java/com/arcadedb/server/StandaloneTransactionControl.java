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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.handler.PostCommandHandler;

import java.util.Locale;
import java.util.Set;
import java.util.logging.Level;

/**
 * Refuses a SQL {@code BEGIN}, {@code COMMIT} or {@code ROLLBACK} (issue #12202), or an openCypher {@code START TRANSACTION},
 * {@code COMMIT} or {@code ROLLBACK} (issue #12232), sent as a command of its own outside any transaction or session of the
 * caller. The server runs such a command in a transaction it opens around that one request, so the statement acted on that
 * wrapper and ended with it: a {@code BEGIN} opened nothing the next command joined, and a later {@code ROLLBACK} was answered OK
 * and undid nothing. A {@code sqlscript} that holds the whole block in one request, and a statement inside the caller's own
 * transaction or session, are not concerned.
 */
public final class StandaloneTransactionControl {

  /** The languages whose engines answer {@code isTransactionControl()}; a script language runs the whole block in one request. */
  private static final Set<String> LANGUAGES = Set.of("sql", "opencypher", "cypher");

  private StandaloneTransactionControl() {
  }

  /**
   * Throws a {@link CommandSemanticException} (a client error on every protocol) when {@code command}, in {@code language} (SQL or
   * openCypher), is a transaction control statement. Call it only for a request that runs outside a transaction or session of its
   * caller.
   */
  public static void refuse(final Database database, final String language, final String command) {
    if (database == null || command == null || language == null || !LANGUAGES.contains(language.toLowerCase(Locale.ENGLISH))
        || !startsWithTransactionControl(command))
      return;

    final boolean transactionControl;
    try {
      transactionControl = database.getQueryEngine(language).analyze(command.trim()).isTransactionControl();
    } catch (final Exception e) {
      // Not a single statement the parser accepts (a script sent as "sql", a typo): the execution reports it, or runs it, as before
      LogManager.instance().log(StandaloneTransactionControl.class, Level.FINE,
          "Could not analyze the command to look for a transaction control: %s", e, e.getMessage());
      return;
    }

    if (transactionControl)
      throw new CommandSemanticException("'" + command.trim()
          + "' cannot run as a command of its own outside a transaction: each such request runs in its own transaction, which it "
          + "would begin, commit or roll back alone. Use the transaction API of the client (RemoteDatabase.begin() or "
          + "RemoteGrpcDatabase.begin(), then commit() or rollback(), or transaction()), the HTTP /begin, /commit and /rollback "
          + "endpoints with the arcadedb-session-id header, or, in SQL, send the whole block as one 'sqlscript' command");
  }

  /**
   * A prefix test, so every other command pays no parse: the text starts with BEGIN, START, COMMIT or ROLLBACK, also behind an
   * EXPLAIN or PROFILE prefix. SQL PROFILE and both openCypher prefixes run the transaction control they wrap; the engine's own
   * {@code isTransactionControl()} then decides, so a SQL EXPLAIN, which only plans, is not refused.
   */
  private static boolean startsWithTransactionControl(final String command) {
    final int start = PostCommandHandler.skipLeadingSpaceAndComments(command);
    if ((command.regionMatches(true, start, "explain", 0, 7) || command.regionMatches(true, start, "profile", 0, 7))
        && command.length() > start + 7 && Character.isWhitespace(command.charAt(start + 7))) {
      final String wrapped = command.substring(start + 7);
      return startsWithTransactionControlWord(wrapped, PostCommandHandler.skipLeadingSpaceAndComments(wrapped));
    }
    return startsWithTransactionControlWord(command, start);
  }

  /**
   * Matches the four words as bare prefixes, so a text like "beginning" or "started" passes the probe too. That is deliberate: the
   * probe only decides whether the text is worth a parse, and the engine's own {@code isTransactionControl()} decides afterwards.
   */
  private static boolean startsWithTransactionControlWord(final String command, final int start) {
    return command.regionMatches(true, start, "begin", 0, 5) || command.regionMatches(true, start, "start", 0, 5)
        || command.regionMatches(true, start, "commit", 0, 6) || command.regionMatches(true, start, "rollback", 0, 8);
  }
}
