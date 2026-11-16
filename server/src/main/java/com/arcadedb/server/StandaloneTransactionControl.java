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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.handler.PostCommandHandler;

import java.util.logging.Level;

/**
 * Refuses a SQL {@code BEGIN}, {@code COMMIT} or {@code ROLLBACK} sent as a command of its own outside any transaction or session
 * of the caller (issue #12202). The server runs such a command in a transaction it opens around that one request, so the
 * statement acted on that wrapper and ended with it: a {@code BEGIN} opened nothing the next command joined, and a later
 * {@code ROLLBACK} was answered OK and undid nothing. A {@code sqlscript} that holds the whole block in one request, and a
 * statement inside the caller's own transaction or session, are not concerned.
 */
public final class StandaloneTransactionControl {

  private StandaloneTransactionControl() {
  }

  /**
   * Throws a {@link CommandSemanticException} (a client error on every protocol) when {@code command}, in {@code language}, is a
   * transaction control statement. Call it only for a request that runs outside a transaction or session of its caller.
   */
  public static void refuse(final Database database, final String language, final String command) {
    if (database == null || command == null || !"sql".equalsIgnoreCase(language) || !startsWithTransactionControl(command))
      return;

    final boolean transactionControl;
    try {
      transactionControl = database.getQueryEngine("sql").analyze(command.trim()).isTransactionControl();
    } catch (final CommandParsingException e) {
      // NOT A SINGLE STATEMENT THE PARSER ACCEPTS (A SCRIPT SENT AS "sql", A TYPO): THE EXECUTION REPORTS OR RUNS IT AS BEFORE
      LogManager.instance().log(StandaloneTransactionControl.class, Level.FINE,
          "Could not analyze the command to look for a transaction control: %s", e, e.getMessage());
      return;
    }

    if (transactionControl)
      throw new CommandSemanticException("'" + command.trim()
          + "' cannot run as a command of its own outside a transaction: each such request runs in its own transaction, which it "
          + "would begin, commit or roll back alone. Use the transaction API of the client (RemoteDatabase.begin(), commit(), "
          + "rollback() or transaction()), the HTTP /begin, /commit and /rollback endpoints with the arcadedb-session-id header, "
          + "or send the whole block as one 'sqlscript' command");
  }

  /** A prefix test, so every other command pays no parse: the text starts with BEGIN, COMMIT or ROLLBACK. */
  private static boolean startsWithTransactionControl(final String command) {
    final int start = PostCommandHandler.skipLeadingSpaceAndComments(command);
    return command.regionMatches(true, start, "begin", 0, 5) || command.regionMatches(true, start, "commit", 0, 6)
        || command.regionMatches(true, start, "rollback", 0, 8);
  }
}
