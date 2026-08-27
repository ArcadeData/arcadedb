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
package com.arcadedb.console;

import java.util.Locale;

/**
 * Hides the plaintext password that the console's own command syntax carries inline, so it does not reach the two places
 * that outlive the session: the `.history` file the line reader saves after every command, and the command echo that batch
 * mode writes to stdout - straight into a build log (issue #6829).
 * <p>
 * Two command shapes carry a password. Anything that opens a remote connection (`connect`, but also `list databases`,
 * `create database` and `drop database` against a `remote:` URL) takes it as the third whitespace-separated token after the
 * URL, and `create user ... identified by ...` takes everything between `identified by` and the optional
 * `grant connect to`.
 * <p>
 * The password is replaced with {@link #MASK} rather than the line being dropped: history keeps the host and the user name,
 * which is the part worth recalling, and a `***` in a build log says plainly that something was hidden instead of leaving
 * the reader to wonder whether the command was simply typed without credentials. A recalled masked line is not runnable as
 * is - it would send `***` as the password - which is why {@code Console} accepts the credentials-less form of both
 * commands and asks for the password with the echo masked instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ConsoleCredentials {
  public static final  String   MASK              = "***";
  private static final String   REMOTE_PREFIX     = "remote:";
  private static final String   IDENTIFIED_BY     = "identified by";
  private static final String   GRANT_CONNECT     = "grant connect to";
  // THE COMMANDS THAT OPEN A REMOTE CONNECTION, I.E. THE ONES Console.execute() ROUTES TO connectToRemoteServer()
  private static final String[] REMOTE_COMMANDS   = { "connect", "list databases", "create database", "drop database" };
  private static final String   CREATE_USER       = "create user";

  private ConsoleCredentials() {
  }

  /**
   * Returns the text with every inline password replaced by {@link #MASK}, or the text itself when it carries none.
   * <p>
   * The text is examined one statement at a time - a console line can carry several, separated by semicolons - and only a
   * statement that actually starts with a credential-bearing command is touched. Matching on the command keyword rather
   * than on `remote:` or `identified by` anywhere in the text is what keeps an ordinary
   * {@code insert into Doc set note = 'identified by the auditor'} from being mangled on its way to the log.
   */
  public static String mask(final String text) {
    if (text == null || text.isEmpty())
      return text;

    final StringBuilder masked = new StringBuilder(text.length());
    for (int pos = 0; pos < text.length(); ) {
      final int end = endOfStatement(text, pos);
      masked.append(maskStatement(text.substring(pos, end)));
      if (end < text.length())
        masked.append(text.charAt(end));
      pos = end + 1;
    }
    return masked.toString();
  }

  /**
   * Returns true when {@link #mask} would change the text, i.e. when it carries a password in plaintext.
   */
  public static boolean carriesPassword(final String text) {
    return text != null && !text.equals(mask(text));
  }

  private static String maskStatement(final String statement) {
    final String lowerCase = statement.trim().toLowerCase(Locale.ENGLISH);

    for (final String command : REMOTE_COMMANDS)
      if (lowerCase.startsWith(command))
        return maskRemoteCredentials(statement);

    if (lowerCase.startsWith(CREATE_USER))
      return maskIdentifiedBy(statement);

    return statement;
  }

  /**
   * Masks the third token after a `remote:` URL: `connect remote:host/db root secret` -&gt; `connect remote:host/db root ***`.
   * <p>
   * The mask runs to the end of the statement rather than to the end of that token, because a password is allowed to
   * contain spaces (issue #6830) and stopping at the first blank would leave the rest of it in the clear.
   */
  private static String maskRemoteCredentials(final String statement) {
    final int urlStart = indexOfRemoteUrl(statement);
    if (urlStart < 0)
      return statement;

    // SKIP THE URL, THEN THE USER NAME: WHAT FOLLOWS IS THE PASSWORD
    int pos = skipBlanks(statement, endOfToken(statement, urlStart));
    if (pos == statement.length())
      return statement;

    pos = skipBlanks(statement, endOfToken(statement, pos));
    if (pos == statement.length())
      // NO PASSWORD ON THIS LINE: NOTHING TO HIDE
      return statement;

    int end = statement.length();
    while (end > pos && Character.isWhitespace(statement.charAt(end - 1)))
      --end;

    return statement.substring(0, pos) + MASK + statement.substring(end);
  }

  /**
   * Masks everything between `identified by` and either `grant connect to` or the end of the statement.
   */
  private static String maskIdentifiedBy(final String statement) {
    final String lowerCase = statement.toLowerCase(Locale.ENGLISH);

    final int identifiedByPos = lowerCase.indexOf(IDENTIFIED_BY);
    if (identifiedByPos < 0)
      return statement;

    final int start = skipBlanks(statement, identifiedByPos + IDENTIFIED_BY.length());

    int end = lowerCase.indexOf(GRANT_CONNECT, start);
    if (end < 0)
      end = statement.length();
    // KEEP THE BLANKS THAT SEPARATE THE PASSWORD FROM WHAT FOLLOWS IT
    while (end > start && Character.isWhitespace(statement.charAt(end - 1)))
      --end;

    if (end <= start)
      // THE PASSWORD WAS ALREADY OMITTED
      return statement;

    return statement.substring(0, start) + MASK + statement.substring(end);
  }

  /**
   * Returns the offset of the first `remote:` token of the statement, or -1. Only a token START counts.
   */
  private static int indexOfRemoteUrl(final String statement) {
    final String lowerCase = statement.toLowerCase(Locale.ENGLISH);
    for (int pos = lowerCase.indexOf(REMOTE_PREFIX); pos >= 0; pos = lowerCase.indexOf(REMOTE_PREFIX, pos + 1))
      if (pos == 0 || Character.isWhitespace(statement.charAt(pos - 1)))
        return pos;
    return -1;
  }

  private static int skipBlanks(final String text, int pos) {
    while (pos < text.length() && Character.isWhitespace(text.charAt(pos)))
      ++pos;
    return pos;
  }

  private static int endOfToken(final String text, int pos) {
    while (pos < text.length() && !Character.isWhitespace(text.charAt(pos)))
      ++pos;
    return pos;
  }

  /**
   * Returns the offset of the semicolon that ends the statement starting at {@code pos}, or the length of the text when
   * there is none. A semicolon inside a quoted string does not end anything, and a backslash escapes the character that
   * follows it, matching how {@link TerminalParser} splits the very same text into commands.
   */
  private static int endOfStatement(final String text, final int pos) {
    char quote = 0;
    for (int i = pos; i < text.length(); ++i) {
      final char c = text.charAt(i);
      if (c == '\\')
        ++i;
      else if (quote != 0) {
        if (c == quote)
          quote = 0;
      } else if (c == '\'' || c == '"')
        quote = c;
      else if (c == ';')
        return i;
    }
    return text.length();
  }
}
