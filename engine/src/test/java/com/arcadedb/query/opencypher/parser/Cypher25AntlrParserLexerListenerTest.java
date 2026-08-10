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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.exception.CommandParsingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5958: {@code Cypher25AntlrParser.parseQuery()} attached a
 * {@link CypherErrorListener} to the {@code Cypher25Parser} but never to the {@code Cypher25Lexer}, unlike the SQL
 * side ({@code SQLAntlrParser}, fixed for the same gap by #5951/#5957). A lexer-level tokenization failure - an
 * unterminated string, an unterminated block comment, an unterminated backtick-quoted name - was therefore left to
 * ANTLR's default {@code ConsoleErrorListener} (prints to stderr, never throws), with the lexer's default recovery
 * silently emitting whatever token its {@code ErrorChar : . ;} catch-all rule produces and carrying on.
 * <p>
 * On the CURRENT grammar this is caught downstream: {@code ErrorChar} always yields <em>some</em> token for any
 * input character, so the lexer never truly fails to tokenize, and the garbled token stream that results from an
 * unterminated construct almost always fails the parser's own grammar rules - which is why every case below already
 * threw a {@link CommandParsingException} before this fix, just via the parser's listener rather than the lexer's
 * (confirmed by running this exact test against the pre-fix code). The fix is still correct to make: it is dead
 * code that costs nothing to attach, it matches the SQL side for maintainers reading both, and it is the difference
 * between "still throws" and "silently parses something else" the moment a future grammar change adds a lexer rule
 * that CAN fail to complete a token (the way SQL's {@code \\uXXXX} escape validation does) - {@code EscapeSequence}
 * here currently accepts any character after a backslash, which is why no such rule exists today. This test pins
 * the "still throws" behavior so a future grammar change is caught the moment it stops holding.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Cypher25AntlrParserLexerListenerTest {

  private final Cypher25AntlrParser parser = new Cypher25AntlrParser();

  @Test
  void unterminatedStringLiteralRaisesParsingException() {
    assertThatThrownBy(() -> parser.parseQuery("RETURN 'unterminated string"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void unterminatedStringLiteralInsideWhereClauseRaisesParsingException() {
    assertThatThrownBy(() -> parser.parseQuery("MATCH (n) WHERE n.name = 'oops RETURN n"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void unterminatedBlockCommentRaisesParsingException() {
    assertThatThrownBy(() -> parser.parseQuery("/* unterminated comment\nRETURN 1"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void unterminatedBacktickQuotedNameRaisesParsingException() {
    assertThatThrownBy(() -> parser.parseQuery("RETURN `unterminated backtick"))
        .isInstanceOf(CommandParsingException.class);
  }
}
