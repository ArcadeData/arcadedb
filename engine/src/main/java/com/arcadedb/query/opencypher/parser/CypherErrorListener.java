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
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.misc.IntervalSet;

import java.util.List;

/**
 * Custom ANTLR error listener that converts parsing errors to CommandParsingException.
 * <p>
 * ANTLR spells a syntax error out as the full set of tokens that would have been legal at the
 * offending position. In Cypher almost every keyword doubles as a valid identifier, so wherever an
 * expression or a name is expected that set is 300+ tokens long and the resulting message is a wall
 * of token names that hides the one thing the user needs: what was expected. This listener collapses
 * such a set into the concept it stands for ("an expression", "a name"), the way the reference
 * implementations phrase it, and only falls back to enumerating alternatives when the set is short
 * enough to stay readable.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CypherErrorListener extends BaseErrorListener {
  /** Beyond this many alternatives ANTLR's own enumeration stops being readable. */
  private static final int    MAX_LISTED_ALTERNATIVES = 8;
  private static final String EXPECTING               = " expecting ";

  @Override
  public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line,
      final int charPositionInLine, final String msg, final RecognitionException e) {

    final String errorMsg = String.format("Syntax error at line %d:%d - %s", line, charPositionInLine,
        describe(recognizer, msg, e));
    throw new CommandParsingException(errorMsg, e);
  }

  /**
   * Rewrites ANTLR's message when its {@code expecting {...}} tail is too long to be useful. Any
   * message without such a tail (for instance {@code no viable alternative at input '...'}) is left
   * untouched.
   */
  private static String describe(final Recognizer<?, ?> recognizer, final String msg, final RecognitionException e) {
    final int expectingAt = msg == null ? -1 : msg.indexOf(EXPECTING);
    if (expectingAt < 0 || !(recognizer instanceof final Parser parser))
      return msg;

    final IntervalSet expected = expectedTokens(parser, e);
    if (expected == null || expected.size() <= MAX_LISTED_ALTERNATIVES)
      // ANTLR already listed few enough alternatives to be helpful, keep its wording
      return msg;

    return msg.substring(0, expectingAt) + ", expected: " + summarize(parser, expected);
  }

  private static IntervalSet expectedTokens(final Parser parser, final RecognitionException e) {
    try {
      // On the "extraneous/missing token" paths ANTLR reports no exception, but the parser is still
      // sitting on the offending state, so its expected set is the one that produced the message.
      return e != null ? e.getExpectedTokens() : parser.getExpectedTokens();
    } catch (final RuntimeException ignore) {
      // Never let error reporting mask the error it is reporting
      return null;
    }
  }

  /**
   * Names the concept a large expected set stands for, falling back to a capped enumeration.
   */
  private static String summarize(final Parser parser, final IntervalSet expected) {
    final Concepts concepts = Concepts.INSTANCE;

    // Most specific first: the name set is a subset of the expression set
    if (covers(concepts.name, expected))
      return "a name";
    if (covers(concepts.expression, expected))
      return "an expression";

    return enumerate(parser.getVocabulary(), expected);
  }

  /** True when every expected token is part of the concept's FIRST set. */
  private static boolean covers(final IntervalSet concept, final IntervalSet expected) {
    return concept != null && expected.subtract(concept).isNil();
  }

  private static String enumerate(final Vocabulary vocabulary, final IntervalSet expected) {
    final List<Integer> types = expected.toList();
    final StringBuilder buffer = new StringBuilder("one of ");

    for (int i = 0; i < MAX_LISTED_ALTERNATIVES; i++) {
      if (i > 0)
        buffer.append(", ");
      buffer.append(vocabulary.getDisplayName(types.get(i)));
    }

    return buffer.append(" or ").append(types.size() - MAX_LISTED_ALTERNATIVES).append(" more").toString();
  }

  /**
   * FIRST sets of the grammar rules a large expected set collapses to, computed once on the first
   * syntax error (the ATN is already deserialized by then, so this costs no start-up time).
   */
  private static final class Concepts {
    static final Concepts INSTANCE = new Concepts();

    final IntervalSet name;
    final IntervalSet expression;

    private Concepts() {
      final ATN atn = Cypher25Parser._ATN;
      name = first(atn, Cypher25Parser.RULE_symbolicNameString);
      expression = first(atn, Cypher25Parser.RULE_expression);
    }

    private static IntervalSet first(final ATN atn, final int rule) {
      try {
        return atn.nextTokens(atn.ruleToStartState[rule]);
      } catch (final RuntimeException ignore) {
        return null;
      }
    }
  }
}
