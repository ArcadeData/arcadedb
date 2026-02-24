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
package com.arcadedb.engine.timeseries.promql;

import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.AggOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.AggregationExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.FunctionCallExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.LabelMatcher;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.MatchOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.MatrixSelector;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.NumberLiteral;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.StringLiteral;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.UnaryExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.VectorSelector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Recursive-descent parser for PromQL expressions.
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PromQLParser {

  private static final Set<String> AGG_OPS          = Set.of("sum", "avg", "min", "max", "count", "topk", "bottomk");
  private static final int        MAX_PARSE_DEPTH  = 128;

  private final Lexer lexer;
  private       int   parseDepth;

  public PromQLParser(final String input) {
    this.lexer = new Lexer(input);
  }

  public PromQLExpr parse() {
    final PromQLExpr expr = parseOr();
    if (lexer.hasMore())
      throw new IllegalArgumentException("Unexpected token: '" + lexer.peek() + "' at position " + lexer.pos);
    return expr;
  }

  // --- Operator precedence chain ---

  private PromQLExpr parseOr() {
    if (++parseDepth > MAX_PARSE_DEPTH)
      throw new IllegalArgumentException("PromQL expression exceeds maximum nesting depth of " + MAX_PARSE_DEPTH);
    try {
    PromQLExpr left = parseAndUnless();
    while (lexer.matchKeyword("or")) {
      left = new BinaryExpr(left, BinaryOp.OR, parseAndUnless());
    }
    return left;
    } finally {
      parseDepth--;
    }
  }

  private PromQLExpr parseAndUnless() {
    PromQLExpr left = parseComparison();
    while (true) {
      if (lexer.matchKeyword("and"))
        left = new BinaryExpr(left, BinaryOp.AND, parseComparison());
      else if (lexer.matchKeyword("unless"))
        left = new BinaryExpr(left, BinaryOp.UNLESS, parseComparison());
      else
        break;
    }
    return left;
  }

  private PromQLExpr parseComparison() {
    PromQLExpr left = parseAddSub();
    while (true) {
      final BinaryOp op;
      if (lexer.match("=="))
        op = BinaryOp.EQ;
      else if (lexer.match("!="))
        op = BinaryOp.NEQ;
      else if (lexer.match("<="))
        op = BinaryOp.LTE;
      else if (lexer.match(">="))
        op = BinaryOp.GTE;
      else if (lexer.match("<"))
        op = BinaryOp.LT;
      else if (lexer.match(">"))
        op = BinaryOp.GT;
      else
        break;
      left = new BinaryExpr(left, op, parseAddSub());
    }
    return left;
  }

  private PromQLExpr parseAddSub() {
    PromQLExpr left = parseMulDiv();
    while (true) {
      if (lexer.match("+"))
        left = new BinaryExpr(left, BinaryOp.ADD, parseMulDiv());
      else if (lexer.matchMinus())
        left = new BinaryExpr(left, BinaryOp.SUB, parseMulDiv());
      else
        break;
    }
    return left;
  }

  private PromQLExpr parseMulDiv() {
    PromQLExpr left = parsePow();
    while (true) {
      if (lexer.match("*"))
        left = new BinaryExpr(left, BinaryOp.MUL, parsePow());
      else if (lexer.match("/"))
        left = new BinaryExpr(left, BinaryOp.DIV, parsePow());
      else if (lexer.match("%"))
        left = new BinaryExpr(left, BinaryOp.MOD, parsePow());
      else
        break;
    }
    return left;
  }

  private PromQLExpr parsePow() {
    PromQLExpr left = parseUnary();
    if (lexer.match("^")) {
      if (++parseDepth > MAX_PARSE_DEPTH)
        throw new IllegalArgumentException("PromQL expression exceeds maximum nesting depth of " + MAX_PARSE_DEPTH);
      try {
        left = new BinaryExpr(left, BinaryOp.POW, parsePow()); // right-associative
      } finally {
        parseDepth--;
      }
    }
    return left;
  }

  private PromQLExpr parseUnary() {
    if (lexer.matchMinus()) {
      if (++parseDepth > MAX_PARSE_DEPTH)
        throw new IllegalArgumentException("PromQL expression exceeds maximum nesting depth of " + MAX_PARSE_DEPTH);
      try {
        return new UnaryExpr('-', parseUnary());
      } finally {
        parseDepth--;
      }
    }
    if (lexer.match("+")) {
      if (++parseDepth > MAX_PARSE_DEPTH)
        throw new IllegalArgumentException("PromQL expression exceeds maximum nesting depth of " + MAX_PARSE_DEPTH);
      try {
        return parseUnary();
      } finally {
        parseDepth--;
      }
    }
    return parsePrimary();
  }

  private PromQLExpr parsePrimary() {
    lexer.skipWhitespace();

    // Parenthesized expression
    if (lexer.match("(")) {
      final PromQLExpr expr = parseOr();
      lexer.expect(")");
      return maybeMatrixOrOffset(expr);
    }

    // String literal
    if (lexer.peekChar() == '"' || lexer.peekChar() == '\'')
      return new StringLiteral(lexer.readString());

    // Number literal
    if (isNumberStart())
      return new NumberLiteral(lexer.readNumber());

    // Identifier: could be aggregation, function, or metric name
    final String ident = lexer.readIdent();
    if (ident.isEmpty())
      throw new IllegalArgumentException("Expected expression at position " + lexer.pos);

    final String lower = ident.toLowerCase();

    // Aggregation operator
    if (AGG_OPS.contains(lower))
      return parseAggregation(lower);

    // Function call or vector selector
    lexer.skipWhitespace();
    if (lexer.peekChar() == '(' && !lexer.peekChar(1, '{'))
      return parseFunctionCall(ident);

    // Vector selector
    return parseVectorSelector(ident);
  }

  private boolean isNumberStart() {
    final char c = lexer.peekChar();
    if (c >= '0' && c <= '9')
      return true;
    // Check for .5 style numbers
    return c == '.' && lexer.pos + 1 < lexer.input.length() && lexer.input.charAt(lexer.pos + 1) >= '0'
        && lexer.input.charAt(lexer.pos + 1) <= '9';
  }

  private PromQLExpr parseAggregation(final String opName) {
    final AggOp op = AggOp.valueOf(opName.toUpperCase());

    lexer.skipWhitespace();
    List<String> groupLabels = List.of();
    boolean without = false;

    // Check for by/without BEFORE the parenthesized expression
    if (lexer.matchKeyword("by")) {
      groupLabels = parseLabelList();
    } else if (lexer.matchKeyword("without")) {
      without = true;
      groupLabels = parseLabelList();
    }

    lexer.skipWhitespace();
    lexer.expect("(");
    PromQLExpr param = null;

    // topk/bottomk have a parameter
    if (op == AggOp.TOPK || op == AggOp.BOTTOMK) {
      param = parseOr();
      lexer.expect(",");
    }

    final PromQLExpr expr = parseOr();
    lexer.expect(")");

    // Check for by/without AFTER the parenthesized expression
    if (groupLabels.isEmpty()) {
      lexer.skipWhitespace();
      if (lexer.matchKeyword("by")) {
        groupLabels = parseLabelList();
      } else if (lexer.matchKeyword("without")) {
        without = true;
        groupLabels = parseLabelList();
      }
    }

    return new AggregationExpr(op, expr, groupLabels, without, param);
  }

  private List<String> parseLabelList() {
    lexer.skipWhitespace();
    lexer.expect("(");
    final List<String> labels = new ArrayList<>();
    while (!lexer.match(")")) {
      if (!labels.isEmpty())
        lexer.expect(",");
      labels.add(lexer.readIdent());
    }
    return labels;
  }

  private PromQLExpr parseFunctionCall(final String name) {
    lexer.expect("(");
    final List<PromQLExpr> args = new ArrayList<>();
    while (!lexer.match(")")) {
      if (!args.isEmpty())
        lexer.expect(",");
      args.add(parseOr());
    }
    return new FunctionCallExpr(name, args);
  }

  private PromQLExpr parseVectorSelector(final String metricName) {
    final List<LabelMatcher> matchers = new ArrayList<>();
    lexer.skipWhitespace();

    // Optional label matchers: {key="value", ...}
    if (lexer.match("{")) {
      while (!lexer.match("}")) {
        if (!matchers.isEmpty())
          lexer.expect(",");
        final String labelName = lexer.readIdent();
        final MatchOp matchOp = lexer.readMatchOp();
        final String labelValue = lexer.readString();
        matchers.add(new LabelMatcher(labelName, matchOp, labelValue));
      }
    }

    PromQLExpr result = new VectorSelector(metricName, matchers, 0);
    return maybeMatrixOrOffset(result);
  }

  private PromQLExpr maybeMatrixOrOffset(final PromQLExpr expr) {
    lexer.skipWhitespace();

    // Range vector: [5m]
    if (lexer.match("[")) {
      final long rangeMs = lexer.readDuration();
      lexer.expect("]");
      VectorSelector vs;
      if (expr instanceof VectorSelector v)
        vs = v;
      else
        throw new IllegalArgumentException("Range selector requires a vector selector");

      // Check for offset after range
      long offsetMs = 0;
      lexer.skipWhitespace();
      if (lexer.matchKeyword("offset"))
        offsetMs = lexer.readDuration();

      if (offsetMs != 0 || vs.offsetMs() != 0)
        vs = new VectorSelector(vs.metricName(), vs.matchers(), offsetMs != 0 ? offsetMs : vs.offsetMs());

      return new MatrixSelector(vs, rangeMs);
    }

    // Offset modifier
    if (lexer.matchKeyword("offset")) {
      final long offsetMs = lexer.readDuration();
      if (expr instanceof VectorSelector vs)
        return new VectorSelector(vs.metricName(), vs.matchers(), offsetMs);
      throw new IllegalArgumentException("Offset modifier requires a vector selector");
    }

    return expr;
  }

  // --- Duration parsing ---

  public static long parseDuration(final String s) {
    long totalMs = 0;
    long current = 0;
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (c >= '0' && c <= '9') {
        current = current * 10 + (c - '0');
      } else {
        // Check for 'ms' (milliseconds) before consuming 'm' (minutes)
        if (c == 'm' && i + 1 < s.length() && s.charAt(i + 1) == 's') {
          totalMs += current; // already in milliseconds
          current = 0;
          i++; // skip the 's'
          continue;
        }
        final long unitMs = switch (c) {
          case 's' -> 1_000L;
          case 'm' -> 60_000L;
          case 'h' -> 3_600_000L;
          case 'd' -> 86_400_000L;
          case 'w' -> 604_800_000L;
          case 'y' -> 31_536_000_000L;
          default -> throw new IllegalArgumentException("Unknown duration unit: " + c);
        };
        if (current > Long.MAX_VALUE / unitMs)
          throw new IllegalArgumentException("Duration value too large: " + s);
        totalMs += current * unitMs;
        current = 0;
      }
    }
    if (current != 0)
      throw new IllegalArgumentException("Duration must end with a unit (ms/s/m/h/d/w/y): " + s);
    return totalMs;
  }

  // --- Inner Lexer ---

  static class Lexer {
    final String input;
    int pos;

    Lexer(final String input) {
      this.input = input;
      this.pos = 0;
    }

    boolean hasMore() {
      skipWhitespace();
      return pos < input.length();
    }

    char peekChar() {
      skipWhitespace();
      return pos < input.length() ? input.charAt(pos) : 0;
    }

    boolean peekChar(final int offset, final char c) {
      final int idx = pos + offset;
      return idx < input.length() && input.charAt(idx) == c;
    }

    String peek() {
      skipWhitespace();
      if (pos >= input.length())
        return "";
      return input.substring(pos, Math.min(pos + 10, input.length()));
    }

    void skipWhitespace() {
      while (pos < input.length() && Character.isWhitespace(input.charAt(pos)))
        pos++;
    }

    boolean match(final String s) {
      skipWhitespace();
      if (input.startsWith(s, pos)) {
        // For multi-char operators, make sure we don't match a prefix
        if (s.length() == 1 && isOperatorChar(s.charAt(0))) {
          // Check that we're not part of a longer operator
          final int next = pos + 1;
          if (s.equals("=") && next < input.length() && (input.charAt(next) == '=' || input.charAt(next) == '~'))
            return false;
          if (s.equals("!") && next < input.length() && (input.charAt(next) == '=' || input.charAt(next) == '~'))
            return false;
          if (s.equals("<") && next < input.length() && input.charAt(next) == '=')
            return false;
          if (s.equals(">") && next < input.length() && input.charAt(next) == '=')
            return false;
        }
        pos += s.length();
        return true;
      }
      return false;
    }

    /**
     * Match a minus sign as a binary operator (not part of a number).
     */
    boolean matchMinus() {
      skipWhitespace();
      if (pos < input.length() && input.charAt(pos) == '-') {
        pos++;
        return true;
      }
      return false;
    }

    boolean matchKeyword(final String keyword) {
      skipWhitespace();
      if (pos + keyword.length() > input.length())
        return false;
      if (!input.substring(pos, pos + keyword.length()).equalsIgnoreCase(keyword))
        return false;
      // Make sure it's not part of a longer identifier
      final int end = pos + keyword.length();
      if (end < input.length() && isIdentChar(input.charAt(end)))
        return false;
      pos = end;
      return true;
    }

    void expect(final String s) {
      skipWhitespace();
      if (!input.startsWith(s, pos))
        throw new IllegalArgumentException("Expected '" + s + "' at position " + pos + ", found: '"
            + input.substring(pos, Math.min(pos + 10, input.length())) + "'");
      pos += s.length();
    }

    String readIdent() {
      skipWhitespace();
      final int start = pos;
      while (pos < input.length() && isIdentChar(input.charAt(pos)))
        pos++;
      if (pos == start)
        throw new IllegalArgumentException("Expected identifier at position " + pos);
      return input.substring(start, pos);
    }

    double readNumber() {
      skipWhitespace();
      final int start = pos;
      // Handle NaN and Inf
      if (input.startsWith("NaN", pos)) {
        pos += 3;
        return Double.NaN;
      }
      if (input.startsWith("Inf", pos) || input.startsWith("+Inf", pos)) {
        pos += input.charAt(pos) == '+' ? 4 : 3;
        return Double.POSITIVE_INFINITY;
      }
      if (input.startsWith("-Inf", pos)) {
        pos += 4;
        return Double.NEGATIVE_INFINITY;
      }

      while (pos < input.length() && (Character.isDigit(input.charAt(pos)) || input.charAt(pos) == '.'
          || input.charAt(pos) == 'e' || input.charAt(pos) == 'E'
          || ((input.charAt(pos) == '+' || input.charAt(pos) == '-') && pos > start
              && (input.charAt(pos - 1) == 'e' || input.charAt(pos - 1) == 'E'))))
        pos++;

      if (pos == start)
        throw new IllegalArgumentException("Expected number at position " + pos);
      return Double.parseDouble(input.substring(start, pos));
    }

    String readString() {
      skipWhitespace();
      final char quote = input.charAt(pos);
      if (quote != '"' && quote != '\'' && quote != '`')
        throw new IllegalArgumentException("Expected string at position " + pos);
      pos++;
      final StringBuilder sb = new StringBuilder();
      while (pos < input.length() && input.charAt(pos) != quote) {
        if (input.charAt(pos) == '\\' && pos + 1 < input.length()) {
          pos++;
          sb.append(switch (input.charAt(pos)) {
            case 'n' -> '\n';
            case 't' -> '\t';
            case '\\' -> '\\';
            default -> input.charAt(pos);
          });
        } else {
          sb.append(input.charAt(pos));
        }
        pos++;
      }
      if (pos >= input.length())
        throw new IllegalArgumentException("Unterminated string");
      pos++; // closing quote
      return sb.toString();
    }

    MatchOp readMatchOp() {
      skipWhitespace();
      if (match("=~"))
        return MatchOp.RE;
      if (match("!~"))
        return MatchOp.NRE;
      if (match("!="))
        return MatchOp.NEQ;
      if (match("="))
        return MatchOp.EQ;
      throw new IllegalArgumentException("Expected match operator at position " + pos);
    }

    long readDuration() {
      skipWhitespace();
      final int start = pos;
      while (pos < input.length() && (Character.isDigit(input.charAt(pos)) || Character.isLetter(input.charAt(pos))))
        pos++;
      if (pos == start)
        throw new IllegalArgumentException("Expected duration at position " + pos);
      return parseDuration(input.substring(start, pos));
    }

    private static boolean isIdentChar(final char c) {
      return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':';
    }

    private static boolean isOperatorChar(final char c) {
      return c == '=' || c == '!' || c == '<' || c == '>';
    }
  }
}
