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
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import java.util.Locale;
import java.util.logging.Level;

/**
 * Builds the expressions that carry a subquery: EXISTS { ... }, COLLECT { ... } and COUNT { ... }, whose body is a
 * pattern with an optional WHERE or a whole statement.
 * <p>
 * Split out of {@link CypherExpressionBuilder}, which decides what an expression is and builds it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SubqueryExpressionParser {
  private SubqueryExpressionParser() {
  }

  /**
   * Parse an EXISTS expression.
   * Examples: EXISTS { MATCH (n)-[:KNOWS]->(m) }, EXISTS { (n)-[:KNOWS]->() WHERE n.age > 18 }
   */
  static ExistsExpression parseExistsExpression(final Cypher25Parser.ExistsExpressionContext ctx) {
    // Use original text (with whitespace preserved) to avoid mangled subqueries
    final String originalText = CypherASTBuilder.getOriginalText(ctx);
    final String text = originalText;

    // Find the opening { and closing } to extract the subquery body
    final int openBrace = originalText.indexOf('{');
    final int closeBrace = originalText.lastIndexOf('}');
    String subquery;
    if (openBrace >= 0 && closeBrace > openBrace)
      subquery = originalText.substring(openBrace + 1, closeBrace).trim();
    else
      subquery = originalText.substring(7, originalText.length() - 1).trim(); // fallback

    CypherASTBuilder.checkMatchMode(ctx.matchMode());
    final CypherStatement parsedBody = parseSubqueryBody(ctx.queryWithLocalDefinitions(), ctx.patternList(),
        ctx.whereClause());

    // Check for update clauses inside EXISTS (not allowed)
    if (containsUpdateClause(parsedBody, subquery))
      throw new CommandParsingException(
          "InvalidClauseComposition: Existential subquery cannot contain update clauses");

    // If it's a pattern without MATCH, add MATCH prefix
    if (!CorrelatedSubqueryRewriter.startsWithClauseKeyword(subquery)) {
      subquery = "MATCH " + subquery + " RETURN true";
    } else if (!bodyReturns(parsedBody, subquery)) {
      // Full subquery without RETURN — add RETURN true
      subquery = subquery + " RETURN true";
    }

    return new ExistsExpression(subquery, text, parsedBody);
  }

  /**
   * Whether the body of an EXISTS or COUNT ends with its own RETURN. Read from the parsed body, not from the text: the RETURN of
   * a nested {@code CALL { }} is not the body's, and the text the body is run as must conclude with a RETURN (issue #12354).
   */
  private static boolean bodyReturns(final CypherStatement parsedBody, final String subquery) {
    // A UNION body is run as one text, so a RETURN appended to it would land on the last branch only: the text check is kept
    // for it, as for a body that did not parse into a statement. An EXISTS or COUNT UNION body without a RETURN in every branch
    // is then refused by the conclusion check of the query it runs as, instead of answering a wrong count
    if (parsedBody == null || parsedBody instanceof UnionStatement)
      return subquery.toUpperCase(Locale.ROOT).contains("RETURN ");
    return parsedBody.getReturnClause() != null;
  }

  /**
   * Tells whether a subquery body writes, which the three read-only subquery expressions must reject.
   * <p>
   * Answered from the parse tree when there is one: a clause is a clause because of where it sits, which is what the
   * statement already recorded in {@link CypherStatement#isReadOnly()}. The text scan it replaces had to guess from
   * spelling alone and got two cases wrong by construction - an update keyword inside a comment was read as a clause,
   * and {@code WITH 1 AS set RETURN set} is lexically indistinguishable from {@code SET n.x = 1} (issue #5541 fixed
   * the third, {@code WHERE n.name = 'SET x'}, by teaching the scan about string literals). It stays as the fallback
   * for a body the statement builder declined, where there is nothing else to answer from.
   */
  private static boolean containsUpdateClause(final CypherStatement parsedBody, final String bodyText) {
    if (parsedBody != null)
      return !parsedBody.isReadOnly();
    return CorrelatedSubqueryRewriter.containsUpdateClause(bodyText);
  }

  /**
   * Builds the AST of a subquery body from the parse tree ANTLR already produced for it, so that the parse-time
   * checks of {@link CypherSemanticValidator} can walk inside it (issue #5626).
   * <p>
   * The three subquery expressions keep their body as text and re-parse it once per outer row - what runs is the
   * text after {@link CorrelatedSubqueryRewriter} has correlated it to that row, so the text has to stay. This
   * builds the body <i>as written</i> alongside it, off the existing sub-tree rather than by re-lexing the text.
   * <p>
   * What that costs is a full statement build over an existing sub-tree, not a bare tree walk: the visit goes through
   * the same {@link CypherASTBuilder} pipeline any statement does, rewriters included. A reader sizing it should read
   * it as "one more statement built", not "one traversal".
   * <p>
   * Two things bound it. It is paid once per distinct query text, because {@code CypherStatementCache} keys the whole
   * parse - this AST included, since it hangs off the statement - on the query string itself. And nesting adds rather
   * than multiplies: a body reached from here is built by its own visit, and the enclosing statement holds the
   * subquery only as an expression, so a subquery inside a subquery is built once as part of its parent's body, not
   * again for every level above it. The total is linear in the text.
   * <p>
   * A body written as a bare pattern - {@code EXISTS { (n)-[:KNOWS]->(m) }} - has no {@code queryWithLocalDefinitions}
   * of its own and is wrapped into the {@code MATCH} the executor synthesizes for it anyway.
   * <p>
   * <b>A body this cannot build is not a query this rejects.</b> Building the AST here puts the statement builder on
   * the parse path of a body that used to be carried as text and only re-parsed at execution, so a construct ANTLR
   * accepts but the builder cannot assemble would turn a query that used to run into a parse failure - a regression,
   * and one this issue has no business causing. The build is therefore best-effort: on failure the body stays text
   * only, the walk treats the expression as a leaf exactly as it did before #5626, and execution re-parses the text
   * as it always has. Logged at {@code FINE} so the gap is still discoverable rather than permanent, since a body
   * with no AST is a body the parse-time checks do not see.
   *
   * @param queryCtx   the body when it is a full query, or null when it is a bare pattern
   * @param patternCtx the pattern list of a bare-pattern body, or null
   * @param whereCtx   the trailing WHERE of a bare-pattern body, or null
   */
  private static CypherStatement parseSubqueryBody(final Cypher25Parser.QueryWithLocalDefinitionsContext queryCtx,
      final Cypher25Parser.PatternListContext patternCtx, final Cypher25Parser.WhereClauseContext whereCtx) {
    if (queryCtx == null && patternCtx == null)
      return null;

    try {
      // A fresh builder: CypherASTBuilder holds no state across a visit, and the expression builder it owns is the one
      // running right now, so it cannot be reused re-entrantly.
      final CypherASTBuilder astBuilder = new CypherASTBuilder();

      if (queryCtx != null)
        return astBuilder.visitQueryWithLocalDefinitions(queryCtx);

      final StatementBuilder builder = new StatementBuilder();
      builder.addMatch(CypherASTBuilder.newMatchClause(astBuilder.visitPatternList(patternCtx), false,
          whereCtx != null ? astBuilder.visitWhereClause(whereCtx) : null));
      return builder.build();
    } catch (final FeatureNotImplementedException | NonBooleanPatternExpressionException e) {
      // A construct the engine refuses by name is refused in a body too. Falling back to the text deferred it to the first
      // row, where the body is rewritten before it is parsed again, and the refusal came out as an unrelated syntax error
      // (issue #9940). A pattern expression used outside a boolean position (issue #11888) would come out at the first row
      // too, or never when the boolean simplifier drops the subquery (true OR EXISTS { ... }).
      throw e;
    } catch (final CommandParsingException e) {
      // The builder refusing a body it does not support is the case this fallback is for, and it is not news.
      LogManager.instance().log(CypherExpressionBuilder.class, Level.FINE,
          "Cannot build the AST of subquery body '%s': it keeps its text and escapes the parse-time checks", e,
          CypherASTBuilder.getOriginalText(queryCtx != null ? queryCtx : patternCtx));
      return null;
    } catch (final RuntimeException e) {
      // Anything else is a defect in the builder rather than a body it declines, and the cost of it is invisible -
      // the query still runs, the checks just stop seeing inside that body. WARNING so a degraded body shows up in
      // an ordinary log instead of only under FINE, which is the difference between discoverable and theoretical.
      LogManager.instance().log(CypherExpressionBuilder.class, Level.WARNING,
          "Unexpected failure building the AST of subquery body '%s': it keeps its text and escapes the parse-time "
              + "checks", e, CypherASTBuilder.getOriginalText(queryCtx != null ? queryCtx : patternCtx));
      return null;
    }
  }

  /**
   * Parse a COLLECT { ... } subquery expression.
   * Example: COLLECT { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name }
   */
  static CollectExpression parseCollectExpression(final Cypher25Parser.CollectExpressionContext ctx) {
    final String originalText = CypherASTBuilder.getOriginalText(ctx);
    final String text = originalText;

    final int openBrace = originalText.indexOf('{');
    final int closeBrace = originalText.lastIndexOf('}');
    String subquery;
    if (openBrace >= 0 && closeBrace > openBrace)
      subquery = originalText.substring(openBrace + 1, closeBrace).trim();
    else
      subquery = originalText.substring(8, originalText.length() - 1).trim(); // fallback "COLLECT{" prefix

    final CypherStatement parsedBody = parseSubqueryBody(ctx.queryWithLocalDefinitions(), null, null);

    // Update clauses are not allowed inside a COLLECT subquery
    if (containsUpdateClause(parsedBody, subquery))
      throw new CommandParsingException(
          "InvalidClauseComposition: COLLECT subquery cannot contain update clauses");

    // No pattern/where context to pass, and none to miss: the grammar gives COLLECT one alternative where EXISTS and
    // COUNT have two - "collectExpression: COLLECT LCURLY queryWithLocalDefinitions RCURLY" against
    // "... (queryWithLocalDefinitions | matchMode? patternList whereClause?) ..." - because a COLLECT body has to
    // RETURN the value being collected. So queryWithLocalDefinitions() is never null here, and a COLLECT body can
    // never end up without an AST and quietly skip the checks this issue exists to reach it with.
    return new CollectExpression(subquery, text, parsedBody);
  }

  /**
   * Parse a COUNT { ... } pattern / subquery expression.
   * Examples:
   * - COUNT { (a)-[:KNOWS]->(b) }
   * - COUNT { MATCH (a)-[:KNOWS]->(b) WHERE b.age > 18 }
   */
  static CountExpression parseCountExpression(final Cypher25Parser.CountExpressionContext ctx) {
    final String originalText = CypherASTBuilder.getOriginalText(ctx);
    final String text = originalText;

    final int openBrace = originalText.indexOf('{');
    final int closeBrace = originalText.lastIndexOf('}');
    String subquery;
    if (openBrace >= 0 && closeBrace > openBrace)
      subquery = originalText.substring(openBrace + 1, closeBrace).trim();
    else
      subquery = originalText.substring(6, originalText.length() - 1).trim(); // fallback "COUNT{" prefix

    CypherASTBuilder.checkMatchMode(ctx.matchMode());
    final CypherStatement parsedBody = parseSubqueryBody(ctx.queryWithLocalDefinitions(), ctx.patternList(),
        ctx.whereClause());

    if (containsUpdateClause(parsedBody, subquery))
      throw new CommandParsingException(
          "InvalidClauseComposition: COUNT subquery cannot contain update clauses");

    // Pattern-only form: wrap with MATCH and add RETURN 1 so the outer evaluator can count rows.
    if (!CorrelatedSubqueryRewriter.startsWithClauseKeyword(subquery)) {
      subquery = "MATCH " + subquery + " RETURN 1";
    } else if (!bodyReturns(parsedBody, subquery)) {
      subquery = subquery + " RETURN 1";
    }

    return new CountExpression(subquery, text, parsedBody);
  }
}
