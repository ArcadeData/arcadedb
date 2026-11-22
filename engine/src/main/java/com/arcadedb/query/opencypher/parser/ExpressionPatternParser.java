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

import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the patterns written inside an expression - a pattern predicate, a pattern comprehension, a shortestPath -
 * from their nodes, relationships, labels and property maps.
 * <p>
 * Split out of {@link CypherExpressionBuilder}, which decides what an expression is and builds it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ExpressionPatternParser {
  private final CypherExpressionBuilder expressions;

  ExpressionPatternParser(final CypherExpressionBuilder expressions) {
    this.expressions = expressions;
  }

  /**
   * Parse a pathPatternNonEmpty into a PathPattern.
   * Grammar: nodePattern (relationshipPattern nodePattern)+
   */
  PathPattern parsePathPatternNonEmpty(final Cypher25Parser.PathPatternNonEmptyContext ctx) {
    final List<NodePattern> nodes = new ArrayList<>();
    final List<RelationshipPattern> relationships = new ArrayList<>();

    // First node
    nodes.add(parseNodePattern(ctx.nodePattern(0)));

    // Relationships and subsequent nodes
    for (int i = 0; i < ctx.relationshipPattern().size(); i++) {
      relationships.add(parseRelationshipPattern(ctx.relationshipPattern(i)));
      nodes.add(parseNodePattern(ctx.nodePattern(i + 1)));
    }

    return new PathPattern(nodes, relationships);
  }

  /**
   * Parse a shortestPath or allShortestPaths expression.
   * Syntax: shortestPath((a)-[:KNOWS*]-(b)) or allShortestPaths((a)-[:KNOWS*]-(b))
   */
  ShortestPathExpression parseShortestPathExpression(final Cypher25Parser.ShortestPathExpressionContext ctx) {
    // Grammar: shortestPathExpression : shortestPathPattern ;
    // shortestPathPattern : (SHORTEST_PATH | ALL_SHORTEST_PATHS) LPAREN patternElement RPAREN ;
    final Cypher25Parser.ShortestPathPatternContext patternCtx = ctx.shortestPathPattern();
    final boolean isAllPaths = patternCtx.ALL_SHORTEST_PATHS() != null;

    // Parse the inner pattern element
    final PathPattern innerPattern = parsePatternElement(patternCtx.patternElement());

    return new ShortestPathExpression(innerPattern, isAllPaths, ctx.getText());
  }

  /**
   * Parse a patternElement into a PathPattern.
   * This is used for shortestPath patterns which contain a patternElement.
   */
  private PathPattern parsePatternElement(final Cypher25Parser.PatternElementContext ctx) {
    final List<NodePattern> nodes = new ArrayList<>();
    final List<RelationshipPattern> relationships = new ArrayList<>();

    // First node
    if (!ctx.nodePattern().isEmpty()) {
      nodes.add(parseNodePattern(ctx.nodePattern(0)));

      // Relationships and subsequent nodes
      for (int i = 0; i < ctx.relationshipPattern().size(); i++) {
        relationships.add(parseRelationshipPattern(ctx.relationshipPattern(i)));
        if (i + 1 < ctx.nodePattern().size()) {
          nodes.add(parseNodePattern(ctx.nodePattern(i + 1)));
        }
      }
    }

    if (relationships.isEmpty()) {
      return new PathPattern(nodes.get(0));
    } else {
      return new PathPattern(nodes, relationships);
    }
  }

  /**
   * Parse a nodePattern into a NodePattern.
   */
  private NodePattern parseNodePattern(final Cypher25Parser.NodePatternContext ctx) {
    String variable = null;
    List<String> labels = null;
    Map<String, Object> properties = null;
    // A node pattern in expression position - a pattern comprehension, a pattern predicate - used to be built with
    // the disjunction flag hardcoded to false, so (y:A|B) arrived at the executor looking exactly like (y:A:B) and
    // matched only what carries BOTH labels. Read off the grammar here, the same way the MATCH-side builder does,
    // rather than left for the executor to guess (issue #6338).
    boolean labelDisjunction = false;

    if (ctx.variable() != null) {
      variable = ParserUtils.stripBackticks(ctx.variable().getText());
    }

    // A negation, the wildcard or a mix of '&' and '|' is checked as a predicate on the node, the way the MATCH-side
    // builder does it (issue #8992).
    BooleanExpression labelCheck = null;
    final LabelPredicate labelPredicate = ParserUtils.buildLabelPredicate(ctx.labelExpression(), expressions::parseExpression);
    if (labelPredicate != null) {
      labels = labelPredicate.requiredLabels();
      if (variable == null)
        variable = ParserUtils.newLabelExpressionVariable();
      labelCheck = ParserUtils.labelCheckOn(variable, labelPredicate, ctx.labelExpression());
    } else if (ctx.labelExpression() != null) {
      labels = extractLabels(ctx.labelExpression());
      labelDisjunction = ParserUtils.isLabelDisjunction(ctx.labelExpression());
      // (b:$(expr)) in a pattern comprehension or predicate: checked as a predicate on the node (issue #11776)
      final List<Expression> dynamicLabels = ParserUtils.parseDynamicLabels(ctx.labelExpression(), expressions::parseExpression);
      if (dynamicLabels != null) {
        if (variable == null)
          variable = ParserUtils.newLabelExpressionVariable();
        labelCheck = ParserUtils.dynamicLabelCheckOn(variable, labels, dynamicLabels, ctx.labelExpression());
        // (n:A|$(expr)): the static labels alone are not a filter, the check above carries all of them
        if (labelDisjunction)
          labels = null;
      }
    }

    String propertiesParameterName = null;
    if (ctx.properties() != null) {
      if (ctx.properties().parameter() != null)
        propertiesParameterName = CypherASTBuilder.stripBackticks(ctx.properties().parameter().parameterName().getText());
      else if (ctx.properties().map() != null)
        properties = parseMapProperties(ctx.properties().map());
    }

    // Inline WHERE predicate, e.g. the WHERE x.v = 2 in [(a)-[:E]->(x:A WHERE x.v = 2) | x]. Parsed
    // as a generic expression and coerced to a predicate, mirroring the relationship form below, so
    // comparisons, AND/OR/NOT and boolean-typed properties are all covered.
    BooleanExpression whereExpression = null;
    if (ctx.expression() != null) {
      BooleanExpressionBuilder.rejectNonBooleanPatternExpressions(ctx.expression());
      final Expression inlineCondition = expressions.parseExpression(ctx.expression());
      // A list literal cannot be coerced to a boolean (issue #12975)
      BooleanCoercionExpression.refuseListLiteral(inlineCondition);
      whereExpression = new BooleanCoercionExpression(inlineCondition);
    }
    whereExpression = ParserUtils.andLabelCheck(labelCheck, whereExpression);

    return new NodePattern(variable, labels, null, properties, propertiesParameterName, labelDisjunction, whereExpression);
  }

  /**
   * Parse a relationshipPattern into a RelationshipPattern.
   */
  private RelationshipPattern parseRelationshipPattern(final Cypher25Parser.RelationshipPatternContext ctx) {
    String variable = null;
    List<String> types = null;
    Map<String, Object> properties = null;
    Integer minHops = null;
    Integer maxHops = null;

    if (ctx.variable() != null) {
      variable = ParserUtils.stripBackticks(ctx.variable().getText());
    }

    BooleanExpression labelCheck = null;
    final LabelPredicate labelPredicate = ParserUtils.buildRelationshipTypePredicate(ctx.labelExpression(), expressions::parseExpression);
    if (labelPredicate != null) {
      // Evaluated per hop on a variable-length relationship too, through the inline WHERE it is ANDed into (#9117).
      final List<String> required = labelPredicate.requiredLabels();
      types = required.isEmpty() ? null : required;
      if (variable == null)
        variable = ParserUtils.newLabelExpressionVariable();
      labelCheck = ParserUtils.labelCheckOn(variable, labelPredicate, ctx.labelExpression());
    } else if (ctx.labelExpression() != null) {
      types = extractLabels(ctx.labelExpression());
      // [r:$(expr)]: the whole type list is checked per relationship, see the MATCH-side builder (issue #11776)
      final List<Expression> dynamicTypes = ParserUtils.parseDynamicLabels(ctx.labelExpression(), expressions::parseExpression);
      if (dynamicTypes != null) {
        if (variable == null)
          variable = ParserUtils.newLabelExpressionVariable();
        labelCheck = ParserUtils.dynamicLabelCheckOn(variable, types, dynamicTypes, ctx.labelExpression());
        types = null;
      }
    }

    String propertiesParameterName = null;
    if (ctx.properties() != null) {
      if (ctx.properties().parameter() != null)
        propertiesParameterName = CypherASTBuilder.stripBackticks(ctx.properties().parameter().parameterName().getText());
      else if (ctx.properties().map() != null)
        properties = parseMapProperties(ctx.properties().map());
    }

    // Path length (variable-length relationships), read through the one place that knows what [*] means.
    // Written as its own copy of the MATCH-side block, this is where [*] lost its normalization to [*1..] and a
    // pattern comprehension planned it as a single fixed hop (issue #6370).
    if (ctx.pathLength() != null) {
      final ParserUtils.PathLength pathLength = ParserUtils.parsePathLength(ctx.pathLength());
      minHops = pathLength.minHops();
      maxHops = pathLength.maxHops();
    }

    // Direction
    final Direction direction;
    if (ctx.leftArrow() != null && ctx.rightArrow() != null) {
      direction = Direction.BOTH;
    } else if (ctx.leftArrow() != null) {
      direction = Direction.IN;
    } else if (ctx.rightArrow() != null) {
      direction = Direction.OUT;
    } else {
      direction = Direction.BOTH;
    }

    // Inline WHERE predicate, e.g. the WHERE r.tag = 'ok' in -[r:E WHERE r.tag = 'ok']->. Parsed as
    // a generic expression and coerced to a predicate so comparisons, AND/OR/NOT and boolean-typed
    // properties are all covered without duplicating the boolean-parsing hierarchy.
    BooleanExpression whereExpression = null;
    if (ctx.expression() != null) {
      BooleanExpressionBuilder.rejectNonBooleanPatternExpressions(ctx.expression());
      final Expression inlineCondition = expressions.parseExpression(ctx.expression());
      // A list literal cannot be coerced to a boolean (issue #12975)
      BooleanCoercionExpression.refuseListLiteral(inlineCondition);
      whereExpression = new BooleanCoercionExpression(inlineCondition);
    }
    whereExpression = ParserUtils.andLabelCheck(labelCheck, whereExpression);

    return new RelationshipPattern(variable, types, direction, properties, propertiesParameterName, minHops, maxHops,
        whereExpression);
  }

  /**
   * Extract labels from a labelExpression context, through the same grammar-based walk the MATCH-side builder uses.
   * <p>
   * This used to split the raw text on {@code :}, {@code &} and {@code |}, which is a different answer from the one
   * {@code MATCH} gives for the same pattern: a backtick-quoted label containing a separator was chopped into
   * pieces, the backticks were kept on every name, and a dynamic {@code $(expression)} label was turned into a
   * literal label named after its own source text. The two blocks were meant to be copies of each other, and every
   * way they were not is a way an expression-position pattern answered differently from the identical MATCH
   * pattern (issues #6370, #6363).
   */
  private List<String> extractLabels(final Cypher25Parser.LabelExpressionContext ctx) {
    return ParserUtils.extractLabels(ctx);
  }

  /**
   * Parse map properties from a MapContext.
   */
  private Map<String, Object> parseMapProperties(final Cypher25Parser.MapContext ctx) {
    final Map<String, Object> map = new LinkedHashMap<>();
    final List<Cypher25Parser.PropertyKeyNameContext> keys = ctx.propertyKeyName();
    final List<Cypher25Parser.ExpressionContext> values = ctx.expression();

    for (int i = 0; i < keys.size() && i < values.size(); i++) {
      final String key = CypherASTBuilder.stripBackticks(keys.get(i).getText());
      final Expression expr = expressions.parseExpression(values.get(i));
      if (expr instanceof LiteralExpression) {
        map.put(key, ((LiteralExpression) expr).getValue());
      } else {
        map.put(key, expr);
      }
    }

    return map;
  }
}
