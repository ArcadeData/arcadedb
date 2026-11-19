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
import com.arcadedb.query.opencypher.ast.CypherTypeName;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.IsNormalizedExpression;
import com.arcadedb.query.opencypher.ast.IsTypedExpression;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import java.text.Normalizer;
import java.util.List;
import java.util.Locale;

/**
 * Builds the predicates on the type of a value - IS TYPED, IS NORMALIZED - and reads the type names they and the
 * property type constraints are written with: the scalar types, the lists of them, a vector and NOT NULL.
 * <p>
 * Split out of {@link CypherASTBuilder}, the visitor that builds a statement from its parse tree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TypePredicateBuilder {
  private TypePredicateBuilder() {
  }

  /**
   * Builds an {@link IsTypedExpression} from a parsed {@code IS [NOT] TYPED type} or
   * {@code :: type} comparison (issue #3365 section 3.3). Static so both
   * {@link CypherASTBuilder} (boolean path) and {@link CypherExpressionBuilder}
   * (expression path) can share a single implementation.
   */
  static IsTypedExpression buildIsTypedExpression(final Expression leftExpr,
      final Cypher25Parser.TypeComparisonContext typeCtx) {
    return new IsTypedExpression(leftExpr, parseTypeName(typeCtx.type(), "IS TYPED"), typeCtx.NOT() != null);
  }

  /**
   * Builds an {@link IsNormalizedExpression} from a parsed {@code IS [NOT] [form] NORMALIZED} comparison (issue #9940). Shared by
   * the boolean and the expression path, like {@link #buildIsTypedExpression}.
   */
  static IsNormalizedExpression buildIsNormalizedExpression(final Expression leftExpr,
      final Cypher25Parser.NormalFormComparisonContext ctx) {
    final Cypher25Parser.NormalFormContext formCtx = ctx.normalForm();
    final Normalizer.Form form;
    if (formCtx == null || formCtx.NFC() != null)
      form = Normalizer.Form.NFC;
    else if (formCtx.NFD() != null)
      form = Normalizer.Form.NFD;
    else if (formCtx.NFKC() != null)
      form = Normalizer.Form.NFKC;
    else
      form = Normalizer.Form.NFKD;
    return new IsNormalizedExpression(leftExpr, form, ctx.NOT() != null);
  }

  /**
   * {@code x IS DIRECTED} is the GQL directed predicate, but the Cypher 25 grammar has no rule for it and reads it as a test
   * for a label named DIRECTED, which answered false for every relationship (issue #9940). It is refused; a label that
   * really is called DIRECTED stays reachable as {@code x:DIRECTED} or {@code x IS `DIRECTED`}.
   */
  static void rejectDirectedPredicate(final Cypher25Parser.LabelExpressionContext ctx) {
    if (ctx != null && ctx.IS() != null && "DIRECTED".equalsIgnoreCase(ctx.labelExpression4().getText()))
      throw new FeatureNotImplementedException("the directed predicate 'IS DIRECTED' is not supported (every "
          + "relationship is directed); to test a label named DIRECTED write ':DIRECTED' or 'IS `DIRECTED`'");
  }

  /**
   * Parses a Cypher type into its canonical {@link CypherTypeName}, shared by the {@code IS ::} predicate and the property
   * type constraint so the two cannot read one spelling two ways (#9709). {@code LIST<T>}, {@code ARRAY<T>} and the postfix
   * {@code T LIST} are all the list of {@code T}; {@code ANY<T>} with a single {@code T} is {@code T}. {@code VECTOR<T>(n)} and
   * {@code VECTOR(n, T)} are the vector of coordinate type {@code T} and dimension {@code n} (#10862). A union of types, a list
   * of lists and a list of vectors are refused: neither the predicate nor the schema can express them.
   *
   * @param where the construct being parsed, for the refusal message
   */
  static CypherTypeName parseTypeName(final Cypher25Parser.TypeContext typeCtx, final String where) {
    if (typeCtx.typePart().size() != 1)
      throw new CommandParsingException("UnsupportedFeature: union types are not supported in " + where);
    final Cypher25Parser.TypePartContext typePart = typeCtx.typePart(0);
    final Cypher25Parser.TypeNameContext nameCtx = typePart.typeName();
    final boolean nonNull = isNonNull(typePart.typeNullability());

    if (nameCtx.VECTOR() != null)
      return wrapInListSuffixes(parseVectorTypeName(nameCtx, nonNull, where), typePart, where);

    if (nameCtx.type() != null) {
      // LIST<T> / ARRAY<T>, or ANY<T> (ANY VALUE<T>), which is T itself when T is a single type
      final CypherTypeName inner = parseTypeName(nameCtx.type(), where);
      if (nameCtx.ANY() != null)
        return wrapInListSuffixes(inner.withNonNull(inner.nonNull() || nonNull), typePart, where);
      if (inner.isList())
        throw new CommandParsingException("UnsupportedFeature: a list of lists is not supported in " + where);
      if (inner.isVector())
        throw new CommandParsingException("UnsupportedFeature: a list of vectors is not supported in " + where);
      return wrapInListSuffixes(new CypherTypeName(CypherTypeName.LIST, inner.name(), nonNull, inner.nonNull()), typePart, where);
    }

    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < nameCtx.getChildCount(); i++) {
      if (i > 0)
        sb.append(' ');
      sb.append(nameCtx.getChild(i).getText().toUpperCase(Locale.ROOT));
    }
    return wrapInListSuffixes(new CypherTypeName(CypherTypeName.canonical(sb.toString()), null, nonNull, false), typePart, where);
  }

  /** {@code T LIST} / {@code T ARRAY}: the postfix spelling of {@code LIST<T>}, with its own nullability. */
  private static CypherTypeName wrapInListSuffixes(final CypherTypeName type, final Cypher25Parser.TypePartContext typePart,
      final String where) {
    final List<Cypher25Parser.TypeListSuffixContext> suffixes = typePart.typeListSuffix();
    if (suffixes.isEmpty())
      return type;
    if (suffixes.size() > 1 || type.isList())
      throw new CommandParsingException("UnsupportedFeature: a list of lists is not supported in " + where);
    if (type.isVector())
      throw new CommandParsingException("UnsupportedFeature: a list of vectors is not supported in " + where);
    return new CypherTypeName(CypherTypeName.LIST, type.name(), isNonNull(suffixes.getFirst().typeNullability()), type.nonNull());
  }

  /**
   * {@code VECTOR}, {@code VECTOR<T>}, {@code VECTOR(n)}, {@code VECTOR<T>(n)} or {@code VECTOR(n, T)}: the coordinate type is
   * reduced to its canonical name like any other numeric type ({@code INT64} is {@code INTEGER}, {@code FLOAT64} is
   * {@code FLOAT}), and the dimension must be a positive int.
   */
  private static CypherTypeName parseVectorTypeName(final Cypher25Parser.TypeNameContext nameCtx, final boolean nonNull,
      final String where) {
    final Cypher25Parser.VectorCoordinateTypeContext coordinateCtx = nameCtx.vectorCoordinateType();
    String coordinate = null;
    boolean coordinateNonNull = false;
    if (coordinateCtx != null) {
      final StringBuilder sb = new StringBuilder();
      for (int i = 0; i < coordinateCtx.getChildCount(); i++) {
        if (coordinateCtx.getChild(i) instanceof Cypher25Parser.TypeNullabilityContext)
          continue;
        if (!sb.isEmpty())
          sb.append(' ');
        sb.append(coordinateCtx.getChild(i).getText().toUpperCase(Locale.ROOT));
      }
      coordinate = CypherTypeName.canonical(sb.toString());
      coordinateNonNull = isNonNull(coordinateCtx.typeNullability());
    }

    int dimension = CypherTypeName.NO_DIMENSION;
    if (nameCtx.signedIntegerLiteral() != null) {
      final String text = nameCtx.signedIntegerLiteral().getText();
      long value;
      try {
        value = Long.parseLong(text.replace("_", ""));
      } catch (final NumberFormatException e) {
        value = -1;
      }
      if (value < 1 || value > Integer.MAX_VALUE)
        throw new CommandParsingException("The dimension of a VECTOR type in " + where + " must be a positive integer, found: " + text);
      dimension = (int) value;
    }
    return new CypherTypeName(CypherTypeName.VECTOR, coordinate, nonNull, coordinateNonNull, dimension);
  }

  private static boolean isNonNull(final Cypher25Parser.TypeNullabilityContext nullability) {
    return nullability != null && (nullability.NOT() != null || nullability.EXCLAMATION_MARK() != null);
  }
}
