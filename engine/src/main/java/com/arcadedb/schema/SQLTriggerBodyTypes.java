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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.parser.Identifier;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The types the body of a SQL trigger names (issue #12941), read off the parse tree of the body: the target of an
 * {@code INSERT INTO}, a {@code CREATE VERTEX}, a {@code CREATE EDGE}, a {@code MOVE VERTEX ... TO} or a {@code TRUNCATE TYPE},
 * every type a {@code FROM} names at any depth ({@code UPDATE}, {@code DELETE FROM}, {@code SELECT}, {@code TRAVERSE}, a
 * subquery in an expression or in a {@code LET}), and a {@code type:} or {@code types:} filter of a {@code MATCH}. Only the
 * parser can tell these apart from a string literal, a comment or a property that holds the same word, which name no type.
 * <p>
 * A body that does not parse names nothing: it fails every time the trigger fires anyway, and refusing a drop or a rename for
 * it would keep the type hostage to a trigger that cannot run. A type reached in any other way (an edge label passed to
 * {@code out('E')} as a string, a name computed at run time) is not a reference either, as no static reading can know it.
 */
final class SQLTriggerBodyTypes {
  private static final Pattern PLAIN_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private final String   body;
  private final String[] typeNames;

  private SQLTriggerBodyTypes(final String body, final String[] typeNames) {
    this.body = body;
    this.typeNames = typeNames;
  }

  static SQLTriggerBodyTypes of(final Database database, final String body) {
    final List<SQLParser.IdentifierContext> references = references(database, body);
    final String[] typeNames = new String[references == null ? 0 : references.size()];
    for (int i = 0; i < typeNames.length; i++)
      typeNames[i] = MaterializedViewSources.identifierName(references.get(i));
    return new SQLTriggerBodyTypes(body, typeNames);
  }

  /** The text these names were read from. */
  String body() {
    return body;
  }

  /** Whether the body names the type {@code canonicalTypeName}, by its name or by one of its aliases. */
  boolean names(final LocalSchema schema, final String canonicalTypeName) {
    for (final String typeName : typeNames)
      if (canonicalTypeName.equals(TypeNameDependant.canonicalNameOf(schema, typeName)))
        return true;
    return false;
  }

  /** Whether the body names the type by exactly {@code typeName}, not by an alias. */
  boolean namesExactly(final String typeName) {
    for (final String name : typeNames)
      if (typeName.equals(name))
        return true;
    return false;
  }

  /**
   * The body naming {@code newName} wherever it names the type {@code oldName}, or {@code null} when that cannot be done with
   * certainty. Each reference is replaced in the user's own text (formatting and comments kept), and the result is accepted only
   * when its parse tree is the one of the body but for those identifiers, which now read {@code newName}: the tokens replaced
   * were the references and nothing else changed. A string literal or a property with the old name is left alone, being no
   * reference. The plain spelling of the new name is tried first, then the back-ticked one, for a name that is a keyword.
   */
  static String retarget(final Database database, final String body, final String oldName, final String newName) {
    final ParseTree before = parse(database, body);
    if (before == null)
      return null;
    final List<SQLParser.IdentifierContext> replaced = new ArrayList<>();
    collect(before, replaced);
    replaced.removeIf(reference -> !oldName.equals(MaterializedViewSources.identifierName(reference)));
    // In the order of the text, which the walk does not keep: the target of a MOVE VERTEX comes after its source subquery
    replaced.sort(Comparator.comparingInt(reference -> reference.getStart().getStartIndex()));
    if (replaced.isEmpty())
      return null;

    final String quoted = Identifier.quote(newName);
    for (final String spelling : PLAIN_IDENTIFIER.matcher(newName).matches() ? new String[] { newName, quoted } : new String[] { quoted }) {
      final StringBuilder retargeted = new StringBuilder(body.length() + replaced.size() * (spelling.length() + 2));
      int copied = 0;
      for (final SQLParser.IdentifierContext reference : replaced) {
        retargeted.append(body, copied, reference.getStart().getStartIndex()).append(spelling);
        copied = reference.getStop().getStopIndex() + 1;
      }
      retargeted.append(body, copied, body.length());

      final String text = retargeted.toString();
      final ParseTree after = parse(database, text);
      if (after != null && sameTreeBut(before, after, replaced, newName))
        return text;
    }
    return null;
  }

  private static List<SQLParser.IdentifierContext> references(final Database database, final String body) {
    final ParseTree tree = parse(database, body);
    if (tree == null)
      return null;
    final List<SQLParser.IdentifierContext> references = new ArrayList<>();
    collect(tree, references);
    return references;
  }

  private static ParseTree parse(final Database database, final String body) {
    try {
      return new SQLAntlrParser(database).parseWithTree(body).tree();
    } catch (final CommandSQLParsingException e) {
      return null;
    }
  }

  /** Collects, in the order of the text, the identifiers that name a type. */
  private static void collect(final ParseTree node, final List<SQLParser.IdentifierContext> references) {
    switch (node) {
    case SQLParser.FromIdentifierContext from -> add(from.identifier(0), references);
    case SQLParser.InsertStatementContext insert -> {
      if (!insert.identifier().isEmpty())
        add(insert.identifier(0), references);
    }
    case SQLParser.CreateVertexBodyContext vertex -> addLeading(vertex, references);
    case SQLParser.CreateEdgeBodyContext edge -> addLeading(edge, references);
    case SQLParser.TruncateTypeBodyContext truncate -> add(truncate.identifier(), references);
    case SQLParser.MoveVertexStatementContext move -> addMoveTarget(move, references);
    case SQLParser.MatchFilterItemContext item -> {
      final SQLParser.MatchFilterItemKeyContext key = item.matchFilterItemKey();
      if (key != null && (key.TYPE() != null || key.TYPES() != null))
        collectBareIdentifiers(item.expression(), references);
    }
    default -> {
    }
    }

    for (int i = 0; i < node.getChildCount(); i++)
      collect(node.getChild(i), references);
  }

  private static void add(final SQLParser.IdentifierContext identifier, final List<SQLParser.IdentifierContext> references) {
    // A $variable names a LET result or a bound record, never a type
    if (identifier != null && !identifier.getText().startsWith("$"))
      references.add(identifier);
  }

  /** The type of {@code CREATE VERTEX} and {@code CREATE EDGE} is the optional identifier the body opens with. */
  private static void addLeading(final ParserRuleContext body, final List<SQLParser.IdentifierContext> references) {
    if (body.getChildCount() > 0 && body.getChild(0) instanceof SQLParser.IdentifierContext identifier)
      add(identifier, references);
  }

  /** {@code MOVE VERTEX ... TO TYPE:<name>} or {@code TO <name>}; {@code TO BUCKET:<name>} names a bucket. */
  private static void addMoveTarget(final SQLParser.MoveVertexStatementContext move, final List<SQLParser.IdentifierContext> references) {
    for (int i = 1; i < move.getChildCount(); i++)
      if (move.getChild(i) instanceof SQLParser.IdentifierContext identifier && (isToken(move.getChild(i - 1), SQLParser.TO)
          || (i > 1 && isToken(move.getChild(i - 1), SQLParser.COLON) && isToken(move.getChild(i - 2), SQLParser.TYPE))))
        add(identifier, references);
  }

  /** The identifiers that are a whole expression of a MATCH type filter: {@code type: T}, or an element of {@code types: [T, U]}. */
  private static void collectBareIdentifiers(final ParseTree node, final List<SQLParser.IdentifierContext> references) {
    if (node instanceof SQLParser.IdentifierContext identifier) {
      final SQLParser.ExpressionContext expression = enclosingExpression(identifier);
      if (expression != null && expression.getStart() == identifier.getStart() && expression.getStop() == identifier.getStop())
        add(identifier, references);
      return;
    }
    for (int i = 0; i < node.getChildCount(); i++)
      collectBareIdentifiers(node.getChild(i), references);
  }

  private static SQLParser.ExpressionContext enclosingExpression(final ParserRuleContext node) {
    for (ParserRuleContext parent = node.getParent(); parent != null; parent = parent.getParent())
      if (parent instanceof SQLParser.ExpressionContext expression)
        return expression;
    return null;
  }

  private static boolean isToken(final ParseTree node, final int tokenType) {
    return node instanceof TerminalNode terminal && terminal.getSymbol().getType() == tokenType;
  }

  /** Whether {@code after} is {@code before} with each of the {@code replaced} identifiers naming {@code newName}. */
  private static boolean sameTreeBut(final ParseTree before, final ParseTree after, final List<SQLParser.IdentifierContext> replaced,
      final String newName) {
    if (before.getClass() != after.getClass())
      return false;
    if (before instanceof TerminalNode beforeTerminal) {
      final Token beforeToken = beforeTerminal.getSymbol();
      final Token afterToken = ((TerminalNode) after).getSymbol();
      return beforeToken.getType() == afterToken.getType() && beforeToken.getText().equals(afterToken.getText());
    }
    // ParserRuleContext does not override equals(): contains() is by identity
    if (before instanceof SQLParser.IdentifierContext && replaced.contains(before))
      return newName.equals(MaterializedViewSources.identifierName((SQLParser.IdentifierContext) after));
    if (before.getChildCount() != after.getChildCount())
      return false;
    for (int i = 0; i < before.getChildCount(); i++)
      if (!sameTreeBut(before.getChild(i), after.getChild(i), replaced, newName))
        return false;
    return true;
  }
}
