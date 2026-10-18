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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.parser.Identifier;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The types the defining query of a materialized view reads (issue #10205): every type a {@code FROM} names, at any depth -
 * the statement's own target, a subquery in {@code FROM}, and a subquery anywhere in an expression ({@code WHERE},
 * projection, {@code LET}, ...). The walk is over the parse tree, the one representation of the query that has a uniform
 * shape for every clause, so no clause can be forgotten the way the first {@code FROM} identifier alone forgot all of them.
 * <p>
 * Some sources reach records no type name announces: a {@code TRAVERSE} or a {@code MATCH} walks to whatever types the graph
 * leads to, a function call in {@code FROM} returns what it computes, and a bucket, an index, the schema, a record id or a
 * parameter are not types at all. A view reading one of them cannot be kept current by the record events of the named types,
 * so {@link #untrackable()} names the first such source, and {@code REFRESH INCREMENTAL} is refused for it.
 * <p>
 * Values reached by following a link or an edge from a tracked record (a {@code address.city} projection, an {@code out()}
 * call) are not sources either: no static reading of the query can tell a link from an embedded value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MaterializedViewSources {
  private final List<String> typeNames;
  private final String       untrackable;

  private MaterializedViewSources(final List<String> typeNames, final String untrackable) {
    this.typeNames = typeNames;
    this.untrackable = untrackable;
  }

  /**
   * @param query the defining query
   *
   * @throws CommandSQLParsingException if the query does not parse
   */
  static MaterializedViewSources of(final Database database, final String query) {
    final ParseTree tree = new SQLAntlrParser(database).parseWithTree(query).tree();
    final Set<String> typeNames = new LinkedHashSet<>();
    final String[] untrackable = { null };
    collect(tree, typeNames, untrackable);
    return new MaterializedViewSources(List.copyOf(typeNames), untrackable[0]);
  }

  /** The types the query reads, in the order the query first names them. Their subtypes are read too. */
  List<String> typeNames() {
    return typeNames;
  }

  /** A description of the first source no type's record events report, or null when the query reads none. */
  String untrackable() {
    return untrackable;
  }

  /** Refuses, naming the source, a view that REFRESH INCREMENTAL could not keep current. */
  void checkIncremental(final String viewName) {
    if (untrackable != null)
      throw new SchemaException("Materialized view '" + viewName + "' cannot be refreshed with REFRESH INCREMENTAL: its query reads "
          + untrackable + ", whose changes no write to a type the query names reports, so the view would stay VALID while out of "
          + "date. Use REFRESH MANUAL or REFRESH EVERY <n> <unit> instead");
  }

  private static void collect(final ParseTree node, final Set<String> typeNames, final String[] untrackable) {
    if (node instanceof SQLParser.FromIdentifierContext from) {
      final String name = identifierName(from.identifier(0));
      // A $variable names a LET result, whose own subquery the walk reaches anyway
      if (name != null && !name.startsWith("$"))
        typeNames.add(name);
    } else if (untrackable[0] == null) {
      final String reason = untrackableSource(node);
      if (reason != null)
        untrackable[0] = reason;
    }

    for (int i = 0; i < node.getChildCount(); i++)
      collect(node.getChild(i), typeNames, untrackable);
  }

  private static String untrackableSource(final ParseTree node) {
    if (node instanceof SQLParser.TraverseStatementContext)
      return "a TRAVERSE";
    if (node instanceof SQLParser.MatchStatementContext)
      return "a MATCH";
    if (node instanceof SQLParser.FromFunctionCallContext f)
      return "the function call '" + f.getText() + "' in FROM";
    if (node instanceof SQLParser.FromBucketContext || node instanceof SQLParser.FromBucketListContext
        || node instanceof SQLParser.FromBucketParameterContext)
      return "the bucket '" + ((ParserRuleContext) node).getText() + "' in FROM";
    if (node instanceof SQLParser.FromIndexContext f)
      return "the index '" + f.getText() + "' in FROM";
    if (node instanceof SQLParser.FromSchemaContext f)
      return "the schema metadata '" + f.getText() + "' in FROM";
    if (node instanceof SQLParser.FromRidsContext || node instanceof SQLParser.FromRidArrayContext)
      return "the record ids '" + ((ParserRuleContext) node).getText() + "' in FROM";
    if (node instanceof SQLParser.FromParamContext || node instanceof SQLParser.FromParamArrayContext)
      return "the parameter '" + ((ParserRuleContext) node).getText() + "' in FROM";
    return null;
  }

  private static String identifierName(final SQLParser.IdentifierContext ctx) {
    if (ctx == null)
      return null;
    if (ctx.QUOTED_IDENTIFIER() != null)
      return Identifier.quoted(ctx.QUOTED_IDENTIFIER().getText()).getStringValue();
    return ctx.getText();
  }

}
