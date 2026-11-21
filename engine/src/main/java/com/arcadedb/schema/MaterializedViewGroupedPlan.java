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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.SqlAstInspector;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.ArrayLiteralExpression;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.BaseIdentifier;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FloatingPoint;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.GeOperator;
import com.arcadedb.query.sql.parser.GtOperator;
import com.arcadedb.query.sql.parser.ILikeOperator;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.InOperator;
import com.arcadedb.query.sql.parser.IsDefinedCondition;
import com.arcadedb.query.sql.parser.IsNotDefinedCondition;
import com.arcadedb.query.sql.parser.IsNotNullCondition;
import com.arcadedb.query.sql.parser.IsNullCondition;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.query.sql.parser.LikeOperator;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.MathExpression;
import com.arcadedb.query.sql.parser.NeOperator;
import com.arcadedb.query.sql.parser.NeqOperator;
import com.arcadedb.query.sql.parser.NotBlock;
import com.arcadedb.query.sql.parser.NullSafeEqualsCompareOperator;
import com.arcadedb.query.sql.parser.OrBlock;
import com.arcadedb.query.sql.parser.PInteger;
import com.arcadedb.query.sql.parser.PNumber;
import com.arcadedb.query.sql.parser.ParenthesisBlock;
import com.arcadedb.query.sql.parser.ParenthesisExpression;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.RecordAttribute;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.SimpleNode;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.SuffixIdentifier;
import com.arcadedb.query.sql.parser.WhereClause;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The shape of a grouped materialized view whose rows a committed transaction can update by its delta (issue #11937): a SELECT
 * over one source type, grouped by plain properties, projecting those properties and {@code count(*)}, {@code count(p)},
 * {@code sum(p)}, {@code avg(p)}, {@code min(p)} and {@code max(p)} of plain properties, with a WHERE that reads the record and
 * nothing else. Anything else (HAVING-like filters on aggregates do not parse as such, ORDER BY, SKIP, LIMIT, LET, UNWIND, DISTINCT,
 * expressions, subqueries, functions or methods in the WHERE) has no plan, and the view keeps the full pass after every commit.
 * <p>
 * Immutable and shared: the WHERE is evaluated on a copy per caller ({@link #newWhere()}), as the AST of a condition is not meant
 * for concurrent evaluation.
 */
final class MaterializedViewGroupedPlan {
  enum Aggregate {
    COUNT_STAR, COUNT, SUM, AVG, MIN, MAX
  }

  private static final String  NAME              = "([A-Za-z_][A-Za-z0-9_]*|`[^`]+`)";
  private static final Pattern PLAIN_PROPERTY    = Pattern.compile(NAME);
  private static final Pattern AGGREGATE_OF_NAME = Pattern.compile("(?i)(count|sum|avg|min|max)\\(\\s*(\\*|" + NAME + ")\\s*\\)");

  /**
   * The node types of a condition that reads the properties of the record and nothing else: the boolean connectives, the scalar
   * comparisons, arithmetic, and literals. A closed list, so that a construct nobody thought of (a link followed, a list element or
   * range, a condition applied to the records a list holds, an embedded value, a function, a method, a parameter, a subquery) keeps
   * the view on the full pass instead of reading some other record, which no commit of the source would then refresh: the state
   * before an update or a delete is evaluated against the data as it is now, so a view reading across a link would drift for good.
   */
  private static final Set<Class<?>> RECORD_ONLY_NODES = Set.of(WhereClause.class, OrBlock.class, AndBlock.class, NotBlock.class,
      ParenthesisBlock.class, BinaryCondition.class, BetweenCondition.class, IsNullCondition.class, IsNotNullCondition.class,
      IsDefinedCondition.class, IsNotDefinedCondition.class, InCondition.class, EqualsCompareOperator.class, NeOperator.class,
      NeqOperator.class, NullSafeEqualsCompareOperator.class, GtOperator.class, GeOperator.class, LtOperator.class, LeOperator.class,
      LikeOperator.class, ILikeOperator.class, InOperator.class, Expression.class, MathExpression.class, BaseExpression.class,
      ParenthesisExpression.class, ArrayLiteralExpression.class, BaseIdentifier.class, SuffixIdentifier.class, Identifier.class,
      RecordAttribute.class, Rid.class, PNumber.class, PInteger.class, FloatingPoint.class);

  final         String      sourceType;
  private final WhereClause where;
  /** The GROUP BY properties, in their order. */
  final         String[]    keyProperties;
  final         Aggregate[] aggregates;
  /** The property each aggregate reads, null for {@code count(*)}. */
  final         String[]    aggregateProperties;
  /** The output columns, in projection order. */
  final         String[]    columnNames;
  /** Per column: the index of its key property when {@code >= 0}, otherwise {@code -1 - index} of its aggregate. */
  final         int[]       columnSources;

  private MaterializedViewGroupedPlan(final String sourceType, final WhereClause where, final String[] keyProperties,
      final Aggregate[] aggregates, final String[] aggregateProperties, final String[] columnNames, final int[] columnSources) {
    this.sourceType = sourceType;
    this.where = where;
    this.keyProperties = keyProperties;
    this.aggregates = aggregates;
    this.aggregateProperties = aggregateProperties;
    this.columnNames = columnNames;
    this.columnSources = columnSources;
  }

  /** The plan of {@code sql}, or null when a commit's delta cannot be applied to its rows exactly. */
  static MaterializedViewGroupedPlan of(final Database database, final String sql) {
    try {
      final Statement parsed = ((DatabaseInternal) database).getStatementCache().get(sql);
      if (!(parsed instanceof SelectStatement select))
        return null;
      if (select.getGroupBy() == null || select.getGroupBy().getItems() == null || select.getGroupBy().getItems().isEmpty())
        return null;
      // ORDER BY decides the order the full pass writes the rows in, which a delta cannot keep; the others change which rows exist
      if (select.getOrderBy() != null || select.getLimit() != null || select.getSkip() != null || select.getLetClause() != null
          || select.getUnwind() != null || select.getFacets() != null || select.timeout != null)
        return null;

      final List<String> sourceTypes = MaterializedViewSources.of(database, sql).typeNames();
      if (sourceTypes.size() != 1)
        return null;
      if (select.getTarget() == null || select.getTarget().getItem() == null)
        return null;
      final FromItem from = select.getTarget().getItem();
      if (from.identifier == null || from.modifier != null || from.statement != null || from.functionCall != null
          || from.bucket != null || from.bucketList != null || from.index != null || from.schema != null || from.rids != null
          || from.inputParam != null || from.inputParams != null)
        return null;
      final String sourceType = from.identifier.getStringValue();
      if (!sourceType.equals(sourceTypes.getFirst()))
        return null;

      // a subquery anywhere, the WHERE included, reads more than the record
      if (countSelects(new SQLAntlrParser(database).parseWithTree(sql).tree()) != 1)
        return null;

      final WhereClause where = select.getWhereClause();
      if (where != null && !SqlAstInspector.allNodesMatch(where, MaterializedViewGroupedPlan::readsOnlyTheRecord))
        return null;

      final Projection projection = select.getProjection();
      if (projection == null || projection.isDistinct() || projection.getItems() == null || projection.getItems().isEmpty())
        return null;

      final List<String> keys = new ArrayList<>();
      for (final Expression item : select.getGroupBy().getItems()) {
        final String key = plainProperty(item.toString());
        if (key == null || keys.contains(key))
          return null;
        keys.add(key);
      }

      final List<Aggregate> aggregates = new ArrayList<>();
      final List<String> aggregateProperties = new ArrayList<>();
      final List<String> columnNames = new ArrayList<>();
      final List<Integer> columnSources = new ArrayList<>();
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);
      for (final ProjectionItem item : projection.getItems()) {
        if (item.isAll() || item.isExpand() || item.getExpression() == null)
          return null;
        final String alias = item.getProjectionAliasAsString();
        if (alias == null || alias.startsWith("@") || columnNames.contains(alias))
          return null;
        final String text = item.getExpression().toString();
        if (item.isAggregate(context)) {
          final Matcher matcher = AGGREGATE_OF_NAME.matcher(text);
          if (!matcher.matches())
            return null;
          final Aggregate aggregate;
          final String argument = matcher.group(2);
          final String property = "*".equals(argument) ? null : plainProperty(argument);
          switch (matcher.group(1).toLowerCase(Locale.ENGLISH)) {
          case "count" -> aggregate = property == null ? Aggregate.COUNT_STAR : Aggregate.COUNT;
          case "sum" -> aggregate = Aggregate.SUM;
          case "avg" -> aggregate = Aggregate.AVG;
          case "min" -> aggregate = Aggregate.MIN;
          default -> aggregate = Aggregate.MAX;
          }
          if (property == null && aggregate != Aggregate.COUNT_STAR)
            return null;
          columnSources.add(-1 - aggregates.size());
          aggregates.add(aggregate);
          aggregateProperties.add(property);
        } else {
          // a non-aggregate column of a group is the value of one of its keys, or the query reads it off one record of the group
          final String property = plainProperty(text);
          if (property == null || !keys.contains(property))
            return null;
          columnSources.add(keys.indexOf(property));
        }
        columnNames.add(alias);
      }

      // GROUP BY can name a projection alias, which then groups by the aliased expression rather than by the property of that name
      for (final ProjectionItem item : projection.getItems())
        if (keys.contains(item.getProjectionAliasAsString()) && !item.getProjectionAliasAsString()
            .equals(plainProperty(item.getExpression().toString())))
          return null;

      return new MaterializedViewGroupedPlan(sourceType, where, keys.toArray(new String[0]), aggregates.toArray(new Aggregate[0]),
          aggregateProperties.toArray(new String[0]), columnNames.toArray(new String[0]),
          columnSources.stream().mapToInt(Integer::intValue).toArray());
    } catch (final Exception e) {
      // The view then keeps the full pass for the life of this instance (the answer is cached): say why, for the one who wonders
      LogManager.instance().log(MaterializedViewGroupedPlan.class, Level.FINE, "No delta plan for the view query '%s': %s", e, sql,
          e.getMessage());
      return null;
    }
  }

  /**
   * A condition that reads the record and nothing else: only nodes of {@link #RECORD_ONLY_NODES}, over plain property names and
   * record attributes (no context variable: {@code $parent}, {@code $current}...).
   */
  private static boolean readsOnlyTheRecord(final SimpleNode node) {
    // the TRUE and FALSE literals are instances of anonymous classes
    if (node == BooleanExpression.TRUE || node == BooleanExpression.FALSE)
      return true;
    if (!RECORD_ONLY_NODES.contains(node.getClass()))
      return false;
    if (node instanceof Identifier identifier)
      return identifier.getStringValue() == null || !identifier.getStringValue().startsWith("$");
    return !(node instanceof SuffixIdentifier suffix) || !suffix.star;
  }

  /** The property a rendered expression names, unquoted, or null when it is anything but one plain property. */
  private static String plainProperty(final String text) {
    if (text == null)
      return null;
    final String trimmed = text.trim();
    if (!PLAIN_PROPERTY.matcher(trimmed).matches())
      return null;
    return trimmed.startsWith("`") ? trimmed.substring(1, trimmed.length() - 1) : trimmed;
  }

  private static int countSelects(final ParseTree node) {
    int count = node instanceof SQLParser.SelectStatementContext ? 1 : 0;
    for (int i = 0; i < node.getChildCount(); i++)
      count += countSelects(node.getChild(i));
    return count;
  }

  /** A copy of the WHERE for one caller's evaluations, null when there is none. */
  WhereClause newWhere() {
    return where != null ? where.copy() : null;
  }

  static CommandContext newContext(final Database database) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    return context;
  }

  /**
   * What one record adds to the view: null when the WHERE leaves it out, {@link Contribution#INEXACT} when one of the values it
   * reads is outside what a delta can apply exactly (see {@link MaterializedViewGroupedState}), otherwise its group key and the
   * value each aggregate reads.
   */
  Contribution contributionOf(final Document record, final WhereClause whereCopy, final CommandContext context) {
    if (whereCopy != null && !Boolean.TRUE.equals(whereCopy.matchesFilters(record, context)))
      return null;

    final Object[] rawKeys = new Object[keyProperties.length];
    final Object[] normalized = new Object[keyProperties.length];
    for (int i = 0; i < keyProperties.length; i++) {
      final Object value = record.get(keyProperties[i]);
      if (!isExactKey(value))
        return Contribution.INEXACT;
      rawKeys[i] = value;
      normalized[i] = Type.normalizeForKey(value);
    }

    final Object[] arguments = new Object[aggregates.length];
    for (int i = 0; i < aggregates.length; i++) {
      if (aggregates[i] == Aggregate.COUNT_STAR)
        continue;
      final Object value = record.get(aggregateProperties[i]);
      if (value != null && !isExactArgument(aggregates[i], value))
        return Contribution.INEXACT;
      arguments[i] = value;
    }
    // a null key is a group of its own, as for the query: Arrays.asList takes nulls, and compares as List.equals does
    return new Contribution(Arrays.asList(normalized), rawKeys, arguments);
  }

  /**
   * The key values a group can be found by exactly. The query groups numerically equal numbers of different classes together and
   * shows the value of the group's first record, so each group also has to see one class only (checked by the state).
   */
  private static boolean isExactKey(final Object value) {
    return value == null || value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Boolean;
  }

  /**
   * {@code sum} and {@code avg} over {@code Integer} and {@code Long} only: their total is exact and the class of the result follows
   * from the values (see {@code NumericSum}); a floating point total depends on the order the values are added in. {@code min} and
   * {@code max} over {@code Integer}, {@code Long} and {@code String}: one class per group, compared as the functions compare.
   */
  private static boolean isExactArgument(final Aggregate aggregate, final Object value) {
    return switch (aggregate) {
      case COUNT_STAR, COUNT -> true;
      case SUM, AVG -> value instanceof Integer || value instanceof Long;
      case MIN, MAX -> value instanceof Integer || value instanceof Long || value instanceof String;
    };
  }

  /** One record's place in the view: its group, the raw values of the key (what the row shows) and each aggregate's argument. */
  static final class Contribution {
    static final Contribution INEXACT = new Contribution(null, null, null);

    final List<Object> key;
    final Object[]     rawKeys;
    final Object[]     arguments;

    Contribution(final List<Object> key, final Object[] rawKeys, final Object[] arguments) {
      this.key = key;
      this.rawKeys = rawKeys;
      this.arguments = arguments;
    }

    boolean sameAs(final Contribution other) {
      if (this == other)
        return true;
      if (other == null || this == INEXACT || other == INEXACT)
        return false;
      return key.equals(other.key) && Arrays.equals(rawKeys, other.rawKeys) && Arrays.equals(arguments, other.arguments);
    }
  }
}
