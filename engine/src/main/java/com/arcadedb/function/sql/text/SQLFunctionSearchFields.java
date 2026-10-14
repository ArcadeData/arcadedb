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
package com.arcadedb.function.sql.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexableSQLFunction;
import com.arcadedb.query.sql.executor.IndexedFunctionPushdown;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/**
 * SQL function to search full-text indexes by field names.
 * Automatically finds the appropriate full-text index for the given fields.
 * <p>
 * Usage: fulltext.searchFields(['field1', 'field2'], 'query')
 * Example: SELECT FROM Article WHERE fulltext.searchFields(['title', 'content'], 'java')
 * <p>
 * The search is restricted to the given fields (issue #9910): on an index over more properties than those, every
 * unqualified clause of the query is searched in the given fields only, as the OR of the clause qualified with each of
 * them, and a clause the query qualifies itself keeps its field. It is therefore the same search, with the same scores,
 * as {@code SEARCH_INDEX} on that index with the clauses qualified by hand; on an index over exactly the given fields it
 * is {@code SEARCH_INDEX} on that index.
 * <p>
 * Implements {@link IndexableSQLFunction}, so a WHERE condition on it is answered by the index rather than by a scan of
 * the type, with the pushdowns of {@code SEARCH_INDEX}: lazy loading, LIMIT and {@code @rid} conditions.
 * <p>
 * Legacy alias: {@code search_fields} (snake_case, kept for backward compatibility).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionSearchFields extends SQLFunctionAbstract implements IndexableSQLFunction {
  public static final String NAME  = "fulltext.searchFields";
  public static final String ALIAS = "search_fields";

  public SQLFunctionSearchFields() {
    super(NAME);
  }

  @Override
  public String getAlias() {
    return ALIAS;
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_FIELDS() requires 2 parameters: fields array and query");

    if (iParams[0] == null || iParams[1] == null)
      throw new CommandExecutionException("SEARCH_FIELDS() parameters cannot be null");

    // The type comes from the current record: without a FROM there is no index to choose
    if (iCurrentRecord == null)
      throw new CommandExecutionException("SEARCH_FIELDS() requires a type context (use in WHERE clause with FROM)");

    final List<String> fieldNames = fieldNames(iParams[0]);
    final String queryString = iParams[1].toString();
    final String typeName = iCurrentRecord.asDocument().getTypeName();

    // Resolved once per statement and type, not once per row: the indexed fetch registers the search it ran for its target
    // type and every subtype, so the rows it returns are evaluated again on the very same index
    final String resolvedKey = resolvedSearchKey(typeName, fieldNames, queryString);
    FullTextFunctionSearch search = (FullTextFunctionSearch) iContext.getCachedValue(resolvedKey);
    if (search == null) {
      search = search(iContext.getDatabase().getSchema().getType(typeName), fieldNames, queryString);
      if (search == null)
        throw new CommandExecutionException("No full-text index found for fields: " + fieldNames);
      iContext.setCachedValue(resolvedKey, search);
    }

    return search.evaluate(iCurrentRecord, iContext);
  }

  // ---- IndexableSQLFunction implementation ----

  @Override
  public boolean shouldExecuteAfterSearch(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Must still execute per-row after the indexed fetch to set $score in the context
    return true;
  }

  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Only `= true` asks for the matches (see IndexableSQLFunction.selectsMatches). Planning only asks: nothing is registered,
    // and arguments the call rejects are left to the per-row evaluation to report, as before the function was indexable
    if (!IndexableSQLFunction.selectsMatches(operator, right))
      return false;
    try {
      return search(target, context, oExpressions) != null;
    } catch (final CommandExecutionException e) {
      return false;
    }
  }

  @Override
  public boolean canExecuteInline(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Can always fall back to full scan with per-row evaluation
    return true;
  }

  @Override
  public long estimate(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions) {
    return -1;
  }

  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions) {
    return searchFromTarget(target, operator, rightValue, context, oExpressions, IndexedFunctionPushdown.NONE);
  }

  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions, final int limit) {
    return searchFromTarget(target, operator, rightValue, context, oExpressions, IndexedFunctionPushdown.ofLimit(limit));
  }

  /** The records of the search of the full-text index of the target type that covers the fields, as SEARCH_INDEX answers them. */
  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions,
      final IndexedFunctionPushdown pushdown) {
    final FullTextFunctionSearch search = search(target, context, oExpressions);
    if (search == null)
      return List.of();
    registerForHierarchy(target, oExpressions, search, context);
    return search.search(target, context, pushdown);
  }

  @Override
  public boolean supportsRIDOrder() {
    return true;
  }

  @Override
  public boolean answersByScore() {
    return true;
  }

  @Override
  public Object getScoringExplain(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions) {
    final FullTextFunctionSearch search = search(target, context, oExpressions);
    return search != null ? search.explainScoring() : null;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_FIELDS(<fields-array>, <query>)";
  }

  // ---- Private helpers ----

  /**
   * The search a call answers on the target type of an indexed fetch, or null when it has no index to search: when the
   * target is not a type, or an argument depends on the record (it evaluates to null without one).
   */
  private static FullTextFunctionSearch search(final FromClause target, final CommandContext context,
      final Expression[] oExpressions) {
    if (oExpressions == null || oExpressions.length < 2)
      return null;
    final Identifier targetType = target != null && target.getItem() != null ? target.getItem().getIdentifier() : null;
    if (targetType == null)
      return null;
    final DocumentType type = context.getDatabase().getSchema().getType(targetType.getStringValue());
    final Object fields = oExpressions[0].execute((Identifiable) null, context);
    final String queryString = SQLFunctionSearchIndex.resolveStringParam(oExpressions[1], context);
    if (type == null || fields == null || queryString == null)
      return null;
    return search(type, fieldNames(fields), queryString);
  }

  /**
   * Registers the search the indexed fetch runs for its target type and each of its subtypes, whose records it returns, so
   * that evaluating the condition again on those rows uses the same index - and the same scores - rather than one a
   * subtype may define for itself.
   */
  private static void registerForHierarchy(final FromClause target, final Expression[] oExpressions,
      final FullTextFunctionSearch search, final CommandContext context) {
    final DocumentType type = context.getDatabase().getSchema().getType(target.getItem().getIdentifier().getStringValue());
    registerForHierarchy(type, fieldNames(oExpressions[0].execute((Identifiable) null, context)),
        SQLFunctionSearchIndex.resolveStringParam(oExpressions[1], context), search, context);
  }

  private static void registerForHierarchy(final DocumentType type, final List<String> fieldNames, final String queryString,
      final FullTextFunctionSearch search, final CommandContext context) {
    context.setCachedValue(resolvedSearchKey(type.getName(), fieldNames, queryString), search);
    for (final DocumentType subType : type.getSubTypes())
      registerForHierarchy(subType, fieldNames, queryString, search, context);
  }

  /** Where the search resolved for a type is kept for the statement. */
  private static String resolvedSearchKey(final String typeName, final List<String> fieldNames, final String queryString) {
    return "search_fields_resolved:" + typeName + ":" + fieldNames + ":" + queryString;
  }

  /**
   * The search of {@code queryString} in {@code fieldNames} on the best full-text index of {@code type} that covers them,
   * or null when none does. The best index is the one over the fewest properties - one over exactly the fields needs no
   * restriction at all - and among those, the first by name, so that every evaluation in a statement picks the same.
   */
  private static FullTextFunctionSearch search(final DocumentType type, final List<String> fieldNames, final String queryString) {
    TypeIndex best = null;
    for (final TypeIndex typeIndex : TypeIndex.filterReadyForQueries(type.getAllIndexes(true)))
      if (typeIndex.getType() == Schema.INDEX_TYPE.FULL_TEXT && covers(typeIndex.getPropertyNames(), fieldNames) && isBetter(
          typeIndex, best))
        best = typeIndex;

    if (best == null)
      return null;
    // The field names are distinct (see fieldNames): the index covers exactly them when it has no other property
    final boolean exactly = best.getPropertyNames().size() == fieldNames.size();
    return new FullTextFunctionSearch(best, queryString, exactly ? null : fieldNames);
  }

  /** Fewer properties first, then the name, so that every evaluation of a statement picks the same index. */
  private static boolean isBetter(final TypeIndex candidate, final TypeIndex best) {
    if (best == null)
      return true;
    final int bySize = Integer.compare(candidate.getPropertyNames().size(), best.getPropertyNames().size());
    return bySize != 0 ? bySize < 0 : candidate.getName().compareTo(best.getName()) < 0;
  }

  /**
   * Whether the index has every field among its properties. A field is named by the property's base name as well: a
   * {@code BY ITEM} property is indexed as {@code tags by item} and queried as {@code tags}.
   */
  private static boolean covers(final List<String> propertyNames, final List<String> fieldNames) {
    for (final String field : fieldNames) {
      boolean found = false;
      for (final String property : propertyNames)
        if (property.equals(field) || Index.basePropertyName(property).equals(field)) {
          found = true;
          break;
        }
      if (!found)
        return false;
    }
    return true;
  }

  /**
   * The field names of the first argument, distinct and sorted: the order and the repetitions of the list change nothing
   * in the search, so {@code ['a', 'b']} and {@code ['b', 'a', 'a']} share one index choice and one cached search.
   */
  private static List<String> fieldNames(final Object fieldsParam) {
    final TreeSet<String> fieldNames = new TreeSet<>();
    if (fieldsParam instanceof final Collection<?> collection)
      for (final Object f : collection)
        fieldNames.add(fieldName(f));
    else if (fieldsParam instanceof final Object[] array)
      for (final Object f : array)
        fieldNames.add(fieldName(f));
    else
      fieldNames.add(fieldName(fieldsParam));
    if (fieldNames.isEmpty())
      throw new CommandExecutionException("SEARCH_FIELDS() requires at least one field name");
    return List.copyOf(fieldNames);
  }

  private static String fieldName(final Object field) {
    if (field == null)
      throw new CommandExecutionException("SEARCH_FIELDS() field names cannot be null");
    return field.toString();
  }
}
