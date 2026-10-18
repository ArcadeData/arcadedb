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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.InsertBody;
import com.arcadedb.query.sql.parser.InsertSetExpression;
import com.arcadedb.query.sql.parser.InsertStatement;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.UpdateItem;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class InsertExecutionPlanner {

  protected Identifier      targetType;
  protected Identifier      targetBucketName;
  protected Bucket          targetBucket;
  protected InsertBody      insertBody;
  protected Projection      returnStatement;
  protected SelectStatement selectStatement;
  protected boolean         onDuplicateKeySkip;

  private static final Pattern NESTED_STATEMENT = Pattern.compile("\\(\\s*(SELECT|MATCH|TRAVERSE)\\b", Pattern.CASE_INSENSITIVE);

  public InsertExecutionPlanner() {
  }

  public InsertExecutionPlanner(final InsertStatement statement) {
    this.targetType = statement.getTargetType() == null ? null : statement.getTargetType().copy();
    this.targetBucketName = statement.getTargetBucketName() == null ? null : statement.getTargetBucketName().copy();
    this.targetBucket = statement.getTargetBucket() == null ? null : statement.getTargetBucket().copy();
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement = statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
    this.selectStatement = statement.getSelectStatement() == null ? null : statement.getSelectStatement().copy();
    this.onDuplicateKeySkip = statement.isOnDuplicateKeySkip();
  }

  public InsertExecutionPlan createExecutionPlan(final CommandContext context) {

    if (targetType != null && targetType.getStringValue().startsWith("$")) {
      String variable = (String) context.getVariable(targetType.getStringValue());
      targetType = new Identifier(variable);
    }

    final InsertExecutionPlan result = new InsertExecutionPlan(context);

    if (selectStatement != null) {
      handleInsertSelect(result, this.selectStatement, context);
    } else {
      handleCreateRecord(result, this.insertBody, context);
    }
    handleTargetType(result, targetType, context);
    handleSetFields(result, insertBody, context);
    if (targetBucket != null) {
      String name = resolveBucketName(targetBucket, context);
      if (name == null && targetBucket.getBucketNumber() != null) {
        name = context.getDatabase().getSchema().getBucketById(targetBucket.getBucketNumber()).getName();
      }
      handleSave(result, new Identifier(name), context);
    } else {
      handleSave(result, targetBucketName, context);
    }
    handleReturn(result, returnStatement, context);

    return result;
  }

  private void handleSave(final InsertExecutionPlan result, final Identifier targetClusterName, final CommandContext context) {
    result.chain(new SaveElementStep(context, targetClusterName, true, onDuplicateKeySkip));
  }

  private void handleReturn(final InsertExecutionPlan result, final Projection returnStatement, final CommandContext context) {
    if (returnStatement != null)
      result.chain(new ProjectionCalculationStep(returnStatement, context));
  }

  private void handleSetFields(final InsertExecutionPlan result, final InsertBody insertBody, final CommandContext context) {
    if (insertBody == null)
      return;

    if (insertBody.getIdentifierList() != null) {
      result.chain(
          new InsertValuesStep(insertBody.getIdentifierList(), insertBody.getValueExpressions(), context));
    } else if (insertBody.getJsonContent() != null) {
      result.chain(new UpdateContentStep(insertBody.getJsonContent(), context));
    } else if (insertBody.getJsonArrayContent() != null) {
      result.chain(new UpdateContentStep(insertBody.getJsonArrayContent(), context));
    } else if (insertBody.getContentInputParam() != null) {
      result.chain(new UpdateContentStep(insertBody.getContentInputParam(), context));
    } else if (insertBody.getSetExpressions() != null) {
      final List<UpdateItem> items = new ArrayList<>();
      for (final InsertSetExpression exp : insertBody.getSetExpressions()) {
        final UpdateItem item = new UpdateItem();
        item.setOperator(UpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, context));
    }
  }

  private void handleTargetType(final InsertExecutionPlan result, final Identifier targetType, final CommandContext context) {
    if (targetType != null)
      result.chain(new SetDocumentStepStep(targetType, context));
  }

  private void handleCreateRecord(final InsertExecutionPlan result, final InsertBody body, final CommandContext context) {
    int tot = 1;
    if (body != null && body.getValueExpressions() != null && body.getValueExpressions().size() > 0)
      tot = body.getValueExpressions().size();
    else if (body != null && body.getJsonArrayContent() != null && body.getJsonArrayContent().items.size() > 0)
      tot = body.getJsonArrayContent().items.size();
    else if (body != null && body.getContentInputParam() != null) {
      // A CONTENT input parameter that resolves to a List must create one record per item, exactly like the
      // JSON-array literal form above (#6463). The parameter is already bound in the context by the time the
      // execution plan is created (see InsertStatement.execute), so it can be sized here rather than deferred.
      final Object paramValue = body.getContentInputParam().getValue(context.getInputParameters());
      if (paramValue instanceof List<?> list && !list.isEmpty())
        tot = list.size();
    }

    if (targetType == null && targetBucket != null) {
      // #5636: the `else bucket = null` arm below makes this guard reachable, but ONLY for the case where no name
      // and no number were given at all. An unknown name or id still raised out of the throwing lookups before it
      // could run - i.e. the guard was dead for exactly the input it reads as though it handles.
      final com.arcadedb.engine.Bucket bucket;
      final String resolvedName = resolveBucketName(targetBucket, context);
      if (resolvedName != null)
        bucket = context.getDatabase().getSchema().getBucketByNameIfExists(resolvedName);
      else if (targetBucket.getBucketNumber() != null)
        bucket = context.getDatabase().getSchema().getBucketByIdIfExists(targetBucket.getBucketNumber());
      else
        bucket = null;

      if (bucket == null)
        // Now that the unknown-name / unknown-id cases actually reach this branch, "not specified" would be a lie
        // for two of the three ways to get here: the target WAS specified, it just does not exist.
        throw new CommandSQLParsingException(resolvedName != null ?
            "Target bucket '" + resolvedName + "' not found" :
            targetBucket.getBucketNumber() != null ?
                "Target bucket with id " + targetBucket.getBucketNumber() + " not found" :
                // Same wording as handleInsertSelect below: it is the bucket that is missing, and the two paths
                // answer the same question.
                "Target bucket not specified");

      targetType = new Identifier(context.getDatabase().getSchema().getTypeNameByBucketId(bucket.getFileId()));
    }

    result.chain(new CreateRecordStep(targetType.getStringValue(), context, tot));
  }

  /**
   * Resolves the bucket name from a Bucket object, handling parameterized bucket names.
   * If the bucket has an inputParam, it resolves the parameter value from the context.
   *
   * @param bucket  the bucket object to resolve
   * @param context the command context containing input parameters
   * @return the resolved bucket name, or null if it cannot be resolved
   */
  private String resolveBucketName(final Bucket bucket, final CommandContext context) {
    // First check for literal bucket name
    if (bucket.getBucketName() != null) {
      return bucket.getBucketName();
    }

    // Check for input parameter
    if (bucket.getInputParam() != null) {
      final Object paramValue = bucket.getInputParam().getValue(context.getInputParameters());
      if (paramValue != null) {
        return paramValue.toString();
      }
    }

    return null;
  }

  private void handleInsertSelect(final InsertExecutionPlan result, final SelectStatement selectStatement,
      final CommandContext context) {
    final InternalExecutionPlan subPlan = selectStatement.createExecutionPlan(context);
    result.chain(new SubQueryStep(subPlan, context, context));

    // If targetType is null but targetBucket is specified, derive the type from the bucket
    Identifier effectiveTargetType = targetType;
    if (effectiveTargetType == null && targetBucket != null) {
      // #5636: identical in shape to handleCreateRecord above, which it had drifted from in three ways. It read
      // getBucketName() directly instead of resolveBucketName(), so a PARAMETERIZED target - INSERT INTO bucket:?
      // FROM SELECT ... - never resolved its parameter and failed even for a bucket that exists. It had no
      // `else bucket = null` arm, so an unknown bucket lost its message to a generic SchemaException and a target
      // with neither name nor number unboxed null on getBucketNumber(). And its message named nothing.
      final com.arcadedb.engine.Bucket bucket = findTargetBucket(context);
      if (bucket == null) {
        final String resolvedName = resolveBucketName(targetBucket, context);
        throw new CommandSQLParsingException(resolvedName != null ?
            "Target bucket '" + resolvedName + "' not found" :
            targetBucket.getBucketNumber() != null ?
                "Target bucket with id " + targetBucket.getBucketNumber() + " not found" :
                "Target bucket not specified");
      }

      effectiveTargetType = new Identifier(context.getDatabase().getSchema().getTypeNameByBucketId(bucket.getFileId()));
    }

    // #10192 (Halloween problem): a SELECT that can read a bucket the statement writes to would meet the rows it inserts -
    // a scan per OR branch copies a row twice, an index range scan never ends - so it is drained before the first insert
    if (hasExpressionSubQuery(selectStatement) || readsAny(subPlan, targetBucketIds(effectiveTargetType, context)))
      result.chain(new MaterializeRowsStep(context));

    if (effectiveTargetType != null)
      result.chain(new CopyDocumentStep(context, effectiveTargetType.getStringValue()));

    result.chain(new RemoveEdgePointersStep(context));
  }

  /**
   * Whether the SELECT nests a statement that its plan does not show. The planner lifts most subqueries into LET steps, whose plans
   * {@link #readsAny} walks, but one buried in an expression - {@code (SELECT ...)[0].c} in a projection - is run row by row by the
   * expression itself, and would read the rows inserted before. The AST offers no complete walk of its nodes (most node classes do
   * not list their children), so this asks the statement's text with its quoted parts - string literals and quoted identifiers -
   * blanked out, so a literal that reads like a subquery does not count.
   */
  private static boolean hasExpressionSubQuery(final SelectStatement statement) {
    return NESTED_STATEMENT.matcher(withoutQuotedText(statement.toString())).find();
  }

  /** {@code text} with what is between single quotes, double quotes or backticks replaced by spaces, escapes honoured. */
  static String withoutQuotedText(final String text) {
    final char[] chars = text.toCharArray();
    char quote = 0;
    for (int i = 0; i < chars.length; i++) {
      final char c = chars[i];
      if (quote == 0) {
        if (c == '\'' || c == '"' || c == '`')
          quote = c;
      } else if (c == '\\' && i + 1 < chars.length) {
        chars[i] = ' ';
        chars[++i] = ' ';
      } else if (c == quote)
        quote = 0;
      else
        chars[i] = ' ';
    }
    return new String(chars);
  }

  /**
   * @return the buckets the statement can write to: the target bucket when one is named, otherwise every bucket of the target
   * type (its own, not its sub-types': a record inserted into a type is never stored in a sub-type's bucket), or null when
   * they cannot be told, which is read as "any"
   */
  private Set<Integer> targetBucketIds(final Identifier effectiveTargetType, final CommandContext context) {
    // com.arcadedb.engine.Bucket is spelled out below, as in the rest of this class: the parser's Bucket is imported
    final Schema schema = context.getDatabase().getSchema();
    final Set<Integer> bucketIds = new HashSet<>();
    if (targetBucket != null) {
      final com.arcadedb.engine.Bucket bucket = findTargetBucket(context);
      if (bucket == null)
        return null;
      bucketIds.add(bucket.getFileId());
      return bucketIds;
    }
    if (effectiveTargetType == null)
      return null;
    final DocumentType type = schema.getTypeOrNull(effectiveTargetType.getStringValue());
    if (type == null)
      return null;
    for (final com.arcadedb.engine.Bucket bucket : type.getBuckets(false))
      bucketIds.add(bucket.getFileId());
    return bucketIds;
  }

  /** The bucket the statement names as its target, by name, parameter or number, or null when it names none that exists. */
  private com.arcadedb.engine.Bucket findTargetBucket(final CommandContext context) {
    final Schema schema = context.getDatabase().getSchema();
    final String name = resolveBucketName(targetBucket, context);
    if (name != null)
      return schema.getBucketByNameIfExists(name);
    if (targetBucket.getBucketNumber() != null)
      return schema.getBucketByIdIfExists(targetBucket.getBucketNumber());
    return null;
  }

  /**
   * Whether the plan, its sub-steps or its sub-plans read one of the buckets: a bucket scan (a type scan is one per bucket), an
   * index lookup or range (by the buckets the index covers), and conservatively every source whose buckets cannot be told - an
   * indexed function (full-text, geospatial, vector), the lightweight edges of a type and a correlated LET subquery, whose plan
   * is only built row by row.
   */
  private static boolean readsAny(final ExecutionPlan plan, final Set<Integer> bucketIds) {
    for (final ExecutionStep step : plan.getSteps())
      if (readsAny(step, bucketIds))
        return true;
    return false;
  }

  private static boolean readsAny(final ExecutionStep step, final Set<Integer> bucketIds) {
    if (step instanceof final FetchFromClusterExecutionStep bucketStep)
      return bucketIds == null || bucketIds.contains(bucketStep.getBucketId());
    if (step instanceof final ScanWithFilterStep bucketStep)
      return bucketIds == null || bucketIds.contains(bucketStep.getBucketId());
    if (step instanceof final FetchFromIndexStep indexStep)
      return indexReadsAny(indexStep.index, bucketIds);
    if (step instanceof FetchFromIndexedFunctionStep || step instanceof FetchFromLightweightEdgeTypeStep || step instanceof LetQueryStep
        || step instanceof FetchFromTimeSeriesStep)
      return true;

    for (final ExecutionStep subStep : step.getSubSteps())
      if (readsAny(subStep, bucketIds))
        return true;
    if (step instanceof final ExecutionStepInternal internal)
      for (final ExecutionPlan subPlan : internal.getSubExecutionPlans())
        if (readsAny(subPlan, bucketIds))
          return true;
    return false;
  }

  private static boolean indexReadsAny(final Index index, final Set<Integer> bucketIds) {
    if (bucketIds == null)
      return true;
    if (index instanceof final TypeIndex typeIndex) {
      for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
        if (bucketIds.contains(bucketIndex.getAssociatedBucketId()))
          return true;
      return false;
    }
    final int bucketId = index.getAssociatedBucketId();
    // AN INDEX THAT NAMES NO BUCKET COULD COVER ANY OF THEM
    return bucketId < 0 || bucketIds.contains(bucketId);
  }
}
