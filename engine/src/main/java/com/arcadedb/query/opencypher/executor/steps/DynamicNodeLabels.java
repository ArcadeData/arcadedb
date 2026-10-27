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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * Evaluates the Cypher 25 dynamic labels of the node patterns of a write clause - {@code CREATE (n:$($label))},
 * {@code MERGE (n:$($label) {...})} - against the input row, so the clause goes on with static labels only.
 * <p>
 * Until issue #10683 these two clauses read {@link NodePattern#getLabels()} alone and ignored {@code getDynamicLabels()}: the
 * statement ran, with no error, and the node was created without the label (in the {@code ~NO_LABEL~} type), where
 * {@code SET}, {@code REMOVE}, {@code MATCH} and {@code WHERE} resolved the same expression. The value contract and the checks
 * are those of {@code SET n:$(expr)} ({@link Labels#appendDynamicLabels}): a string is one label, a list of strings is that
 * many, {@code null} contributes nothing, anything else fails the statement.
 */
final class DynamicNodeLabels {
  private DynamicNodeLabels() {
  }

  /**
   * @param node      the node pattern of the clause
   * @param row       the input row the expressions are evaluated against, null for a clause with no input
   * @param evaluator the evaluator of the step
   * @param context   the command context
   * @param clause    the clause name for the error messages, {@code "CREATE"} or {@code "MERGE"}
   *
   * @return {@code node} itself when it has no dynamic label, otherwise a copy with the static labels followed by the evaluated
   * ones and no dynamic label left
   */
  static NodePattern resolve(final NodePattern node, final Result row, final ExpressionEvaluator evaluator,
      final CommandContext context, final String clause) {
    if (!node.hasDynamicLabels())
      return node;
    if (evaluator == null)
      throw new CommandExecutionException("A dynamic label in " + clause + " needs an expression evaluator");

    final List<String> labels = new ArrayList<>(node.getLabels().size() + node.getDynamicLabels().size());
    labels.addAll(node.getLabels());
    for (final Expression dynamicLabel : node.getDynamicLabels())
      Labels.appendDynamicLabels(labels, evaluator.evaluate(dynamicLabel, row, context), clause);
    return node.withResolvedLabels(labels);
  }

  /** {@link #resolve} for every node of a path; the path itself when none has a dynamic label. */
  static PathPattern resolve(final PathPattern path, final Result row, final ExpressionEvaluator evaluator,
      final CommandContext context, final String clause) {
    List<NodePattern> resolved = null;
    final List<NodePattern> nodes = path.getNodes();
    for (int i = 0; i < nodes.size(); i++) {
      final NodePattern node = nodes.get(i);
      final NodePattern replacement = resolve(node, row, evaluator, context, clause);
      if (replacement != node && resolved == null)
        resolved = new ArrayList<>(nodes);
      if (resolved != null)
        resolved.set(i, replacement);
    }
    return resolved == null ? path : path.withNodes(resolved);
  }
}
