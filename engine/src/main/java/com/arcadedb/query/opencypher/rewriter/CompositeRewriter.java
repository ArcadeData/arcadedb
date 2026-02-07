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
package com.arcadedb.query.opencypher.rewriter;

import com.arcadedb.query.opencypher.ast.Expression;

import java.util.Arrays;
import java.util.List;

/**
 * Composite rewriter that applies multiple rewrite rules in sequence.
 * Each rewriter's output becomes the input to the next rewriter.
 *
 * Example:
 * <pre>
 * final ExpressionRewriter rewriter = new CompositeRewriter(
 *     new ComparisonNormalizer(),
 *     new BooleanSimplifier(),
 *     new ConstantFolder()
 * );
 * final Expression rewritten = rewriter.rewrite(originalExpression);
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CompositeRewriter extends ExpressionRewriter {
  private final List<ExpressionRewriter> rewriters;

  public CompositeRewriter(final ExpressionRewriter... rewriters) {
    this.rewriters = Arrays.asList(rewriters);
  }

  public CompositeRewriter(final List<ExpressionRewriter> rewriters) {
    this.rewriters = rewriters;
  }

  @Override
  public Object rewrite(final Object expression) {
    Object current = expression;
    for (final ExpressionRewriter rewriter : rewriters)
      current = rewriter.rewrite(current);
    return current;
  }
}
