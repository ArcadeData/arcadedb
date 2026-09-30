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
package com.arcadedb.query.opencypher;

import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The expressions a parallel label scan evaluates on several workers at once share one instance (issue #8725), so every
 * instance field of them must be final or volatile. A cache added to one of them later fails here instead of racing
 * under a parallel scan.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParallelSafeExpressionsStateTest {
  @Test
  void theWhitelistedExpressionsHoldNoUnsafeMutableState() {
    final List<String> unsafe = new ArrayList<>();
    for (final Class<?> type : new Class<?>[] { ArithmeticExpression.class, BooleanCoercionExpression.class,
        BooleanWrapperExpression.class, ComparisonExpression.class, ComparisonExpressionWrapper.class, InExpression.class,
        IsNullExpression.class, ListExpression.class, LiteralExpression.class, LogicalExpression.class,
        ParameterExpression.class, PropertyAccessExpression.class, StringMatchExpression.class,
        TernaryLogicalExpression.class, VariableExpression.class })
      for (final Field field : type.getDeclaredFields()) {
        final int modifiers = field.getModifiers();
        if (!Modifier.isStatic(modifiers) && !Modifier.isFinal(modifiers) && !Modifier.isVolatile(modifiers))
          unsafe.add(type.getSimpleName() + "." + field.getName());
      }
    assertThat(unsafe).as("instance fields that are neither final nor volatile").isEmpty();
  }
}
