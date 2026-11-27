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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

/**
 * Expression representing a query parameter.
 * Example: $param, $1, $p1
 * Parameters are placeholders that are replaced with actual values at runtime.
 */
public class ParameterExpression implements Expression {
  private final String parameterName;
  private final String text;

  public ParameterExpression(final String parameterName, final String text) {
    this.parameterName = parameterName;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // Look up the parameter value from the context
    if (context != null && context.getInputParameters() != null)
      return asCypherValue(context.getInputParameters().get(parameterName));
    return null;
  }

  /**
   * A temporal a Bolt driver sends is decoded to a {@code java.time} value (BoltStructureMapper). Handed to the expression as such,
   * it is refused by temporal arithmetic and by the {@code date()}, {@code datetime()}, {@code time()} ... constructors and the
   * {@code truncate()} functions, which only know the Cypher temporal classes, although the literal of the same value is accepted
   * (issue #13569). The parameter is therefore the Cypher temporal the literal would be.
   * <p>
   * Only the classes that were refused are wrapped. A {@code LocalDateTime} and a {@code LocalDate} are left as they are: the
   * arithmetic and the constructors take them, and a {@code LocalDateTime} parameter deliberately compares with a zoned datetime by
   * instant, which its Cypher wrapper would not (CypherTemporalTest). An {@code Instant}, {@code OffsetDateTime} or
   * {@code java.util.Date} bound through the Java API keeps the type it is stored as.
   */
  private static Object asCypherValue(final Object value) {
    if (value instanceof ZonedDateTime || value instanceof LocalTime || value instanceof OffsetTime)
      return TemporalUtil.fromCoreJavaType(value);
    return value;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public String getParameterName() {
    return parameterName;
  }
}
