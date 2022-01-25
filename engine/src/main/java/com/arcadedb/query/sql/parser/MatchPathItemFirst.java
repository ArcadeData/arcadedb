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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class MatchPathItemFirst extends MatchPathItem {
  protected          FunctionCall function;
  protected volatile MethodCall   methodWrapper;

  public MatchPathItemFirst(int id) {
    super(id);
  }

  public MatchPathItemFirst(final SqlParser p, final int id) {
    super(p, id);
  }

  public boolean isBidirectional() {
    return false;
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    function.toString(params, builder);
    if (filter != null)
      filter.toString(params, builder);

  }

  protected Iterable<Identifiable> traversePatternEdge(final MatchStatement.MatchContext matchContext, final Identifiable startingPoint,
      final CommandContext iCommandContext) {
    final Object qR = this.function.execute(startingPoint, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((Identifiable) qR);
  }

  @Override
  public MatchPathItem copy() {
    final MatchPathItemFirst result = (MatchPathItemFirst) super.copy();
    result.function = function == null ? null : function.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (!super.equals(o))
      return false;

    final MatchPathItemFirst that = (MatchPathItemFirst) o;
    return Objects.equals(function, that.function);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (function != null ? function.hashCode() : 0);
    return result;
  }

  public FunctionCall getFunction() {
    return function;
  }

  public void setFunction(final FunctionCall function) {
    this.function = function;
  }

  @Override
  public MethodCall getMethod() {
    if (methodWrapper == null) {
      synchronized (this) {
        if (methodWrapper == null) {
          final MethodCall m = new MethodCall(-1);
          m.params = function.params;
          m.methodName = function.name;
          methodWrapper = m;
        }
      }
    }
    return methodWrapper;
  }
}
