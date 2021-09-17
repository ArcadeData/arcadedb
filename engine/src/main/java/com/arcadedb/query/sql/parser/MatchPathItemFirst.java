/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Collections;
import java.util.Map;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class MatchPathItemFirst extends MatchPathItem {
  protected FunctionCall function;

  protected volatile MethodCall methodWrapper;

  public MatchPathItemFirst(int id) {
    super(id);
  }

  public MatchPathItemFirst(SqlParser p, int id) {
    super(p, id);
  }

  public boolean isBidirectional() {
    return false;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {

    function.toString(params, builder);
    if (filter != null) {
      filter.toString(params, builder);
    }
  }

  protected Iterable<Identifiable> traversePatternEdge(MatchStatement.MatchContext matchContext, Identifiable startingPoint, CommandContext iCommandContext) {
    Object qR = this.function.execute(startingPoint, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((Identifiable) qR);
  }

  @Override
  public MatchPathItem copy() {
    MatchPathItemFirst result = (MatchPathItemFirst) super.copy();
    result.function = function == null ? null : function.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    MatchPathItemFirst that = (MatchPathItemFirst) o;

    return function != null ? function.equals(that.function) : that.function == null;
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

  public void setFunction(FunctionCall function) {
    this.function = function;
  }

  @Override
  public MethodCall getMethod() {
    if (methodWrapper == null) {
      synchronized (this) {
        if (methodWrapper == null) {
          methodWrapper = new MethodCall(-1);
          methodWrapper.params = function.params;
          methodWrapper.methodName = function.name;
        }
      }
    }
    return methodWrapper;
  }
}
