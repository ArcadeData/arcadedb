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

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Created by luigidellaquila on 07/11/14.
 */
public abstract class BooleanExpression extends SimpleNode {

  public static final BooleanExpression TRUE = new BooleanExpression(0) {
    @Override
    public boolean evaluate(Identifiable currentRecord, CommandContext ctx) {
      return true;
    }

    @Override
    public boolean evaluate(Result currentRecord, CommandContext ctx) {
      return true;
    }

    @Override
    protected boolean supportsBasicCalculation() {
      return true;
    }

    @Override
    protected int getNumberOfExternalCalculations() {
      return 0;
    }

    @Override
    protected List<Object> getExternalCalculationConditions() {
      return Collections.EMPTY_LIST;
    }

    @Override
    public boolean needsAliases(Set<String> aliases) {
      return false;
    }

    @Override
    public BooleanExpression copy() {
      return TRUE;

    }

    @Override
    public List<String> getMatchPatternInvolvedAliases() {
      return null;
    }

    @Override
    public boolean isCacheable() {
      return true;
    }

    @Override
    public String toString() {
      return "true";
    }

    public void toString(Map<String, Object> params, StringBuilder builder) {
      builder.append("true");
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public void extractSubQueries(SubQueryCollector collector) {
    }

    @Override
    public boolean refersToParent() {
      return false;
    }
  };

  public static final BooleanExpression FALSE = new BooleanExpression(0) {
    @Override
    public boolean evaluate(Identifiable currentRecord, CommandContext ctx) {
      return false;
    }

    @Override
    public boolean evaluate(Result currentRecord, CommandContext ctx) {
      return false;
    }

    @Override
    protected boolean supportsBasicCalculation() {
      return true;
    }

    @Override
    protected int getNumberOfExternalCalculations() {
      return 0;
    }

    @Override
    protected List<Object> getExternalCalculationConditions() {
      return Collections.EMPTY_LIST;
    }

    @Override
    public boolean needsAliases(Set<String> aliases) {
      return false;
    }

    @Override
    public BooleanExpression copy() {
      return FALSE;
    }

    @Override
    public List<String> getMatchPatternInvolvedAliases() {
      return null;
    }

    @Override
    public boolean isCacheable() {
      return true;
    }

    @Override
    public String toString() {
      return "false";
    }

    public void toString(Map<String, Object> params, StringBuilder builder) {
      builder.append("false");
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public void extractSubQueries(SubQueryCollector collector) {

    }

    @Override
    public boolean refersToParent() {
      return false;
    }
  };

  public BooleanExpression(int id) {
    super(id);
  }

  public BooleanExpression(SqlParser p, int id) {
    super(p, id);
  }

  public abstract boolean evaluate(Identifiable currentRecord, CommandContext ctx);

  public abstract boolean evaluate(Result currentRecord, CommandContext ctx);

  /**
   * @return true if this expression can be calculated in plain Java, false otherwise
   */
  protected abstract boolean supportsBasicCalculation();

  /**
   * @return the number of sub-expressions that have to be calculated using an external engine
   */
  protected abstract int getNumberOfExternalCalculations();

  /**
   * @return the sub-expressions that have to be calculated using an external engine
   */
  protected abstract List<Object> getExternalCalculationConditions();

  public List<BinaryCondition> getIndexedFunctionConditions(DocumentType iSchemaClass, Database database) {
    return null;
  }

  public List<AndBlock> flatten() {

    return Collections.singletonList(encapsulateInAndBlock(this));
  }

  protected AndBlock encapsulateInAndBlock(BooleanExpression item) {
    if (item instanceof AndBlock) {
      return (AndBlock) item;
    }
    AndBlock result = new AndBlock(-1);
    result.subBlocks.add(item);
    return result;
  }

  public abstract boolean needsAliases(Set<String> aliases);

  public abstract BooleanExpression copy();

  public boolean isEmpty() {
    return false;
  }

  public abstract void extractSubQueries(SubQueryCollector collector);

  public abstract boolean refersToParent();

  /**
   * returns the equivalent of current condition as an UPDATE expression with the same syntax, if possible.
   * <p>
   * Eg. name = 3 can be considered a condition or an assignment. This method transforms the condition in an assignment.
   * This is used mainly for UPSERT operations.
   *
   * @return the equivalent of current condition as an UPDATE expression with the same syntax, if possible.
   */
  public Optional<UpdateItem> transformToUpdateItem() {
    return Optional.empty();
  }

  public abstract List<String> getMatchPatternInvolvedAliases();

  public static BooleanExpression deserializeFromOResult(Result doc) {
    try {
      BooleanExpression result = (BooleanExpression) Class.forName(doc.getProperty("__class")).getConstructor(Integer.class).newInstance(-1);
      result.deserialize(doc);
    } catch (Exception e) {
      throw new CommandExecutionException("", e);
    }
    return null;
  }

  public Result serialize() {
    ResultInternal result = new ResultInternal();
    result.setProperty("__class", getClass().getName());
    return result;
  }

  public void deserialize(Result fromResult) {
    throw new UnsupportedOperationException();
  }

  public abstract boolean isCacheable();
}
