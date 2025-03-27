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
import com.arcadedb.query.sql.executor.IndexSearchInfo;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Created by luigidellaquila on 07/11/14.
 */
public abstract class BooleanExpression extends SimpleNode {

  public static final BooleanExpression TRUE = new BooleanExpression(0) {
    @Override
    public Boolean evaluate(final Identifiable currentRecord, final CommandContext context) {
      return true;
    }

    @Override
    public Boolean evaluate(final Result currentRecord, final CommandContext context) {
      return true;
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

    public void toString(final Map<String, Object> params, final StringBuilder builder) {
      builder.append("true");
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public void extractSubQueries(final SubQueryCollector collector) {
      // NO ACTIONS
    }

    @Override
    public boolean refersToParent() {
      return false;
    }
  };

  public static final BooleanExpression FALSE = new BooleanExpression(0) {
    @Override
    public Boolean evaluate(final Identifiable currentRecord, final CommandContext context) {
      return false;
    }

    @Override
    public Boolean evaluate(final Result currentRecord, final CommandContext context) {
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

    public void toString(final Map<String, Object> params, final StringBuilder builder) {
      builder.append("false");
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public void extractSubQueries(final SubQueryCollector collector) {
      // NO ACTIONS
    }

    @Override
    public boolean refersToParent() {
      return false;
    }
  };

  public BooleanExpression(final int id) {
    super(id);
  }

  public abstract Boolean evaluate(final Identifiable currentRecord, final CommandContext context);

  public abstract Boolean evaluate(final Result currentRecord, final CommandContext context);

  public List<BinaryCondition> getIndexedFunctionConditions(final DocumentType iSchemaClass, final CommandContext context) {
    return null;
  }

  public List<AndBlock> flatten() {
    return List.of(encapsulateInAndBlock(this));
  }

  protected AndBlock encapsulateInAndBlock(final BooleanExpression item) {
    if (item instanceof AndBlock block)
      return block;

    final AndBlock result = new AndBlock(-1);
    result.subBlocks.add(item);
    return result;
  }

  public abstract BooleanExpression copy();

  public boolean isEmpty() {
    return false;
  }

  public abstract void extractSubQueries(final SubQueryCollector collector);

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

  public boolean createRangeWith(final BooleanExpression match) {
    return false;
  }

  public BooleanExpression rewriteIndexChainsAsSubqueries(CommandContext ctx, DocumentType clazz) {
    return this;
  }

  /**
   * returns true only if the expression does not need any further evaluation (eg. "true") and
   * always evaluates to true. It is supposed to be used as and optimization, and is allowed to
   * return false negatives
   *
   * @return
   */
  public boolean isAlwaysTrue() {
    return false;
  }

  public boolean isIndexAware(final IndexSearchInfo info) {
    return false;
  }

  public Expression resolveKeyFrom(BinaryCondition additional) {
    throw new UnsupportedOperationException("Cannot execute index query with " + this);
  }

  public Expression resolveKeyTo(BinaryCondition additional) {
    throw new UnsupportedOperationException("Cannot execute index query with " + this);
  }

  public boolean isKeyFromIncluded(final BinaryCondition additional) {
    throw new UnsupportedOperationException("Cannot execute index query with " + this);
  }

  public boolean isKeyToIncluded(final BinaryCondition additional) {
    throw new UnsupportedOperationException("Cannot execute index query with " + this);
  }
}
