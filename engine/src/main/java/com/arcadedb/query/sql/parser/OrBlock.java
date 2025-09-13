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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;

import java.util.*;
import java.util.stream.*;

public class OrBlock extends BooleanExpression {
  List<BooleanExpression> subBlocks = new ArrayList<BooleanExpression>();

  public OrBlock(final int id) {
    super(id);
  }

  @Override
  public Boolean evaluate(final Identifiable currentRecord, final CommandContext context) {
    if (getSubBlocks() == null) {
      return true;
    }

    for (final BooleanExpression block : subBlocks) {
      if (block.evaluate(currentRecord, context)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Boolean evaluate(final Result currentRecord, final CommandContext context) {
    if (getSubBlocks() == null)
      return true;

    for (final BooleanExpression block : subBlocks) {
      final Boolean result = block.evaluate(currentRecord, context);
      if (result == null)
        return null;
      else if (result)
        return true;
    }
    return false;
  }

  public Boolean evaluate(final Object currentRecord, final CommandContext context) {
    if (currentRecord instanceof Result result) {
      return evaluate(result, context);
    } else if (currentRecord instanceof Identifiable identifiable) {
      return evaluate(identifiable, context);
    } else if (currentRecord instanceof Map) {
      final ResultInternal result = new ResultInternal((Map<String, Object>) currentRecord);
      return evaluate(result, context);
    }
    return false;
  }

  public List<BooleanExpression> getSubBlocks() {
    return subBlocks;
  }

  public void setSubBlocks(final List<BooleanExpression> subBlocks) {
    this.subBlocks = subBlocks;
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    if (subBlocks == null || subBlocks.size() == 0) {
      return;
    }

    boolean first = true;
    for (BooleanExpression expr : subBlocks) {
      if (!first) {
        builder.append(" OR ");
      }
      expr.toString(params, builder);
      first = false;
    }
  }

  public List<BinaryCondition> getIndexedFunctionConditions(final DocumentType iSchemaClass, final CommandContext context) {
    if (subBlocks == null || subBlocks.size() > 1) {
      return null;
    }
    final List<BinaryCondition> result = new ArrayList<BinaryCondition>();
    for (final BooleanExpression exp : subBlocks) {
      final List<BinaryCondition> sub = exp.getIndexedFunctionConditions(iSchemaClass, context);
      if (sub != null && sub.size() > 0) {
        result.addAll(sub);
      }
    }
    return result.size() == 0 ? null : result;
  }

  public List<AndBlock> flatten() {
    final List<AndBlock> result = new ArrayList<AndBlock>();
    for (final BooleanExpression sub : subBlocks) {
      final List<AndBlock> childFlattened = sub.flatten();
      for (final AndBlock child : childFlattened) {
        result.add(child);
      }
    }
    return result;
  }

  @Override
  public OrBlock copy() {
    final OrBlock result = new OrBlock(-1);
    result.subBlocks = subBlocks.stream().map(x -> x.copy()).collect(Collectors.toList());
    return result;
  }

  @Override
  public boolean isEmpty() {
    if (subBlocks.isEmpty()) {
      return true;
    }
    for (final BooleanExpression block : subBlocks) {
      if (!block.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void extractSubQueries(final SubQueryCollector collector) {
    for (final BooleanExpression block : subBlocks) {
      block.extractSubQueries(collector);
    }
  }

  @Override
  public List<String> getMatchPatternInvolvedAliases() {
    final List<String> result = new ArrayList<String>();
    for (final BooleanExpression exp : subBlocks) {
      final List<String> x = exp.getMatchPatternInvolvedAliases();
      if (x != null) {
        result.addAll(x);
      }
    }
    return result.isEmpty() ? null : result;
  }

  protected Object[] getIdentityElements() {
    return getCacheableElements();
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    return subBlocks.toArray(new BooleanExpression[subBlocks.size()]);
  }

  @Override
  public boolean isAlwaysTrue() {
    if (subBlocks.isEmpty())
      return true;

    for (BooleanExpression exp : subBlocks) {
      if (exp.isAlwaysTrue()) {
        return true;
      }
    }
    return false;
  }
}
/* JavaCC - OriginalChecksum=98d3077303a598705894dbb7bd4e1573 (do not edit this line) */
