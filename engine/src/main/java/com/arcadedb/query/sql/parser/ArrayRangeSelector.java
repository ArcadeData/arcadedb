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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.lang.reflect.*;
import java.util.*;

public class ArrayRangeSelector extends SimpleNode {
  protected Integer from;
  protected Integer to;
  protected boolean newRange = false;
  protected boolean included = false;

  protected ArrayNumberSelector fromSelector;
  protected ArrayNumberSelector toSelector;

  public ArrayRangeSelector(final int id) {
    super(id);
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    if (from != null) {
      builder.append(from);
    } else {
      fromSelector.toString(params, builder);
    }
    if (newRange) {
      builder.append("..");
      if (included) {
        builder.append('.');
      }
    } else {
      builder.append("-");
    }
    if (to != null) {
      builder.append(to);
    } else {
      toSelector.toString(params, builder);
    }
  }

  public Object execute(final Identifiable currentRecord, final Object result, final CommandContext context) {
    if (result == null) {
      return null;
    }
    if (!MultiValue.isMultiValue(result)) {
      return null;
    }
    Integer lFrom = from;
    if (fromSelector != null) {
      lFrom = fromSelector.getValue(currentRecord, result, context);
    }
    if (lFrom == null) {
      lFrom = 0;
    }
    Integer lTo = to;
    if (toSelector != null) {
      lTo = toSelector.getValue(currentRecord, result, context);
    }
    if (included) {
      lTo++;
    }
    if (lFrom > lTo) {
      return null;
    }
    final Object[] arrayResult = MultiValue.array(result);

    if (arrayResult == null || arrayResult.length == 0) {
      return arrayResult;
    }
    lFrom = Math.max(lFrom, 0);
    if (arrayResult.length < lFrom) {
      return null;
    }
    lFrom = Math.min(lFrom, arrayResult.length - 1);

    lTo = Math.min(lTo, arrayResult.length);

    return Arrays.asList(Arrays.copyOfRange(arrayResult, lFrom, lTo));
  }

  public Object execute(final Result currentRecord, final Object result, final CommandContext context) {
    if (result == null) {
      return null;
    }
    if (!MultiValue.isMultiValue(result)) {
      return null;
    }
    Integer lFrom = from;
    if (fromSelector != null) {
      lFrom = fromSelector.getValue(currentRecord, result, context);
    }
    if (lFrom == null) {
      lFrom = 0;
    }
    Integer lTo = to;
    if (toSelector != null) {
      lTo = toSelector.getValue(currentRecord, result, context);
    }
    if (included) {
      lTo++;
    }
    if (lFrom > lTo) {
      return null;
    }
    final Object[] arrayResult = MultiValue.array(result);

    if (arrayResult == null || arrayResult.length == 0) {
      return arrayResult;
    }
    lFrom = Math.max(lFrom, 0);
    if (arrayResult.length < lFrom)
      return null;

    lFrom = Math.min(lFrom, arrayResult.length - 1);
    lTo = Math.min(lTo, arrayResult.length);

    return Arrays.asList(Arrays.copyOfRange(arrayResult, lFrom, lTo));
  }

  public ArrayRangeSelector copy() {
    final ArrayRangeSelector result = new ArrayRangeSelector(-1);
    result.from = from;
    result.to = to;
    result.newRange = newRange;
    result.included = included;
    result.fromSelector = fromSelector == null ? null : fromSelector.copy();
    result.toSelector = toSelector == null ? null : toSelector.copy();
    return result;
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    if (fromSelector != null) {
      fromSelector.extractSubQueries(collector);
    }
    if (toSelector != null) {
      toSelector.extractSubQueries(collector);
    }
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    return new SimpleNode[] { fromSelector, toSelector };
  }

  public void setValue(final Object target, final Object value, final CommandContext context) {
    if (target == null) {
      return;
    }
    if (target.getClass().isArray()) {
      setArrayValue(target, value, context);
    } else if (target instanceof List list) {
      setValue(list, value, context);
    } else if (MultiValue.isMultiValue(value)) {
      //TODO
    }
    //TODO

  }

  public void setValue(final List target, final Object value, final CommandContext context) {
    final int from = this.from == null ? 0 : this.from;
    int to = target.size() - 1;
    if (this.to != null) {
      to = this.to;
      if (!included) {
        to--;
      }
    }
    if (from > to) {
      target.clear();
      return;
    }
    for (int i = 0; i <= to; i++) {
      if (i < from && target.size() - 1 < i) {
        target.set(i, null);
      } else if (i >= from) {
        target.set(i, value);
      }
      //else leave untouched the existing element
    }
  }

  public void setValue(final Set target, final Object value, final CommandContext context) {
    final Set result = new LinkedHashSet<>();
    final int from = this.from == null ? 0 : this.from;
    int to = target.size() - 1;
    if (this.to != null) {
      to = this.to;
      if (!included) {
        to--;
      }
    }
    if (from > to) {
      target.clear();
      return;
    }
    final Iterator targetIterator = target.iterator();
    for (int i = 0; i <= to; i++) {
      Object next = null;
      if (targetIterator.hasNext()) {
        next = targetIterator.next();
      }
      if (i < from && target.size() - 1 < i) {
        result.add(null);
      } else if (i >= from) {
        result.add(value);
      } else {
        result.add(next);
      }
      target.clear();
      target.addAll(result);
    }
  }

  public void setValue(final Map target, final Object value, final CommandContext context) {
    final int from = this.from == null ? 0 : this.from;
    int to = this.to;
    if (!included) {
      to--;
    }
    if (from > to) {
      target.clear();
      return;
    }
    for (int i = from; i <= to; i++) {
      target.put(i, value);
    }
  }

  private void setArrayValue(final Object target, final Object value, final CommandContext context) {

    final int from = this.from == null ? 0 : this.from;
    int to = Array.getLength(target) - 1;
    if (this.to != null) {
      to = this.to;
      if (!included) {
        to--;
      }
    }
    if (from > to || from >= Array.getLength(target)) {
      return;
    }
    to = Math.min(to, Array.getLength(target) - 1);
    for (int i = from; i <= to; i++) {
      Array.set(target, i, value);//TODO type conversion?
    }
  }

  public void applyRemove(final Object currentValue, final ResultInternal originalRecord, final CommandContext context) {
    if (currentValue == null) {
      return;
    }
    Integer from = this.from;
    if (fromSelector != null) {
      from = fromSelector.getValue(originalRecord, null, context);
    }
    Integer to = this.to;
    if (toSelector != null) {
      to = toSelector.getValue(originalRecord, null, context);
    }
    if (from == null || to == null) {
      throw new CommandExecutionException("Invalid range expression: " + this + " one of the elements is null");
    }
    if (included) {
      to++;
    }
    if (from < 0) {
      from = 0;
    }
    if (from >= to) {
      return;
    }
    final int range = to - from;
    if (currentValue instanceof List list) {
      for (int i = 0; i < range; i++) {
        if (list.size() > from) {
          list.remove(from);
        } else {
          break;
        }
      }
    } else if (currentValue instanceof Set set) {
      final Iterator iter = set.iterator();
      int count = 0;
      while (iter.hasNext()) {
        iter.next();
        if (count >= from) {
          if (count < to) {
            iter.remove();
          } else {
            break;
          }
        }
        count++;
      }
    } else {
      throw new CommandExecutionException("Trying to remove elements from " + currentValue + " (" + currentValue.getClass().getSimpleName() + ")");
    }
  }
}
/* JavaCC - OriginalChecksum=594a372e31fcbcd3ed962c2260e76468 (do not edit this line) */
