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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

/**
 * <p>Reads an upstream result set and returns a new result set that contains copies of the original OResult instances
 * </p>
 * <p>This is mainly used from statements that need to copy of the original data before modifying it,
 * eg. UPDATE ... RETURN BEFORE</p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class CopyRecordContentBeforeUpdateStep extends AbstractExecutionStep {

  public CopyRecordContentBeforeUpdateStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet lastFetched = getPrev().syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return lastFetched.hasNext();
      }

      @Override
      public Result next() {
        final Result result = lastFetched.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {

          if (result instanceof UpdatableResult updatableResult) {
            final ResultInternal prevValue = new ResultInternal(context.getDatabase());
            final Record rec = result.getElement().get().getRecord();
            prevValue.setProperty(RID_PROPERTY, rec.getIdentity());
            if (rec instanceof Document document)
              prevValue.setProperty(TYPE_PROPERTY, document.getTypeName());

            for (final String propName : result.getPropertyNames())
              prevValue.setProperty(propName, deepCopyMultiValue(result.getProperty(propName)));

            updatableResult.previousValue = prevValue;
          } else {
            throw new CommandExecutionException("Cannot fetch previous value: " + result);
          }
          return result;
        } finally {
          if (context.isProfiling()) {
            cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void close() {
        lastFetched.close();
      }
    };
  }

  /**
   * Returns a deep copy of {@code value} when it is a {@link List}/{@link Set}/{@link Map}, an embedded
   * {@link Document}/{@link com.arcadedb.database.EmbeddedDocument}, or any nesting of them, so a later in-place
   * mutation of the live property (e.g. {@code REMOVE coll = val}, {@code REMOVE map[k]}, {@code REMOVE emb.field})
   * cannot leak into this BEFORE snapshot (issues #6456 and #6517). Scalars, RIDs, and arrays are returned unchanged:
   * {@code MultiValue.remove()} already replaces arrays with a fresh copy rather than mutating them, so they never
   * share state with the live value.
   */
  private static Object deepCopyMultiValue(final Object value) {
    if (value instanceof Map<?, ?> map) {
      final Map<Object, Object> copy = new LinkedHashMap<>(map.size());
      for (final Map.Entry<?, ?> entry : map.entrySet())
        copy.put(entry.getKey(), deepCopyMultiValue(entry.getValue()));
      return copy;
    }
    if (value instanceof Set<?> set) {
      final Set<Object> copy = new LinkedHashSet<>(set.size());
      for (final Object o : set)
        copy.add(deepCopyMultiValue(o));
      return copy;
    }
    if (value instanceof List<?> list) {
      final List<Object> copy = new ArrayList<>(list.size());
      for (final Object o : list)
        copy.add(deepCopyMultiValue(o));
      return copy;
    }
    if (value instanceof Document document) {
      // Issue #6517: an embedded Document is shared (not copied) by the BEFORE snapshot, and REMOVE emb.field
      // mutates it in place via MutableDocument.modify(). Snapshot via toMap() + recursive deep copy so the
      // BEFORE image keeps the pre-removal state.
      final Map<String, Object> map = document.toMap(false);
      final Map<String, Object> copy = new LinkedHashMap<>(map.size());
      for (final Map.Entry<String, Object> entry : map.entrySet())
        copy.put(entry.getKey(), deepCopyMultiValue(entry.getValue()));
      return copy;
    }
    return value;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY RECORD CONTENT BEFORE UPDATE");
    if (context.isProfiling()) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }

}
