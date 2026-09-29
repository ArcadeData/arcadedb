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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * Joins a node pattern onto the rows of the rest of the pattern by seeking an index for each row: {@code MATCH (a:T),
 * (b:T) WHERE b.y = a.x} with an index on {@code T.y} looks up, for every {@code a}, the {@code b} whose {@code y} is
 * {@code a.x}, instead of crossing every {@code a} with every {@code b} (issue #8584). Nothing is buffered: the memory
 * the join holds does not grow with either side.
 * <p>
 * The key columns cover a leading prefix of the index, each from a conjunct {@code b.p = expression}: an expression of
 * the left row, or a literal or a parameter. The rest of the WHERE is still evaluated above the join, and the
 * conjuncts on {@code b} alone before a sought row is merged, so the seek only has to never lose a row the WHERE
 * accepts. An index converts a key to the type of its property, which the Cypher {@code =} does not: a key it could
 * convert into a value the comparison would not call equal only adds rows the WHERE drops, and a key whose equality
 * the conversion cannot follow - a number against a string property, which equals a string spelling a RID, a temporal,
 * a list - reads the whole label for that row, as a product would.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IndexNestedLoopJoin extends AbstractPhysicalOperator {
  private enum KeyUse {SEEK, SCAN, NONE}

  /** 2^53: past it a double no longer tells consecutive longs apart. */
  private static final double MAX_EXACT_DOUBLE_INTEGER = 9_007_199_254_740_992.0;

  private final String            variable;
  private final String            label;
  private final String            indexName;
  /** Every property of the index, in key order. */
  private final List<String>      indexProperties;
  /** The value of each leading key column: {@link EquiJoinKey#evaluateLeft} reads it from the left row. */
  private final EquiJoinKey[]     keys;
  /** The WHERE conjuncts that read the sought node alone, or null. */
  private final BooleanExpression rightFilter;

  public IndexNestedLoopJoin(final PhysicalOperator left, final String variable, final String label, final String indexName,
      final List<String> indexProperties, final EquiJoinKey[] keys, final BooleanExpression rightFilter,
      final double estimatedCost, final long estimatedCardinality) {
    super(left, estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.label = label;
    this.indexName = indexName;
    this.indexProperties = indexProperties;
    this.keys = keys;
    this.rightFilter = rightFilter;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private ResultSet leftResults;
      private boolean   initialized = false;
      private boolean   finished    = false;

      private TypeIndex index;
      private boolean   inheritedIndex;
      private boolean   wholeKey;
      private Type[]    keyTypes;
      private Schema    schema;

      private Result                         currentLeft;
      /** The keys still to seek for the current left row, past the one {@link #cursor} walks. */
      private List<Object[]>                 pendingKeys;
      private IndexCursor                    cursor;
      private Iterator<? extends Identifiable> scan;
      private Result                         pending;

      @Override
      public boolean hasNext() {
        if (pending != null)
          return true;
        if (finished)
          return false;
        if (!initialized)
          initialize();
        return advance();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result rightRow = pending;
        pending = null;

        // The sought node's variable is bound by no other part of the pattern, so it overwrites nothing
        final ResultInternal merged = new ResultInternal();
        for (final String property : currentLeft.getPropertyNames())
          merged.setProperty(property, currentLeft.getProperty(property));
        merged.setProperty(variable, rightRow.getProperty(variable));
        return merged;
      }

      private void initialize() {
        initialized = true;
        schema = context.getDatabase().getSchema();
        final DocumentType type = schema.getTypeOrNull(label);
        // A non-vertex type with the same name matches no node pattern (issue #5194)
        if (!(type instanceof VertexType)) {
          finish();
          return;
        }
        // The very index the planner chose, by name: one dropped and created again with other properties or of another
        // kind since the plan was made (a hash index cannot walk the key prefix a range seek needs) asks for a new plan
        final Index named = schema.existsIndex(indexName) ? schema.getIndexByName(indexName) : null;
        index = named instanceof TypeIndex typeIndex && typeIndex.getPropertyNames().equals(indexProperties) ? typeIndex : null;
        if (index == null)
          throw new CommandExecutionException(
              "Index '" + indexName + "' on type '" + label + "' is no longer available: re-plan the query");
        inheritedIndex = Labels.isInheritedIndex(index, label);
        wholeKey = keys.length == index.getPropertyNames().size();
        keyTypes = index.getKeyTypes();
        leftResults = child.execute(context, nRecords);
      }

      private boolean advance() {
        while (!finished) {
          final Result rightRow = nextRight();
          if (rightRow != null) {
            pending = rightRow;
            return true;
          }

          if (!leftResults.hasNext()) {
            finish();
            break;
          }
          currentLeft = leftResults.next();
          openRight();
        }
        return false;
      }

      // The next row of the node sought for the current left row that passes the conjuncts on the node alone
      private Result nextRight() {
        while (true) {
          guard.check();
          final Identifiable identifiable;
          if (cursor != null && cursor.hasNext())
            identifiable = cursor.next();
          else if (scan != null && scan.hasNext())
            identifiable = scan.next();
          else if (pendingKeys != null && !pendingKeys.isEmpty()) {
            if (cursor != null)
              cursor.close();
            cursor = seek(pendingKeys.removeLast());
            continue;
          } else {
            closeRight();
            return null;
          }

          // A record of another type in the hierarchy is rejected without loading it (issue #7021)
          if (cursor != null && inheritedIndex && !Labels.carriesLabel(schema, identifiable, label))
            continue;

          final ResultInternal row = new ResultInternal();
          row.setProperty(variable, identifiable.asVertex());
          if (rightFilter == null || Boolean.TRUE.equals(rightFilter.evaluateTernary(row, context)))
            return row;
        }
      }

      private void openRight() {
        final Object[] key = new Object[keys.length];
        KeyUse use = KeyUse.SEEK;
        for (int i = 0; i < keys.length; i++) {
          key[i] = keys[i].evaluateLeft(currentLeft, context);
          final KeyUse keyUse = keyUse(key[i], keyTypes[i]);
          if (keyUse == KeyUse.NONE)
            // The WHERE cannot hold for this left row
            return;
          if (keyUse == KeyUse.SCAN)
            use = KeyUse.SCAN;
          else if (key[i] instanceof Float f)
            // Cypher reads a float through its decimal form: the index would widen its bits instead
            key[i] = Type.widenFloat(f);
        }

        if (use == KeyUse.SEEK) {
          try {
            // Cypher calls -0.0 and 0.0 equal, the index orders them apart: a zero is sought under both signs
            final List<Object[]> signedKeys = withBothZeros(key, keyTypes);
            cursor = seek(signedKeys.removeLast());
            pendingKeys = signedKeys;
            return;
          } catch (final IllegalArgumentException e) {
            // A key the index refuses to convert: read the label, where the comparison decides
          }
        }
        @SuppressWarnings("unchecked")
        final Iterator<? extends Identifiable> all = (Iterator<? extends Identifiable>) (Object) context.getDatabase()
            .iterateType(label, true);
        scan = all;
      }

      // A key covering every index property is a single-entry lookup; a prefix is a range of the ordered index
      private IndexCursor seek(final Object[] key) {
        return wholeKey ? index.get(key) : index.range(true, key, true, key, true);
      }

      private void closeRight() {
        pendingKeys = null;
        if (cursor != null) {
          // An index cursor holds its file until closed (issue #5635)
          cursor.close();
          cursor = null;
        }
        scan = null;
      }

      private void finish() {
        finished = true;
        currentLeft = null;
        closeRight();
        if (leftResults != null) {
          leftResults.close();
          leftResults = null;
        }
      }

      @Override
      public void close() {
        pending = null;
        finish();
      }
    };
  }

  /**
   * How a key value can be looked up in an index of the given key type: sought, never equal to what the property
   * holds, or only found by reading every record, when the index conversion cannot follow the Cypher equality.
   */
  private static KeyUse keyUse(final Object value, final Type keyType) {
    if (value == null)
      return KeyUse.NONE;

    switch (keyType) {
    case STRING:
      if (value instanceof String)
        return KeyUse.SEEK;
      if (value instanceof Boolean)
        return KeyUse.NONE;
      // A number equals a string spelling the RID it encodes
      return KeyUse.SCAN;
    case BYTE:
    case SHORT:
    case INTEGER:
    case LONG:
      if (value instanceof Number number)
        return integralKeyUse(number, keyType);
      if (value instanceof String string)
        return RID.is(string) ? KeyUse.SCAN : KeyUse.NONE;
      if (value instanceof Boolean)
        return KeyUse.NONE;
      return KeyUse.SCAN;
    case FLOAT:
    case DOUBLE:
      if (value instanceof Number number)
        return number instanceof Double d && d.isNaN() || number instanceof Float f && f.isNaN() ? KeyUse.NONE : KeyUse.SEEK;
      if (value instanceof String string)
        return RID.is(string) ? KeyUse.SCAN : KeyUse.NONE;
      if (value instanceof Boolean)
        return KeyUse.NONE;
      return KeyUse.SCAN;
    case BOOLEAN:
      if (value instanceof Boolean)
        return KeyUse.SEEK;
      if (value instanceof String || value instanceof Number)
        return KeyUse.NONE;
      return KeyUse.SCAN;
    default:
      return KeyUse.SCAN;
    }
  }

  /**
   * The keys to seek for {@code key}: itself, and for every floating-point zero in it the same key with the other sign,
   * since an index orders -0.0 before 0.0 while the Cypher {@code =} calls them equal. A zero against an integral
   * property is one key already, and is left alone.
   */
  private static List<Object[]> withBothZeros(final Object[] key, final Type[] keyTypes) {
    final List<Object[]> keys = new ArrayList<>(1);
    keys.add(key);
    for (int i = 0; i < key.length; i++) {
      // An integral property holds one zero, which both signs convert to: seeking it twice would find its rows twice
      if (keyTypes[i] != Type.FLOAT && keyTypes[i] != Type.DOUBLE)
        continue;
      final Object value = key[i];
      final Object otherZero;
      if (value instanceof Double d && d == 0.0)
        otherZero = Double.doubleToRawLongBits(d) == 0L ? -0.0d : 0.0d;
      else if (value instanceof Float f && f == 0.0f)
        otherZero = Float.floatToRawIntBits(f) == 0 ? -0.0f : 0.0f;
      else
        continue;
      final int existing = keys.size();
      for (int k = 0; k < existing; k++) {
        final Object[] signed = keys.get(k).clone();
        signed[i] = otherZero;
        keys.add(signed);
      }
    }
    return keys;
  }

  /**
   * A number against an integral property: the property only holds integers of its type's range, so a fraction or a
   * number out of the range equals none of them - and the index would refuse to narrow it. A double past 2^53 stands
   * for several longs the Cypher equality, which compares a long and a double as doubles, calls equal to it - and so does
   * 2^53 itself, which the long 2^53 + 1 rounds to.
   */
  private static KeyUse integralKeyUse(final Number number, final Type keyType) {
    final long value;
    if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte)
      value = number.longValue();
    else if (number instanceof Double || number instanceof Float || number instanceof BigDecimal) {
      final double d = number instanceof Float f ? Type.widenFloat(f) : number.doubleValue();
      if (Double.isNaN(d) || d != Math.rint(d))
        return KeyUse.NONE;
      // 2^53 itself too: the long 2^53 + 1 widens to it
      if (Math.abs(d) >= MAX_EXACT_DOUBLE_INTEGER)
        return KeyUse.SCAN;
      value = (long) d;
    } else
      return KeyUse.SCAN;

    final long min;
    final long max;
    switch (keyType) {
    case BYTE -> {
      min = Byte.MIN_VALUE;
      max = Byte.MAX_VALUE;
    }
    case SHORT -> {
      min = Short.MIN_VALUE;
      max = Short.MAX_VALUE;
    }
    case INTEGER -> {
      min = Integer.MIN_VALUE;
      max = Integer.MAX_VALUE;
    }
    default -> {
      min = Long.MIN_VALUE;
      max = Long.MAX_VALUE;
    }
    }
    return value < min || value > max ? KeyUse.NONE : KeyUse.SEEK;
  }

  /** The index key types a join can seek by: the others go through a conversion the Cypher equality does not make. */
  public static boolean isSeekableKeyType(final Type type) {
    return switch (type) {
      case STRING, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN -> true;
      default -> false;
    };
  }

  @Override
  public String getOperatorType() {
    return "IndexNestedLoopJoin";
  }

  @Override
  public String explain(final int depth) {
    final String indent = getIndent(depth);
    final StringBuilder sb = new StringBuilder();
    sb.append(indent).append("+ IndexNestedLoopJoin(").append(variable).append(":").append(label).append(")");
    sb.append(" [index=").append(indexName);
    for (int i = 0; i < keys.length; i++)
      sb.append(", ").append(indexProperties.get(i)).append("=").append(keys[i].left().getText());
    sb.append("]");
    if (rightFilter != null)
      sb.append(" [filter: ").append(rightFilter.getText()).append("]");
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    if (child != null)
      sb.append(child.explain(depth + 1));
    return sb.toString();
  }

  public String getVariable() {
    return variable;
  }

  public String getLabel() {
    return label;
  }
}
