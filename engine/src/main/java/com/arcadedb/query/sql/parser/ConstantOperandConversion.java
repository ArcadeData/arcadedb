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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.Type;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The conversion a comparison makes of an operand that is the same string on every record of an execution, a string literal or a
 * string bound to a parameter, to the class of the value it is compared with: made once per execution rather than once per
 * record (issue #12713).
 * <p>
 * {@code ts > '2024-06-01 00:00:00'} handed the operator the literal on every row, and the operator converted it to the class of
 * {@code ts} with {@link Type#convertOrNull(Database, Object, Class)}: a datetime parse per record, which took 70% of the time of
 * a scan. A memo keeps the last two conversions, each keyed by the string, the target class and the database, and answers them
 * while the rows ask for one of them. Any other request is converted as before and replaces the older one, so a memo makes a
 * conversion cheaper and never changes its answer. Two entries rather than one so a property that holds a DATE on some records
 * and a DATETIME on others (a schemaless property) does not convert again on every record that alternates.
 * <p>
 * Only a string is remembered: it is immutable, so its text is its value, and parsing it is the conversion that costs. A memo
 * lives in the context of one execution ({@link CommandContext#getCachedValue(String)}), never on the node of a cached plan, so a
 * database setting the conversion reads (the datetime format, the zone) changed between two runs reaches the second one. A
 * parallel scan gives each of its workers a context, and so a memo, of its own; each conversion is an immutable entry in a
 * volatile field all the same, so a memo reached from two threads stays consistent: a racing reader may miss, never mismatch.
 * <p>
 * The converted value is shared by the rows of the execution. The comparisons only read it, a {@code java.util.Date} or a
 * {@code Calendar} included, and it never leaves them: a consumer that mutated it would corrupt the comparison of every row after.
 */
public final class ConstantOperandConversion {
  private static final AtomicLong KEY_SEQUENCE = new AtomicLong();

  /** {@link #literalState} of an operand not asked yet. */
  static final byte UNKNOWN      = 0;
  /** {@link #literalState} of a literal or a parameter: the same value on every record of an execution. */
  static final byte CONSTANT     = 1;
  /** {@link #literalState} of any other operand. */
  static final byte NOT_CONSTANT = 2;

  private record Converted(String value, Class<?> targetClass, Database database, Object converted) {
    boolean matches(final String value, final Class<?> targetClass, final Database database) {
      return this.targetClass == targetClass && this.database == database && this.value.equals(value);
    }
  }

  private volatile Converted last;
  private volatile Converted previous;
  /**
   * How many conversions this memo made. Surfaced for tests only: a plain counter, not thread-safe, so a memo reached from two
   * threads may under-count.
   */
  private          int       conversions;

  ConstantOperandConversion() {
  }

  /**
   * {@link Type#convertOrNull(Database, Object, Class)}, through {@code memo} when there is one and {@code value} is a string.
   *
   * @param memo the memo of the operand {@code value} stands for, or null to convert as it is
   */
  public static Object convertOrNull(final ConstantOperandConversion memo, final Database database, final Object value,
      final Class<?> targetClass) {
    if (memo == null || !(value instanceof String string))
      return Type.convertOrNull(database, value, targetClass);
    return memo.convertOrNull(database, string, targetClass);
  }

  private Object convertOrNull(final Database database, final String value, final Class<?> targetClass) {
    final Converted entry = last;
    if (entry != null && entry.matches(value, targetClass, database))
      return entry.converted();
    final Converted older = previous;
    if (older != null && older.matches(value, targetClass, database))
      return older.converted();

    final Object converted = Type.convertOrNull(database, value, targetClass);
    previous = entry;
    last = new Converted(value, targetClass, database, converted);
    ++conversions;
    return converted;
  }

  /**
   * Whether comparing {@code value} with {@code operand} converts a string operand: a string compared with a value of another
   * class. The caller also asks whether the operand is the same on every record, which {@link #literalState} answers once per node.
   */
  static boolean isStringToConvert(final Object value, final Object operand) {
    return operand instanceof String && value != null && !(value instanceof String);
  }

  /**
   * {@link #CONSTANT} when {@code expression} is a literal or a parameter, else {@link #NOT_CONSTANT}. It depends only on the shape
   * of the expression (a parameter counts whatever it is bound to), so a node asks it once and keeps the answer rather than walking
   * the expression on every record.
   */
  static byte literalState(final Expression expression) {
    return expression != null && expression.isLiteral(true) ? CONSTANT : NOT_CONSTANT;
  }

  /** The memo the execution {@code context} runs keeps under {@code key}, created at its first use; null without a context. */
  static ConstantOperandConversion of(final CommandContext context, final String key) {
    if (context == null)
      return null;

    ConstantOperandConversion memo = (ConstantOperandConversion) context.getCachedValue(key);
    if (memo == null) {
      memo = new ConstantOperandConversion();
      context.setCachedValue(key, memo);
    }
    return memo;
  }

  /**
   * A key of its own into {@link CommandContext#getCachedValue(String)}, for one operand of one node. A sequence, as the memo of
   * {@link InCondition}: a key derived from the node's content or identity hash could collide with another node's.
   */
  static String newKey() {
    return "$CONV$" + KEY_SEQUENCE.incrementAndGet();
  }

  int conversions() {
    return conversions;
  }
}
