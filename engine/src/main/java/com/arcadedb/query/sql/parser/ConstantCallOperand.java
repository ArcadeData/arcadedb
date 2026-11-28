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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An operand of a condition that is a call over literals or parameters, such as {@code date('2024-06-01 00:00:00', 'yyyy-MM-dd
 * HH:mm:ss')}: evaluated once per execution rather than once per record, when the function answers the same value for every record
 * of the execution with those arguments (issue #13523). Any other operand is evaluated as it was, on every record.
 * <p>
 * {@code ts > date('2024-06-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')} built a formatter and parsed the string on every row, 3.4 times
 * slower than the same instant bound as a parameter. The call cannot be folded at plan time: {@code date()} with no argument reads
 * the clock, so the function is not {@link com.arcadedb.query.sql.executor.SQLFunction#isDeterministic() deterministic}, and a
 * value folded into a cached plan would outlive the database settings it was parsed under. The function is asked instead, with
 * the values of the execution, whether those arguments give one answer for the execution
 * ({@link com.arcadedb.query.sql.executor.SQLFunction#isConstantForArguments(Object[])}).
 * <p>
 * The value lives in the context of one execution ({@link CommandContext#getCachedValue(String)}), as the conversions of
 * {@link ConstantOperandConversion} do, never on the node of a cached plan. It is kept with the parameters and the database it
 * was computed with, and answers only while the rows ask with those same objects: a context that reaches the memo of its parent
 * (a nested execution of the same statement with parameters of its own) computes the value again. A parallel scan gives each of
 * its workers a context, and so a memo, of its own; the entry is immutable in a volatile field all the same.
 * <p>
 * The value is shared by the rows of the execution, which only compare it: a {@code java.util.Date} or a {@code Calendar} is read,
 * never mutated, as the converted operand of {@link ConstantOperandConversion} is.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
final class ConstantCallOperand {
  private static final AtomicLong KEY_SEQUENCE = new AtomicLong();

  /** The operand this stands for, so a node whose operand was replaced builds a new one. */
  private final Expression   operand;
  /** The call when the operand is a call over constants, else null: the operand is evaluated on every record. */
  private final FunctionCall call;
  /** A key of its own into the context's cached values: a sequence, as the memo of {@link InCondition}. */
  private final String       key;

  private ConstantCallOperand(final Expression operand) {
    this.operand = operand;
    this.call = callOverConstants(operand);
    this.key = call != null ? "$CALL$" + KEY_SEQUENCE.incrementAndGet() : null;
  }

  /**
   * The operand of {@code expression}, reusing {@code current} when it was built for it. A benign race when two threads build one
   * each: they are equivalent, and the loser only computes the value of its execution once more.
   */
  static ConstantCallOperand of(final ConstantCallOperand current, final Expression expression) {
    return current != null && current.operand == expression ? current : new ConstantCallOperand(expression);
  }

  Object execute(final Identifiable record, final CommandContext context) {
    if (call == null || context == null)
      return operand.execute(record, context);
    final Memo memo = memo(context);
    final Entry entry = memo.entry;
    if (entry != null && entry.matches(context))
      return entry.constant ? entry.value : operand.execute(record, context);
    return memo.compute(context, operand.execute(record, context));
  }

  Object execute(final Result record, final CommandContext context) {
    if (call == null || context == null)
      return operand.execute(record, context);
    final Memo memo = memo(context);
    final Entry entry = memo.entry;
    if (entry != null && entry.matches(context))
      return entry.constant ? entry.value : operand.execute(record, context);
    return memo.compute(context, operand.execute(record, context));
  }

  /** Whether the operand is a call this may evaluate once per execution, depending on the values of that execution. */
  boolean isCallOverConstants() {
    return call != null;
  }

  /** The memo of the execution {@code context} runs, created at its first use. */
  Memo memo(final CommandContext context) {
    Memo memo = (Memo) context.getCachedValue(key);
    if (memo == null) {
      memo = new Memo(call);
      context.setCachedValue(key, memo);
    }
    return memo;
  }

  private static FunctionCall callOverConstants(final Expression expression) {
    if (expression == null || !(expression.mathExpression instanceof BaseExpression base) || base.modifier != null)
      return null;
    final BaseIdentifier identifier = base.identifier;
    if (identifier == null || identifier.suffix != null || identifier.levelZero == null)
      return null;
    final FunctionCall call = identifier.levelZero.functionCall;
    return call != null && call.isCallOverConstants() ? call : null;
  }

  /**
   * The value of the call, with the parameters and the database it was computed with, or the verdict that those arguments give no
   * single value ({@code constant} false), so the rows of the execution do not ask the function again.
   */
  private record Entry(Map<String, Object> parameters, Database database, boolean constant, Object value) {
    boolean matches(final CommandContext context) {
      return parameters == context.getInputParameters() && database == context.getDatabase();
    }
  }

  static final class Memo {
    private final    FunctionCall call;
    private volatile Entry        entry;
    /**
     * How many values this memo computed. Surfaced for tests only: a plain counter, not thread-safe, so a memo reached from two
     * threads may under-count.
     */
    private          int          computations;

    private Memo(final FunctionCall call) {
      this.call = call;
    }

    private Object compute(final CommandContext context, final Object value) {
      entry = new Entry(context.getInputParameters(), context.getDatabase(), call.isConstantForArguments(context), value);
      ++computations;
      return value;
    }

    int computations() {
      return computations;
    }

    /** Whether the rows of the execution are answered the value this memo computed, rather than evaluating the call each. */
    boolean holdsTheValue() {
      final Entry current = entry;
      return current != null && current.constant;
    }
  }
}
