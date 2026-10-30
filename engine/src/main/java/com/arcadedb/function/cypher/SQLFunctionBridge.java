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
package com.arcadedb.function.cypher;

import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.function.sql.vector.SQLFunctionVectorDequantizeInt8;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.SQLFunction;

/**
 * Bridge from Cypher function to SQL function.
 * For aggregation functions, we configure them to enable proper state accumulation.
 */
public class SQLFunctionBridge implements StatelessFunction, HeapBufferingFunction {
  private final SQLFunction sqlFunction;
  private final String cypherFunctionName;
  private final boolean isAggregation;
  // AN SQL AGGREGATE THAT KEEPS EVERY VALUE (list(), percentile(), mode()...) IS CHARGED THE SAME FROM CYPHER AS FROM SQL
  // (ISSUE #9793). NULL FOR ANY OTHER FUNCTION, WHOSE STATE DOES NOT GROW WITH ITS INPUT
  private final SQLAggregatedFunction heldValuesFunction;
  private OperationHeapLimit limit;
  // TRUE WHEN NO STEP HANDED AN OPERATION: THE CHARGE IS THEN THIS FUNCTION'S TO GIVE BACK
  private boolean ownLimit;
  private static final String DEQUANTIZE_INT8 = SQLFunctionVectorDequantizeInt8.NAME;
  private final boolean isVectorFunction;
  private long held;

  public SQLFunctionBridge(final SQLFunction sqlFunction, final String cypherFunctionName) {
    this.sqlFunction = sqlFunction;
    this.cypherFunctionName = cypherFunctionName;
    // KEYED ON THE SQL FUNCTION'S OWN NAME, NOT THE CYPHER SPELLING: sql.vector.add() REACHES THE SAME FUNCTION. dequantizeInt8() TAKES
    // A QUANTIZED byte[] BY DESIGN, SO IT IS THE ONE VECTOR FUNCTION THAT MUST SEE THE BYTES AS THEY ARE
    final String sqlName = sqlFunction.getName();
    this.isVectorFunction = sqlName != null && sqlName.startsWith("vector.") && !sqlName.equals(DEQUANTIZE_INT8);

    // Configure the function to enable aggregation mode
    // SQL aggregation functions check configuredParameters in aggregateResults()
    // We configure with a dummy parameter since the actual values come through execute() args
    sqlFunction.config(new Object[]{"dummy"});

    // Cache whether this is an aggregation function
    this.isAggregation = sqlFunction.aggregateResults();
    this.heldValuesFunction =
        isAggregation && sqlFunction instanceof SQLAggregatedFunction function && function.holdsEveryValue() ? function : null;
  }

  @Override
  public void setHeapLimit(final OperationHeapLimit owner) {
    if (heldValuesFunction != null) {
      limit = owner.child(cypherFunctionName + "()");
      ownLimit = false;
    }
  }

  @Override
  public String getName() {
    return cypherFunctionName;
  }

  /**
   * The wrapped SQL function's own argument-count contract, passed straight through.
   * <p>
   * Without this the bridge reported {@link com.arcadedb.function.StatelessFunction}'s "anything goes" defaults, so the
   * seven Cypher functions that reach a SQL function this way - {@code count}, {@code distance}, {@code stdev},
   * {@code stdev_pop}, {@code stdev_samp}, {@code stdevp} and {@code sum} - were the one group
   * {@code CypherFunctionArityRegistryTest} could not check. That is exactly where the #5484 bug happened:
   * {@code distance()} was declared to the parser as taking exactly two arguments while it had always accepted an
   * optional unit. See issue #5602.
   */
  @Override
  public int getMinArgs() {
    return sqlFunction.getMinArgs();
  }

  @Override
  public int getMaxArgs() {
    return sqlFunction.getMaxArgs();
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    // Held to the same declaration every other executor is, so "each function's runtime guard reads its own bounds"
    // has no exception here either. The bounds are the wrapped SQL function's own, so this can only reject a call
    // that function could not have served anyway; one that declares none (the SQLFunctionAbstract defaults) is
    // unaffected.
    checkArity(args);
    // SQL functions expect (self, currentRecord, currentResult, params, context)
    // For now, we pass nulls for self, currentRecord, currentResult
    final Object result = sqlFunction.execute(null, null, null, isVectorFunction ? widenInt8Vectors(args) : args, context);
    // CHARGED AFTER THE FUNCTION TOOK THE VALUE: WHAT IT HOLDS NOW IS WHAT IT REPORTS, AND A CAP TRIPPED HERE FAILS THE
    // QUERY WITH ONE VALUE OVER IT, AS FunctionAggregationContext DOES
    if (heldValuesFunction != null) {
      if (limit == null) {
        limit = OperationHeapLimit.of(context, cypherFunctionName + "()");
        ownLimit = true;
      }
      held = heldValuesFunction.chargeHeld(limit, held, args.length == 1 ? args[0] : args);
    }
    return result;
  }

  /**
   * A Cypher INT8 vector is a {@code byte[]} of plain integer coordinates (#10917), which the SQL vector functions would read as
   * a quantized one and refuse: it is widened to the {@code float[]} they take.
   */
  private static Object[] widenInt8Vectors(final Object[] args) {
    Object[] widened = args;
    for (int i = 0; i < args.length; i++)
      if (args[i] instanceof byte[]) {
        if (widened == args)
          widened = args.clone();
        widened[i] = VectorUtils.toFloatArrayOfCoordinates(args[i]);
      }
    return widened;
  }

  @Override
  public boolean aggregateResults() {
    return isAggregation;
  }

  @Override
  public Object getAggregatedResult() {
    if (ownLimit) {
      // THE VALUES GO TO THE CALLER, WHICH HOLDS THEM FROM NOW ON
      limit.release();
      limit = null;
      ownLimit = false;
    }
    // THE FUNCTION STARTS OVER FROM NOTHING HELD IF IT IS FED AGAIN
    held = 0L;
    final Object result = sqlFunction.getResult();
    // Cypher's sum() over no value is 0 (Neo4j), while the SQL function answers NULL since issue #9351. An explicit
    // sql.sum() keeps the SQL answer.
    if (result == null && sqlFunction instanceof SQLFunctionSum && !cypherFunctionName.regionMatches(true, 0, "sql.", 0, 4))
      return 0;
    return result;
  }

  @Override
  public boolean canMergePartials() {
    return isAggregation && sqlFunction instanceof SQLAggregatedFunction function && function.canMergePartials();
  }

  @Override
  public void mergePartial(final StatelessFunction other) {
    if (!(sqlFunction instanceof SQLAggregatedFunction function) || !(other instanceof SQLFunctionBridge bridge)
        || !(bridge.sqlFunction instanceof SQLAggregatedFunction otherFunction))
      throw new UnsupportedOperationException("Function '" + cypherFunctionName + "' cannot merge partial aggregations");
    function.mergePartial(otherFunction);
  }
}
