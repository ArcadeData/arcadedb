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
package com.arcadedb.function.temporal;

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.DateTimeException;

/**
 * datetime.fromepoch() function - creates a CypherDateTime from epoch seconds and nanoseconds.
 */
public class DateTimeFromEpochFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "datetime.fromepoch";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args[0] == null || args[1] == null)
      return null;
    // Issue #10850: whole numbers or a type error - longValue() read 1.9 as 1, and a non-number was a ClassCastException
    final long seconds = CypherFunctionHelper.requireWholeNumberArgument(args[0], getName(), "seconds");
    final long nanos = CypherFunctionHelper.requireWholeNumberArgument(args[1], getName(), "nanoseconds");
    try {
      return CypherDateTime.fromEpoch(seconds, nanos);
    } catch (final DateTimeException | ArithmeticException e) {
      // an instant past the java.time range: the caller's value, a client error as a datetime() constructor's (issue #12744)
      throw new ArithmeticErrorException(getName() + "() has no datetime for " + seconds + " seconds and " + nanos + " nanoseconds: " + e.getMessage(), e);
    }
  }
}
