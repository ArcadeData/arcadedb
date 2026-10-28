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
package com.arcadedb.function.sql.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLFunctionAbstract;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Returns different values based on the condition. If it's true the first value is returned, otherwise the second one.
 * <br>
 * Syntax: <blockquote>
 * <p>
 * {@literal if(&lt;field|value|expression&gt;, &lt;return_value_if_true&gt; [,&lt;return_value_if_false&gt;])}
 *
 * </blockquote>
 * <br>
 * <br>
 * Examples: <blockquote>
 *
 * <pre>
 * SELECT <b>if(rich, 'rich', 'poor')</b> FROM ...
 * <br>
 * SELECT <b>if( eval( 'salary &gt; 1000000' ), 'rich', 'poor')</b> FROM ...
 * </pre>
 *
 * </blockquote>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */

public class SQLFunctionIf extends SQLFunctionAbstract {

  public static final String NAME = "if";

  public SQLFunctionIf() {
    super(NAME);
  }

  /**
   * Two, as {@link #getSyntax()} has always said: the false branch is optional.
   */
  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    final boolean result;

    final Object condition = params[0];
    switch (condition) {
    case Boolean boolean1 -> result = boolean1;
    case String s -> result = Boolean.parseBoolean(s);
    case Number number -> result = isPositive(number);
    case null, default -> {
      return null;
    }
    }

    // AN OMITTED FALSE BRANCH IS A null, WHICH IS WHAT THE DOCUMENTED TWO-ARGUMENT FORM MEANS. THE BODY USED TO READ
    // params[2] UNCONDITIONALLY AND HAND THE RESULTING ArrayIndexOutOfBoundsException TO A catch (Exception) THAT
    // LOGGED IT AT SEVERE AND ANSWERED null: ONE FULL STACK TRACE PER EVALUATED ROW (issue #6826). THAT catch IS GONE
    // WITH THE OVER-READ - A GENUINE ERROR RAISED WHILE PRODUCING THE CHOSEN BRANCH IS THE CALLER'S TO SEE - AND THE
    // WRONG ARGUMENT COUNTS IT ALSO HID ARE NOW A CLEAN 400 FROM checkArity()
    return result ? params[1] : params.length > 2 ? params[2] : null;
  }

  /**
   * The sign of the condition, read without narrowing it first: {@code intValue()} read {@code 0.5} as 0 and
   * {@code 3000000000} as a negative int, so both took the false branch (issue #10699). NaN is not positive.
   */
  private static boolean isPositive(final Number number) {
    return switch (number) {
      case BigDecimal bigDecimal -> bigDecimal.signum() > 0;
      case BigInteger bigInteger -> bigInteger.signum() > 0;
      case Integer i -> i > 0;
      case Long l -> l > 0;
      case Short s -> s > 0;
      case Byte b -> b > 0;
      default -> number.doubleValue() > 0;
    };
  }

  @Override
  public String getSyntax() {
    return "if(<field|value|expression>, <return_value_if_true> [,<return_value_if_false>])";
  }

  /**
   * A pure selection among its arguments (issue #6190).
   */
  @Override
  public boolean isDeterministic() {
    return true;
  }
}
