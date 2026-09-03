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
package com.arcadedb.query.sql.method;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.utility.NumberUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Johann Sorel (Geomatys)
 */
public abstract class AbstractSQLMethod implements SQLMethod {
  private final String name;
  private final int    minParams;
  private final int    maxParams;

  public AbstractSQLMethod(final String name) {
    this(name, 0);
  }

  public AbstractSQLMethod(final String name, final int nbparams) {
    this(name, nbparams, nbparams);
  }

  public AbstractSQLMethod(final String name, final int minParams, final int maxParams) {
    this.name = name;
    this.minParams = minParams;
    this.maxParams = maxParams;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getMinParams() {
    return minParams;
  }

  @Override
  public int getMaxParams() {
    return maxParams;
  }

  @Override
  public String getSyntax() {
    final StringBuilder sb = new StringBuilder("<field>.");
    sb.append(getName());
    sb.append('(');
    for (int i = 0; i < minParams; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append("param");
      sb.append(i + 1);
    }
    if (minParams != maxParams) {
      if (maxParams == -1) {
        // UNBOUNDED/VARIADIC: MIRRORS FunctionArity.describe()'S "AT LEAST N" PHRASING FOR THE EQUIVALENT CASE
        if (minParams == 0) {
          sb.append("[param1[, param2]*]");
        } else {
          sb.append("[, param");
          sb.append(minParams + 1);
          sb.append("]*");
        }
      } else {
        sb.append('[');
        for (int i = minParams; i < maxParams; i++) {
          if (i != 0) {
            sb.append(", ");
          }
          sb.append("param");
          sb.append(i + 1);
        }
        sb.append(']');
      }
    }
    sb.append(')');

    return sb.toString();
  }

  /**
   * Reads a character-index argument (a from/to position, a length, a character count) as an int.
   * <p>
   * {@code Integer.parseInt(param.toString())} - what every one of these methods used to do - rejects a decimal
   * literal, so the perfectly ordinary {@code "abcdef".substring(2.5)} answered a NumberFormatException (HTTP 500)
   * instead of an index, and the #5885 clamps could never run because the parse threw first (issue #6389). A number
   * is truncated toward zero and saturated into the int range; a string is parsed the same way; anything else is a
   * typed argument error.
   *
   * @param param        the argument value
   * @param argumentName the argument's name, for the error message
   *
   * @return the argument as an int
   *
   * @throws IllegalArgumentException if the value cannot be read as a number
   */
  protected int indexArgument(final Object param, final String argumentName) {
    final Integer index = NumberUtils.saturateToIntOrNull(param);
    if (index != null)
      return index;

    throw new IllegalArgumentException(
        getName() + "() requires a numeric <" + argumentName + ">, but received " + NumberUtils.describeRejectedNumber(
            param));
  }

  /**
   * Reads the receiver of a numeric conversion method ({@code asInteger()}, {@code asLong()}, ...) as the text to
   * parse, answering {@code null} when there is nothing to parse: a {@code null} value, an empty string, or a string
   * of nothing but whitespace.
   * <p>
   * The seven conversion methods each did their own triage and only {@code asInteger()} had a blank guard at all, so
   * {@code ''.asInteger()} answered {@code null} while {@code ''.asLong()} threw a NumberFormatException and failed
   * the whole query - and an empty string is the ordinary representation of a blank field in imported CSV/JSON, so a
   * cast that worked at one width broke at every other one. {@code asInteger()}'s guard also tested the untrimmed
   * string and parsed the trimmed one, which let {@code ' '.asInteger()} through to {@code Integer.valueOf("")}
   * (issue #6825). Trimming before the blank test is what closes that gap, and having one helper is what keeps the
   * next hardening from reaching only one of the seven - the same move {@code SQLAggregatedFunction} made for the
   * aggregates in #6390.
   *
   * @param value the value the method was invoked on
   *
   * @return the trimmed text to parse, or {@code null} when the value is null or blank
   */
  protected String numericTextOrNull(final Object value) {
    if (value == null)
      return null;
    final String text = value.toString().trim();
    return text.isEmpty() ? null : text;
  }

  /**
   * Reads the receiver of a collection method ({@code join()}, {@code sort()}, {@code transform()}, ...) as a
   * {@link List}, answering {@code null} when the value is not a collection at all: {@code null}, a scalar, or a map.
   * <p>
   * Every collection method used to type-test its receiver for {@code List} (or {@code Collection}) on its own and
   * fall through to {@code toString()} or to an identity return on anything else. A {@code String[]} - what
   * {@code split()} produced, and what a JSON array parameter still arrives as - therefore met a different failure in
   * every method: {@code join()} and {@code asString()} leaked the array's identity {@code toString()}
   * ({@code "[Ljava.lang.String;@7a8fa663"}) into the result set, {@code sort()} returned it unsorted with no error,
   * {@code asList()} wrapped the whole array as a single element (issue #7027). One helper is what keeps the next
   * hardening from reaching only one of them, the same move {@link #numericTextOrNull} made for the conversion
   * methods in #6825.
   * <p>
   * A {@code List} is answered as is, so the read-only methods pay nothing for it; an {@code Object[]} is answered as
   * a fixed-size view of the array; every other collection, primitive array, iterable or iterator is materialised
   * into a new list. Callers that mutate the result must copy it first.
   *
   * @param value the value the method was invoked on
   *
   * @return the receiver as a list, or {@code null} when it is not a collection
   */
  protected static List<Object> listReceiverOrNull(final Object value) {
    // A BARE Iterator IS NOT A MultiValue.isMultiValue() (THAT TEST COVERS Iterable, NOT Iterator), SO IT IS ADMITTED
    // EXPLICITLY OR THE MATERIALISATION BELOW WOULD NEVER SEE ONE
    if (value == null || value instanceof Map || !(MultiValue.isMultiValue(value) || value instanceof Iterator))
      return null;

    final List<Object> list = MultiValue.getMultiValueAsList(value);
    if (list != null)
      return list;

    // AN ITERABLE, AN ITERATOR OR A RESULT SET: MATERIALISE IT ONCE
    final List<Object> materialised = new ArrayList<>();
    for (final Iterator<?> iterator = MultiValue.getMultiValueIterator(value); iterator.hasNext(); )
      materialised.add(iterator.next());
    return materialised;
  }

  protected Object getParameterValue(final Identifiable iRecord, final String iValue) {
    if (iValue == null) {
      return null;
    }

    if (iValue.charAt(0) == '\'' || iValue.charAt(0) == '"') {
      // GET THE VALUE AS STRING
      return iValue.substring(1, iValue.length() - 1);
    }

    if (iRecord == null) {
      return null;
    }
    // SEARCH FOR FIELD
    return iRecord.asDocument().get(iValue);
  }

  @Override
  public int compareTo(final SQLMethod o) {
    return this.getName().compareTo(o.getName());
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean evaluateParameters() {
    return true;
  }
}
