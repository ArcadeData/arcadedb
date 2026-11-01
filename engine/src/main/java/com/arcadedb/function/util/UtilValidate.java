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
package com.arcadedb.function.util;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Collection;

/**
 * util.validate(predicate, message[, params]) - Throws an exception carrying {@code message} if the predicate is TRUE, as
 * APOC's {@code apoc.util.validate} does: the predicate states the condition that must abort the query, so a guard reads
 * {@code CALL apoc.util.validate(n IS NULL, 'node %s not found', [$id])}. When {@code params} is a non-empty list its values
 * fill the {@code %s} placeholders of {@code message}, and only then is the message formatted, so a plain message can
 * carry a literal {@code %}. Answers {@code true} otherwise.
 * <p>
 * Before issue #11114 it took two arguments and raised when the predicate was FALSE, so every call written for APOC was
 * refused for its arity, and a call that passed it would have aborted exactly the writes the guard was there to let
 * through.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class UtilValidate extends AbstractUtilFunction {
  @Override
  protected String getSimpleName() {
    return "validate";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Throws an exception with the given message, formatted with the optional params, if the predicate is true";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (isTrue(args[0]))
      throw new IllegalArgumentException(message(args[1], args.length > 2 ? args[2] : null));
    return true;
  }

  private static boolean isTrue(final Object predicate) {
    if (predicate == null)
      return false;
    if (predicate instanceof Boolean b)
      return b;
    if (predicate instanceof String str)
      return !str.isEmpty() && !"false".equalsIgnoreCase(str) && !"0".equals(str);
    if (predicate instanceof Number n)
      return n.doubleValue() != 0.0;
    return true;
  }

  private static String message(final Object message, final Object params) {
    final String text = message != null ? message.toString() : "Validation failed";
    if (params instanceof Collection<?> list && !list.isEmpty())
      return String.format(text, list.toArray());
    if (params instanceof Object[] array && array.length > 0)
      return String.format(text, array);
    return text;
  }
}
