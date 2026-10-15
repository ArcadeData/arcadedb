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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Locale;

/**
 * How the value written after an attribute name is read, shared by {@code CREATE PROPERTY ... ( <name> <value> )} and
 * {@code ALTER PROPERTY ... <name> <value>} (issue #10099). The two statements set the same attributes, and each used to read the
 * value its own way: CREATE rendered every value as {@code "" + value}, so the SQL literal {@code null} reached
 * {@code setRegexp} as the four characters {@code null} - a valid, satisfiable pattern that left the property able to hold only
 * that text - where ALTER read it as "no pattern". One reading, in one place, is what keeps the two from disagreeing again.
 * <ul>
 *   <li>a value that is a bare word is refused: with no record to read a field from it could only evaluate to {@code null}, which
 *   would then silently clear the attribute ({@code ALTER PROPERTY T.a MAX nine} dropped the bound). Quote it to use it as a text;</li>
 *   <li>{@code null} is "no value": the attribute is cleared, not set to the text {@code null}. A quoted {@code 'null'} is the text;</li>
 *   <li>a flag takes {@code true} or {@code false} and nothing else, and says so.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PropertyAttributeValues {
  private PropertyAttributeValues() {
  }

  /**
   * Evaluates the value of an attribute, with no current record.
   *
   * @throws CommandExecutionException when the value is a bare word, a field reference that could only evaluate to {@code null}
   */
  static Object evaluate(final String attribute, final Expression value, final CommandContext context) {
    if (value.isBaseIdentifier())
      throw new CommandExecutionException(
          "The value `" + value + "` of " + attribute.toUpperCase(Locale.ENGLISH) + " is a field reference, not a value: there is"
              + " no record to read it from, so it would be read as null. Quote it as '" + value
              + "' to use it as a text, or write null to remove the attribute");
    return value.execute((Identifiable) null, context);
  }

  /** The text of a value, or {@code null} when there is none: the attribute is cleared instead of set to the text "null". */
  static String asText(final Object value) {
    return value == null ? null : String.valueOf(value);
  }

  /** A flag attribute takes {@code true} or {@code false}: anything else, {@code null} included, is refused with a message. */
  static boolean asBoolean(final String attribute, final Object value) {
    if (value instanceof Boolean flag)
      return flag;
    throw new CommandExecutionException(
        attribute.toUpperCase(Locale.ENGLISH) + " takes true or false, not " + (value == null ? "null" : "`" + value + "`"));
  }
}
