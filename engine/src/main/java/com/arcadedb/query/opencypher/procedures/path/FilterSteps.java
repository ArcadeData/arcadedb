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
package com.arcadedb.query.opencypher.procedures.path;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Splits a {@code relationshipFilter} or {@code labelFilter} argument into the steps of its sequence, the input handling
 * {@link RelationshipFilter} and {@link LabelFilter} share (issue #10025): a string is split on {@code ,} into the steps of
 * a repeating sequence, a list is one step whose elements are its {@code |} alternatives (it never forms a sequence), a
 * map is refused, and anything else is read as its string form.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class FilterSteps {
  private FilterSteps() {
  }

  /**
   * The steps of {@code filter}, or {@code null} when it filters nothing: null, or every step blank (a lone {@code |}
   * included).
   */
  static List<String> split(final String procedureName, final String parameterName, final Object filter) {
    final List<String> steps = switch (filter) {
      case null -> null;
      case String s -> List.of(s.split(",", -1));
      case Collection<?> collection -> {
        final StringJoiner joined = new StringJoiner("|");
        for (final Object item : collection)
          if (item != null)
            joined.add(item.toString());
        yield List.of(joined.toString());
      }
      case Map<?, ?> ignored ->
          throw new IllegalArgumentException(procedureName + "(): " + parameterName + " must be a string or a list of strings, got a map");
      default -> List.of(filter.toString());
    };
    return steps == null || isBlank(steps) ? null : steps;
  }

  /** Whether no step names anything: every one is blank once its {@code |} separators are dropped. */
  static boolean isBlank(final List<String> steps) {
    for (final String step : steps)
      if (!step.replace('|', ' ').isBlank())
        return false;
    return true;
  }
}
