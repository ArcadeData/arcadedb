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
package com.arcadedb.test.support;

/**
 * Sums the samples of a Prometheus text-format body by metric name and, optionally, by one label.
 */
public final class PrometheusScrape {
  private PrometheusScrape() {
  }

  /** @return the sum of every series named exactly {@code name}, 0 when absent */
  public static double sum(final String body, final String name) {
    return sum(body, name, null, null);
  }

  /** @return the sum of the series named exactly {@code name} whose labels include {@code label="value"}, 0 when absent */
  public static double sum(final String body, final String name, final String label, final String value) {
    final String wanted = label == null ? null : label + "=\"" + value + "\"";
    double total = 0.0;
    for (final String raw : body.split("\n")) {
      final String line = raw.trim();
      if (line.isEmpty() || line.charAt(0) == '#')
        continue;
      final int brace = line.indexOf('{');
      final int space = line.indexOf(' ');
      final int nameEnd = brace >= 0 && (space < 0 || brace < space) ? brace : space;
      if (nameEnd < 0 || !line.regionMatches(0, name, 0, nameEnd) || nameEnd != name.length())
        continue;
      if (wanted != null) {
        final int close = line.indexOf('}', nameEnd);
        if (brace < 0 || close < 0 || !hasLabel(line.substring(brace + 1, close), wanted))
          continue;
      }
      total += Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
    }
    return total;
  }

  private static boolean hasLabel(final String labels, final String wanted) {
    int from = 0;
    while ((from = labels.indexOf(wanted, from)) >= 0) {
      // the label name must start a label, so that "xreason" does not match "reason"
      if (from == 0 || labels.charAt(from - 1) == ',')
        return true;
      from++;
    }
    return false;
  }
}
