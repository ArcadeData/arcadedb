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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.PatternConst;
import com.arcadedb.utility.TimeBoundRegex;

import java.text.Normalizer;
import java.util.regex.Pattern;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodNormalize extends AbstractSQLMethod {

  public static final String NAME = "normalize";

  public SQLMethodNormalize() {
    super(NAME, 0, 2);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {

    if (value != null) {
      final Normalizer.Form form =
          params != null && params.length > 0 ?
              Normalizer.Form.valueOf(FileUtils.getStringContent(params[0].toString())) :
              Normalizer.Form.NFD;

      final String normalized = Normalizer.normalize(value.toString(), form);
      if (params != null && params.length > 1) {
        // The 2nd argument is a caller-supplied regex, not a literal (issue #5886) - bounded the same way as
        // every other user-controlled regex entry point, and sharing one deadline across every row this method
        // runs against within the same query (same rationale as TextRegexReplace - otherwise a pathological
        // pattern matched against many rows could cost up to rowCount * regexTimeout overall). context is null
        // in some direct/unit-test invocations of this method (see SQLMethodNormalizeTest);
        // GlobalConfiguration.getValueAsLong(Database) falls back to the compiled-in default in that case, and
        // the deadline is simply not shared across calls when there's no context to carry it. A
        // TimeoutException from the bound propagates as-is rather than being rewrapped (unlike TextRegexReplace,
        // which rewraps to preserve its own pre-existing IllegalArgumentException contract for regex failures) -
        // this method had no such prior contract, and TimeoutException is the shape
        // MatchesCondition/RegexExpression/LIKE/full-text/PromQL all surface too.
        final long regexDeadline = context != null ?
            context.getRegexDeadline() :
            TimeBoundRegex.newDeadline(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(null));
        return TimeBoundRegex.replaceAllUntil(Pattern.compile(FileUtils.getStringContent(params[1].toString())), normalized, "", regexDeadline);
      }
      return PatternConst.PATTERN_DIACRITICAL_MARKS.matcher(normalized).replaceAll("");
    }
    return null;
  }
}
