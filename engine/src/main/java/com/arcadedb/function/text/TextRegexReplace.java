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
package com.arcadedb.function.text;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.TimeBoundRegex;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * text.regexReplace(string, regex, replace) - Replace using regular expression.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextRegexReplace extends AbstractTextFunction {
  private static final int MAX_PATTERN_LENGTH = 500;

  // context.getCachedValue()/setCachedValue() key for the shared deadline - see execute() for why this needs to
  // be shared, not recomputed per call.
  private static final String DEADLINE_CACHE_KEY = "__TEXT_REGEXREPLACE_DEADLINE__";

  @Override
  protected String getSimpleName() {
    return "regexReplace";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Replace all matches of a regular expression with replacement";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final String regex = asString(args[1]);
    final String replacement = asString(args[2]);

    if (regex == null)
      return str;

    // Validate pattern length to prevent ReDoS attacks
    if (regex.length() > MAX_PATTERN_LENGTH) {
      throw new IllegalArgumentException(
          "Regex pattern exceeds maximum allowed length (" + MAX_PATTERN_LENGTH + "): " + regex.length());
    }

    // One deadline shared across every row this function runs against within the same query (issue #5886
    // follow-up): unlike LikeOperator/ILikeOperator (no CommandContext to cache on), this function does receive
    // one, so it gets the same treatment as MatchesCondition/RegexExpression rather than a fresh budget per
    // call - otherwise SELECT text.regexReplace(col, :pattern, 'x') FROM LargeType with a pathological pattern
    // could still cost up to rowCount * regexTimeout overall. context is null in some direct/unit-test
    // invocations of this function (see TextRegexReplaceTest); GlobalConfiguration.getValueAsLong(Database)
    // falls back to the compiled-in default in that case, and the deadline is simply not shared across calls
    // when there's no context to cache it on.
    final long regexDeadline = context != null ?
        context.getOrComputeRegexDeadline(DEADLINE_CACHE_KEY) :
        TimeBoundRegex.newDeadline(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(null));

    try {
      return TimeBoundRegex.replaceAllUntil(Pattern.compile(regex), str, replacement == null ? "" : replacement, regexDeadline);
    } catch (final PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regex pattern: " + e.getMessage(), e);
    } catch (final TimeoutException e) {
      // Catastrophic backtracking (issue #5886): TimeBoundRegex already bounds this to regexTimeout instead of
      // running unbounded, but still surfaces it through this function's existing IllegalArgumentException contract.
      throw new IllegalArgumentException("Regex pattern caused catastrophic backtracking and was aborted: " + regex, e);
    } catch (final StackOverflowError e) {
      // TimeBoundRegex only converts a StackOverflowError into a TimeoutException when regexTimeout is active;
      // with it explicitly disabled (arcadedb.command.regexTimeout <= 0), a stack-overflow-inducing pattern
      // propagates as itself instead - keep this function's original, documented IllegalArgumentException
      // contract for that combination too, not just for the bounded case.
      throw new IllegalArgumentException("Regex pattern caused stack overflow (possible catastrophic backtracking): " + regex, e);
    }
  }
}
