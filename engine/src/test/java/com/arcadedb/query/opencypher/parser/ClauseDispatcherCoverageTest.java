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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Since issue #9940 a clause the dispatcher has no handler for is refused instead of silently dropped. A new alternative of
 * the grammar's {@code clause} rule must therefore get a handler (or an explicit refusal) in {@link ClauseDispatcher}: this
 * test counts the alternatives of {@link Cypher25Parser.ClauseContext} against the registered handlers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClauseDispatcherCoverageTest {

  @Test
  void everyClauseAlternativeOfTheGrammarHasAHandler() {
    final long alternatives = Arrays.stream(Cypher25Parser.ClauseContext.class.getDeclaredMethods())
        .filter(m -> Modifier.isPublic(m.getModifiers()) && m.getParameterCount() == 0)
        .filter(m -> ParserRuleContext.class.isAssignableFrom(m.getReturnType()))
        .map(Method::getName)
        .distinct()
        .count();
    assertThat(alternatives).isPositive();
    assertThat(new ClauseDispatcher().handlerCount()).isEqualTo((int) alternatives);
  }
}
