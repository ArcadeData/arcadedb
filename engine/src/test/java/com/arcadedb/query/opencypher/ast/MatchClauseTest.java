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
package com.arcadedb.query.opencypher.ast;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MatchClause#hasDisconnectedPathPatterns()}, the signal
 * {@code DeleteStep} uses to decide whether it must fully read its input before deleting anything
 * (issue #6491).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MatchClauseTest {

  private static NodePattern node(final String variable) {
    return new NodePattern(variable, null, null);
  }

  private static PathPattern singleNode(final String variable) {
    return new PathPattern(List.of(node(variable)), null, null, null);
  }

  private static PathPattern relationship(final String from, final String to) {
    return new PathPattern(List.of(node(from), node(to)),
        List.of(new RelationshipPattern(null, null, Direction.OUT, null, null, null)), null, null);
  }

  @Test
  void singlePatternIsNeverDisconnected() {
    final MatchClause match = new MatchClause(List.of(relationship("a", "b")), false);
    assertThat(match.hasDisconnectedPathPatterns()).isFalse();
  }

  @Test
  void chainedPatternsSharingAVariableAreConnected() {
    // MATCH (a)-->(b), (b)-->(c): both path patterns share node variable "b"
    final MatchClause match = new MatchClause(List.of(relationship("a", "b"), relationship("b", "c")), false);
    assertThat(match.hasDisconnectedPathPatterns()).isFalse();
  }

  @Test
  void independentPatternsWithNoSharedVariableAreDisconnected() {
    // MATCH (a)-->(b), (c)-->(d): a Cartesian product of two unrelated path patterns
    final MatchClause match = new MatchClause(List.of(relationship("a", "b"), relationship("c", "d")), false);
    assertThat(match.hasDisconnectedPathPatterns()).isTrue();
  }

  @Test
  void selfLoopCrossJoinedWithAnUnrelatedPatternIsDisconnected() {
    // MATCH (a)-->(b), (n)<-[]-(n): the self-loop on "n" shares no variable with the "a"/"b" pattern,
    // so the two can independently re-enumerate and bind "n" to the same vertex across output rows.
    final MatchClause match = new MatchClause(List.of(relationship("a", "b"), relationship("n", "n")), false);
    assertThat(match.hasDisconnectedPathPatterns()).isTrue();
  }
}
