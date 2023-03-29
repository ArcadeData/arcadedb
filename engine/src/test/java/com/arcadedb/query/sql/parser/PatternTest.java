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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

/** Created by luigidellaquila on 11/10/16. */
public class PatternTest extends ParserTestAbstract {

  @Test
  public void testSimplePattern() {
    final String query = "MATCH {as:a, type:Person} return a";
    final SqlParser parser = getParserFor(query);
    try {
      final MatchStatement stm = (MatchStatement) parser.Parse();
      stm.buildPatterns();
      final Pattern pattern = stm.pattern;
      Assertions.assertEquals(0, pattern.getNumOfEdges());
      Assertions.assertEquals(1, pattern.getAliasToNode().size());
      Assertions.assertNotNull(pattern.getAliasToNode().get("a"));
      Assertions.assertEquals(1, pattern.getDisjointPatterns().size());
    } catch (final ParseException e) {
      Assertions.fail();
    }
  }

  @Test
  public void testCartesianProduct() {
    final String query = "MATCH {as:a, type:Person}, {as:b, type:Person} return a, b";
    final SqlParser parser = getParserFor(query);
    try {
      final MatchStatement stm = (MatchStatement) parser.Parse();
      stm.buildPatterns();
      final Pattern pattern = stm.pattern;
      Assertions.assertEquals(0, pattern.getNumOfEdges());
      Assertions.assertEquals(2, pattern.getAliasToNode().size());
      Assertions.assertNotNull(pattern.getAliasToNode().get("a"));
      final List<Pattern> subPatterns = pattern.getDisjointPatterns();
      Assertions.assertEquals(2, subPatterns.size());
      Assertions.assertEquals(0, subPatterns.get(0).getNumOfEdges());
      Assertions.assertEquals(1, subPatterns.get(0).getAliasToNode().size());
      Assertions.assertEquals(0, subPatterns.get(1).getNumOfEdges());
      Assertions.assertEquals(1, subPatterns.get(1).getAliasToNode().size());

      final Set<String> aliases = new HashSet<>();
      aliases.add("a");
      aliases.add("b");
      aliases.remove(subPatterns.get(0).getAliasToNode().keySet().iterator().next());
      aliases.remove(subPatterns.get(1).getAliasToNode().keySet().iterator().next());
      Assertions.assertEquals(0, aliases.size());

    } catch (final ParseException e) {
      Assertions.fail();
    }
  }

  @Test
  public void testComplexCartesianProduct() {
    final String query =
        "MATCH {as:a, type:Person}-->{as:b}, {as:c, type:Person}-->{as:d}-->{as:e}, {as:d, type:Foo}-->{as:f} return a, b";
    final SqlParser parser = getParserFor(query);
    try {
      final MatchStatement stm = (MatchStatement) parser.Parse();
      stm.buildPatterns();
      final Pattern pattern = stm.pattern;
      Assertions.assertEquals(4, pattern.getNumOfEdges());
      Assertions.assertEquals(6, pattern.getAliasToNode().size());
      Assertions.assertNotNull(pattern.getAliasToNode().get("a"));
      final List<Pattern> subPatterns = pattern.getDisjointPatterns();
      Assertions.assertEquals(2, subPatterns.size());

      final Set<String> aliases = new HashSet<>();
      aliases.add("a");
      aliases.add("b");
      aliases.add("c");
      aliases.add("d");
      aliases.add("e");
      aliases.add("f");
      aliases.removeAll(subPatterns.get(0).getAliasToNode().keySet());
      aliases.removeAll(subPatterns.get(1).getAliasToNode().keySet());
      Assertions.assertEquals(0, aliases.size());

    } catch (final ParseException e) {
      Assertions.fail();
    }
  }
}
