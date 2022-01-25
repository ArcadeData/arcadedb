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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Created by luigidellaquila on 11/10/16. */
public class PatternTest extends ParserTestAbstract {

  @Test
  public void testSimplePattern() {
    String query = "MATCH {as:a, type:Person} return a";
    SqlParser parser = getParserFor(query);
    try {
      MatchStatement stm = (MatchStatement) parser.parse();
      stm.buildPatterns();
      Pattern pattern = stm.pattern;
      Assertions.assertEquals(0, pattern.getNumOfEdges());
      Assertions.assertEquals(1, pattern.getAliasToNode().size());
      Assertions.assertNotNull(pattern.getAliasToNode().get("a"));
      Assertions.assertEquals(1, pattern.getDisjointPatterns().size());
    } catch (ParseException e) {
      Assertions.fail();
    }
  }

  @Test
  public void testCartesianProduct() {
    String query = "MATCH {as:a, type:Person}, {as:b, type:Person} return a, b";
    SqlParser parser = getParserFor(query);
    try {
      MatchStatement stm = (MatchStatement) parser.parse();
      stm.buildPatterns();
      Pattern pattern = stm.pattern;
      Assertions.assertEquals(0, pattern.getNumOfEdges());
      Assertions.assertEquals(2, pattern.getAliasToNode().size());
      Assertions.assertNotNull(pattern.getAliasToNode().get("a"));
      List<Pattern> subPatterns = pattern.getDisjointPatterns();
      Assertions.assertEquals(2, subPatterns.size());
      Assertions.assertEquals(0, subPatterns.get(0).getNumOfEdges());
      Assertions.assertEquals(1, subPatterns.get(0).getAliasToNode().size());
      Assertions.assertEquals(0, subPatterns.get(1).getNumOfEdges());
      Assertions.assertEquals(1, subPatterns.get(1).getAliasToNode().size());

      Set<String> aliases = new HashSet<>();
      aliases.add("a");
      aliases.add("b");
      aliases.remove(subPatterns.get(0).getAliasToNode().keySet().iterator().next());
      aliases.remove(subPatterns.get(1).getAliasToNode().keySet().iterator().next());
      Assertions.assertEquals(0, aliases.size());

    } catch (ParseException e) {
      Assertions.fail();
    }
  }

  @Test
  public void testComplexCartesianProduct() {
    String query =
        "MATCH {as:a, type:Person}-->{as:b}, {as:c, type:Person}-->{as:d}-->{as:e}, {as:d, type:Foo}-->{as:f} return a, b";
    SqlParser parser = getParserFor(query);
    try {
      MatchStatement stm = (MatchStatement) parser.parse();
      stm.buildPatterns();
      Pattern pattern = stm.pattern;
      Assertions.assertEquals(4, pattern.getNumOfEdges());
      Assertions.assertEquals(6, pattern.getAliasToNode().size());
      Assertions.assertNotNull(pattern.getAliasToNode().get("a"));
      List<Pattern> subPatterns = pattern.getDisjointPatterns();
      Assertions.assertEquals(2, subPatterns.size());

      Set<String> aliases = new HashSet<>();
      aliases.add("a");
      aliases.add("b");
      aliases.add("c");
      aliases.add("d");
      aliases.add("e");
      aliases.add("f");
      aliases.removeAll(subPatterns.get(0).getAliasToNode().keySet());
      aliases.removeAll(subPatterns.get(1).getAliasToNode().keySet());
      Assertions.assertEquals(0, aliases.size());

    } catch (ParseException e) {
      Assertions.fail();
    }
  }
}
