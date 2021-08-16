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
