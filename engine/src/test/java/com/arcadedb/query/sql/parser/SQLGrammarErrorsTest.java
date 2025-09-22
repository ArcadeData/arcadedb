package com.arcadedb.query.sql.parser;

import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Assertions;

/**
 * Simple test to verify improved SQL grammar error messages
 */
public class SQLGrammarErrorsTest {
  public static void main(String[] args) {
    StatementCache cache = new StatementCache(null, 100);

    // Test case 1: Duplicate AND operators
    try {
      cache.get("select from Chat where #22:1 IN $brain AND  and contextVariables['local:insuranceGrid'] = true");
      Assertions.fail();
    } catch (CommandSQLParsingException e) {
      Assertions.assertTrue(e.getMessage().contains("AND operator") && e.getMessage().contains("must be followed by a condition"),
          "Expected improved error message about AND operator, got: " + e.getMessage());
    }

    // Test case 2: OR followed by AND
    try {
      cache.get("select from Chat where col = 1 OR AND col2 = 2");
      Assertions.fail();
    } catch (CommandSQLParsingException e) {
      Assertions.assertTrue(e.getMessage().contains("OR operator") && e.getMessage().contains("must be followed by a condition"),
          "Expected improved error message about OR operator, got: " + e.getMessage());
    }

    // Test case 3: Missing condition after AND
    try {
      cache.get("select from Chat where col = 1 AND");
      Assertions.fail();
    } catch (CommandSQLParsingException e) {
      Assertions.assertTrue(e.getMessage().contains("AND operator") && e.getMessage().contains("must be followed by a condition"),
          "Expected improved error message about AND operator, got: " + e.getMessage());
    }

    // Test case 4: Valid query should work
    cache.get("select from Chat where col = 1 AND col2 = 2");
  }
}
