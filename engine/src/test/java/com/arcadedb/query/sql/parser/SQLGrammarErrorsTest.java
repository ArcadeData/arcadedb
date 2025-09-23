package com.arcadedb.query.sql.parser;

import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Simple test to verify improved SQL grammar error messages
 */
class SQLGrammarErrorsTest {

  @Test
  void testSQLGrammarErrors() {
    StatementCache cache = new StatementCache(null, 100);

    // Test case 1: Duplicate AND operators
    assertThatThrownBy(
        () -> cache.get("select from Chat where #22:1 IN $brain AND  and contextVariables['local:insuranceGrid'] = true"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("AND operator")
        .hasMessageContaining("must be followed by a condition");

    // Test case 2: OR followed by AND
    assertThatThrownBy(() -> cache.get("select from Chat where col = 1 OR AND col2 = 2"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("OR operator")
        .hasMessageContaining("must be followed by a condition");

    // Test case 3: Missing condition after AND
    assertThatThrownBy(() -> cache.get("select from Chat where col = 1 AND"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("AND operator")
        .hasMessageContaining("must be followed by a condition");

    // Test case 4: Valid query should work
    cache.get("select from Chat where col = 1 AND col2 = 2");
  }
}
