package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StatementCacheTest {

  @Test
  public void testInIsNotAReservedWord() {
    StatementCache cache = new StatementCache(null,2);
    cache.get("select from foo");
    cache.get("select from bar");
    cache.get("select from baz");

    Assertions.assertTrue(cache.contains("select from bar"));
    Assertions.assertTrue(cache.contains("select from baz"));
    Assertions.assertFalse(cache.contains("select from foo"));

    cache.get("select from bar");
    cache.get("select from foo");

    Assertions.assertTrue(cache.contains("select from bar"));
    Assertions.assertTrue(cache.contains("select from foo"));
    Assertions.assertFalse(cache.contains("select from baz"));
  }
}
