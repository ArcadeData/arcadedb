package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Created by luigidellaquila on 26/04/16. */
public class IdentifierTest {

  @Test
  public void testBackTickQuoted() {
    Identifier identifier = new Identifier("foo`bar");

    Assertions.assertEquals(identifier.getStringValue(), "foo`bar");
    Assertions.assertEquals(identifier.getValue(), "foo\\`bar");
  }
}
