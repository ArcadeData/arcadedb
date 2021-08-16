package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class RebuildIndexStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("REBUILD INDEX *");
    checkRightSyntax("REBUILD INDEX Foo");
    checkRightSyntax("rebuild index Foo");
    checkRightSyntax("REBUILD INDEX Foo.bar");
    checkRightSyntax("REBUILD INDEX Foo.bar.baz");
    checkWrongSyntax("REBUILD INDEX Foo.bar foo");
  }
}
