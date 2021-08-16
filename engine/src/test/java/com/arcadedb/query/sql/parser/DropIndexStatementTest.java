package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class DropIndexStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP INDEX *");
    checkRightSyntax("DROP INDEX Foo");
    checkRightSyntax("drop index Foo");
    checkRightSyntax("DROP INDEX Foo.bar");
    checkRightSyntax("DROP INDEX Foo.bar.baz");
    checkRightSyntax("DROP INDEX Foo.bar.baz if exists");
    checkRightSyntax("DROP INDEX Foo.bar.baz IF EXISTS");
    checkWrongSyntax("DROP INDEX Foo.bar foo");
  }
}
