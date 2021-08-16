package com.arcadedb.query.sql.parser;

public class AlterPropertyStatementTest extends ParserTestAbstract {

  //@Test
  public void testPlain() {
    checkRightSyntax("ALTER PROPERTY Foo.foo NAME Bar");
    checkRightSyntax("alter property Foo.foo NAME Bar");
  }
}
