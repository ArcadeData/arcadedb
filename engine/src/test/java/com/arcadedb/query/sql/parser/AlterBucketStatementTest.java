package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class AlterBucketStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER BUCKET Foo name bar");
    checkRightSyntax("alter bucket Foo name bar");
    checkRightSyntax("alter bucket Foo* name bar");
  }
}
