package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class DropBucketStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP BUCKET Foo");
    checkRightSyntax("drop bucket Foo");
    checkRightSyntax("DROP BUCKET 14");

    checkRightSyntax("DROP BUCKET 14 IF EXISTS");

    checkWrongSyntax("DROP BUCKET foo 14");
    checkWrongSyntax("DROP BUCKET foo bar");
    checkWrongSyntax("DROP BUCKET 14.1");
    checkWrongSyntax("DROP BUCKET 14 1");

    checkWrongSyntax("DROP BUCKET 14 IF NOT EXISTS");
  }
}
