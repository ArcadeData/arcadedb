package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class CreateBucketStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE BUCKET Foo");
    checkRightSyntax("CREATE BUCKET Foo ID 14");
    checkRightSyntax("create bucket Foo");
    checkRightSyntax("create bucket Foo id 14");
    checkRightSyntax("CREATE BLOB BUCKET Foo");
    checkRightSyntax("create blob bucket Foo id 14");

    checkRightSyntax("create blob bucket Foo IF NOT EXISTS");
    checkRightSyntax("create blob bucket Foo IF NOT EXISTS id 14");

    checkWrongSyntax("CREATE Bucket");
    checkWrongSyntax("CREATE Bucket foo bar");
    checkWrongSyntax("CREATE Bucket foo.bar");
    checkWrongSyntax("CREATE Bucket foo id bar");

    checkWrongSyntax("create blob bucket Foo IF EXISTS");
    checkWrongSyntax("create blob bucket Foo IF EXISTS id 14");
  }
}
