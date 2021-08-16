package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class TruncateBucketStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("TRUNCATE BUCKET Foo");
    checkRightSyntax("truncate bucket Foo");

    checkRightSyntax("TRUNCATE BUCKET 12");
    checkRightSyntax("truncate bucket 12");

    checkRightSyntax("TRUNCATE BUCKET Foo unsafe");

    checkRightSyntax("TRUNCATE BUCKET `Foo bar`");

    checkWrongSyntax("TRUNCATE CsUSTER Foo");
    checkWrongSyntax("truncate bucket Foo bar");
  }
}
