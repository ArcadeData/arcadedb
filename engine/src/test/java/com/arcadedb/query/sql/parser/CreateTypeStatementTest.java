package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class CreateTypeStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE DOCUMENT TYPE Foo");
    checkRightSyntax("create document type Foo");
    checkRightSyntax("create document type Foo extends bar, baz bucket 12, 13, 14 buckets 5 abstract");
    checkRightSyntax("CREATE DOCUMENT TYPE Foo EXTENDS bar, baz BUCKET 12, 13, 14 BUCKETS 5 ABSTRACT");
    checkRightSyntax("CREATE DOCUMENT TYPE Foo EXTENDS bar, baz BUCKET 12,13, 14 BUCKETS 5 ABSTRACT");

    checkWrongSyntax("CREATE DOCUMENT TYPE Foo EXTENDS ");
    checkWrongSyntax("CREATE DOCUMENT TYPE Foo BUCKET ");
    checkWrongSyntax("CREATE DOCUMENT TYPE Foo BUCKETS ");
    checkWrongSyntax("CREATE DOCUMENT TYPE Foo BUCKETS 1,2 ");
  }

  @Test
  public void testIfNotExists() {
    checkRightSyntax("CREATE DOCUMENT TYPE Foo if not exists");
    checkRightSyntax("CREATE DOCUMENT TYPE Foo IF NOT EXISTS");
    checkRightSyntax("CREATE DOCUMENT TYPE Foo if not exists extends V");

    checkWrongSyntax("CREATE DOCUMENT TYPE Foo if");
    checkWrongSyntax("CREATE DOCUMENT TYPE Foo if not");
  }
}
