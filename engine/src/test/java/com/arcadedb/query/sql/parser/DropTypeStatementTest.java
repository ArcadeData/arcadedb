package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class DropTypeStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP TYPE Foo");
    checkRightSyntax("drop type Foo");
    checkRightSyntax("drop type Foo UNSAFE");
    checkRightSyntax("drop type Foo unsafe");
    checkRightSyntax("DROP TYPE `Foo bar`");
    checkRightSyntax("drop type ?");

    checkWrongSyntax("drop type Foo UNSAFE foo");
    checkWrongSyntax("drop type Foo bar");
  }

  @Test
  public void testIfExists() {
    checkRightSyntax("DROP TYPE Foo if exists");
    checkRightSyntax("DROP TYPE Foo IF EXISTS");
    checkRightSyntax("DROP TYPE if if exists");
    checkRightSyntax("DROP TYPE if if exists unsafe");
    checkRightSyntax("DROP TYPE ? IF EXISTS");

    checkWrongSyntax("drop type Foo if");
    checkWrongSyntax("drop type Foo if exists lkj");
    checkWrongSyntax("drop type Foo if lkj");
  }
}
