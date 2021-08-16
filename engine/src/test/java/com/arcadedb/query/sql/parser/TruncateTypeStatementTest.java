package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class TruncateTypeStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("TRUNCATE TYPE Foo");
    checkRightSyntax("truncate type Foo");
    checkRightSyntax("TRUNCATE TYPE Foo polymorphic");
    checkRightSyntax("truncate type Foo POLYMORPHIC");
    checkRightSyntax("TRUNCATE TYPE Foo unsafe");
    checkRightSyntax("truncate type Foo UNSAFE");
    checkRightSyntax("TRUNCATE TYPE Foo polymorphic unsafe");
    checkRightSyntax("truncate type Foo POLYMORPHIC UNSAFE");
    checkWrongSyntax("TRUNCATE TYPE Foo polymorphic unsafe FOO");
    checkRightSyntax("truncate type `Foo bar` ");
    checkWrongSyntax("truncate type Foo bar ");
    checkWrongSyntax("truncate clazz Foo ");
  }
}
