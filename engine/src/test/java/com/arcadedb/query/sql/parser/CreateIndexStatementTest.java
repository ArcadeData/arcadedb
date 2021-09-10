package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class CreateIndexStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE INDEX Foo DICTIONARY");

    checkWrongSyntax("CREATE INDEX Foo");
    checkRightSyntax("CREATE INDEX Foo.bar DICTIONARY");

    checkRightSyntax("CREATE INDEX Foo.bar on Foo (bar) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, baz) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, @rid) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar by key, baz by value) UNIQUE");

    checkRightSyntax("CREATE INDEX Foo.bar on Foo (bar) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, baz) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar by key, baz by value) UNIQUE");

    checkRightSyntax("create index OUser.name UNIQUE ENGINE LSM");
    checkRightSyntax("create index OUser.name UNIQUE engine LSM");

    checkRightSyntax("CREATE INDEX Foo.bar IF NOT EXISTS on Foo (bar) UNIQUE");
    checkWrongSyntax("CREATE INDEX Foo.bar IF EXISTS on Foo (bar) UNIQUE");
  }
}
