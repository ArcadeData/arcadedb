package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

public class BatchScriptTest {

  @Test
  public void testPlain() {
    checkRightSyntax("begin;select from foo; return bar;");
    checkRightSyntax("begin;\nselect from foo;\n return bar;");
    checkRightSyntax("begin;\nselect from foo;/*foo bar*/ return bar;");
    checkRightSyntax("/*foo bar*/ begin;\nselect from foo;return bar;/*foo bar*/ ");

    String s =
        ""
            + "begin;"
            + "let $a = select from foo let a = 13 where bar = 'baz';"
            + "let $b = insert into foo set name = 'baz';"
            + "let $c = update v set name = 'lkajsd';"
            + "if($c < $a){"
            + "   update v set surname = baz;"
            + "   if($c < $b){"
            + "       return 0;"
            + "   }"
            + "}"
            + "return 1;";

    checkRightSyntax(s);

    s =
        ""
            + "begin;\n"
            + "let $a = select from foo let a = 13 where bar = 'baz';\n"
            + "let $b = insert into foo set name = 'baz';\n"
            + "let $c = update v set name = 'lkajsd';\n"
            + "if($c < $a){\n"
            + "   update v set surname = baz;\n"
            + "   if($c < $b){\n"
            + "       return 0;\n"
            + "   }\n"
            + "}\n"
            + "return 1;\n";
    checkRightSyntax(s);

    s =
        ""
            + "begin;\n"
            + "let $a = select from foo let a = 13 where bar = 'baz';\n"
            + "let $b = insert into foo set name = 'baz';\n"
            + "let $c = update v set \n"
            + "/** foo bar */\n"
            + "name = 'lkajsd';\n"
            + "if($c < $a){\n"
            + "   update v set surname = baz;\n"
            + "   if($c < $b){\n"
            + "       return 0;\n"
            + "   }\n"
            + "}\n"
            + "return 1;\n";
    checkRightSyntax(s);
  }

  protected List<Statement> checkRightSyntax(String query) {
    return checkSyntax(query, true);
  }

  protected List<Statement> checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected List<Statement> checkSyntax(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      List<Statement> result = osql.parseScript();
      //      for(Statement stm:result){
      //        System.out.println(stm.toString()+";");
      //      }
      if (!isCorrect) {
        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  protected SqlParser getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    SqlParser osql = new SqlParser(is);
    return osql;
  }
}
