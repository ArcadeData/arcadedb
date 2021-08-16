package com.arcadedb.query.sql.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.fail;

public abstract class ParserTestAbstract {

  protected SimpleNode checkRightSyntax(String query) {
    SimpleNode result = checkSyntax(query, true);
    StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntax(builder.toString(), true);
    //    return checkSyntax(query, true);
  }

  protected SimpleNode checkRightSyntaxServer(String query) {
    SimpleNode result = checkSyntaxServer(query, true);
    StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntaxServer(builder.toString(), true);
    //    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkWrongSyntaxServer(String query) {
    return checkSyntaxServer(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        //        System.out.println(query);
        //        if (result != null) {
        //          System.out.println("->");
        //          StringBuilder builer = new StringBuilder();
        //          result.toString(null, builer);
        //          System.out.println(builer.toString());
        //          System.out.println("............");
        //        }

        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        System.out.println(query);
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  protected SimpleNode checkSyntaxServer(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();//parseServerStatement();
      if (!isCorrect) {
        //        System.out.println(query);
        //        if (result != null) {
        //          System.out.println("->");
        //          StringBuilder builer = new StringBuilder();
        //          result.toString(null, builer);
        //          System.out.println(builer.toString());
        //          System.out.println("............");
        //        }

        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        System.out.println(query);
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  private void printTree(String s) {
    SqlParser osql = getParserFor(s);
    try {
      SimpleNode n = osql.parse();

    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  protected SqlParser getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    SqlParser osql = new SqlParser(is);
    return osql;
  }
}
