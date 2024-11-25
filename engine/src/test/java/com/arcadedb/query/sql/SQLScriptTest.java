package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLScriptTest extends TestHelper {
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE DOCUMENT TYPE foo");
      database.command("sql", "insert into foo (name, bar) values ('a', 1)");
      database.command("sql", "insert into foo (name, bar) values ('b', 2)");
      database.command("sql", "insert into foo (name, bar) values ('c', 3)");
    });
  }

  @Test
  public void testQueryOnDeprecated() {
    String script = """
        begin;
        let $a = select from foo;
        commit;
        return $a;
        """;
    ResultSet qResult = database.command("sqlscript", script);

    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  public void testQuery() {
    String script = """
        begin;
        let $a = select from foo;
        commit;
        return $a;
        """;
    ResultSet qResult = database.command("SQLScript", script);
    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  public void testTx() {
    String script = """
        begin isolation REPEATABLE_READ;
        let $a = insert into V set test = 'sql script test';
        commit retry 10;
        return $a;
        """;
    Document qResult = database.command("SQLScript", script).next().toElement();

    assertThat(qResult).isNotNull();
  }

  @Test
  public void testReturnExpanded() {
    database.transaction(() -> {
      String script = """
          let $a = insert into V set test = 'sql script test';
          return $a.asJSON();
          """;
      String qResult = database.command("SQLScript", script).next().getProperty("value").toString();
      assertThat(qResult).isNotNull();
      // VALIDATE JSON
      new JSONObject(qResult);

      script = """
          let $a = select from V limit 2;
          return $a.asJSON();
          """;
      String result = database.command("SQLScript", script).next().getProperty("value").toString();

      assertThat(result).isNotNull();
      result = result.trim();
      assertThat(result).startsWith("{").endsWith("}");

      // VALIDATE JSON
      new JSONObject(result);
    });

  }

  @Test
  public void testSleep() {
    long begin = System.currentTimeMillis();

    database.command("SQLScript", "sleep 500");

    assertThat(System.currentTimeMillis() - begin >= 500).isTrue();
  }

  //@Test
  public void testConsoleLog() {
    String script = """
        LET $a = 'log'
        console.log 'This is a test of log for ${a}'
        """;
    database.command("SQLScript", script);
  }

  //@Test
  public void testConsoleOutput() {
    String script = """
        LET $a = 'output'
        console.output 'This is a test of log for ${a}'
        """;
    database.command("SQLScript", script);
  }

  //@Test
  public void testConsoleError() {
    String script = """
        LET $a = 'error';
        CONSOLE.ERROR 'This is a test of log for ${a}';
        """;
    database.command("SQLScript", script);
  }

  @Test
  public void testReturnObject() {
    ResultSet result = database.command("SQLScript", "return [{ a: 'b' }]");

    assertThat(Optional.ofNullable(result)).isNotNull();

    assertThat(result.next().<String>getProperty("a")).isEqualTo("b");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  public void testIncrementAndLet() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestCounter");

      String script = """
          INSERT INTO TestCounter set weight = 3;
          LET counter = SELECT count(*) as count FROM TestCounter;
          UPDATE TestCounter SET weight += $counter[0].count RETURN AFTER @this;
          """;
      ResultSet qResult = database.command("SQLScript", script.toString());

      assertThat(qResult.hasNext()).isTrue();
      Result result = qResult.next();
      assertThat(result.<Long>getProperty("weight")).isEqualTo(4L);
    });
  }

  @Test
  public void testIf1() {
    String script = """
        let $a = select 1 as one;
        if($a[0].one = 1){
         return 'OK';
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testIf2() {
    String script = """
        let $a = select 1 as one;
        if ($a[0].one = 1) {
          return 'OK';
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script);

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testIf3() {

    String script = """
        let $a = select 1 as one;
        if ($a[0].one = 1) {
          return 'OK';
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script);
    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testNestedIf2() {
    String script = """
        let $a = select 1 as one;
        if ($a[0].one = 1) {
            if ($a[0].one = 'zz') {
              return 'FAIL';
            }
          return 'OK';
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script);

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testNestedIf3() {
    String script = """
        let $a = select 1 as one;
        if ($a[0].one = 'zz') {
            if ($a[0].one = 1) {
              return 'FAIL';
            }
          return 'FAIL';
        }
        return 'OK';
        """;
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testIfRealQuery() {
    String script = """
        let $a = select from foo;
        if ($a is not null and $a.size() = 3 ){
          return $a;
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  public void testIfMultipleStatements() {
    String script = """
        let $a = select 1 as one;
        -- this is a comment
        if ($a[0].one = 1) {
          let $b = select 'OK' as ok;
          return $b[0].ok;
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script);

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testSemicolonInString() {
    // testing parsing problem
    ResultSet qResult = database.command("SQLScript", "let $a = select 'foo ; bar' as one\n");
  }

  @Test
  public void testQuotedRegex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE QuotedRegex2");
      String batch = "INSERT INTO QuotedRegex2 SET regexp=\"'';\"";

      database.command("SQLScript", batch.toString());

      ResultSet result = database.query("sql", "SELECT FROM QuotedRegex2");
      Document doc = result.next().toElement();

      assertThat(result.hasNext()).isFalse();
      assertThat(doc.get("regexp")).isEqualTo("'';");

    });
  }

  @Test
  public void testParameters1() {
    String className = "testParameters1";
    database.getSchema().createVertexType(className);
    database.getSchema().createEdgeType("E");
    String script = """
        BEGIN;
        LET $a = CREATE VERTEX %s SET name = :name;
        LET $b = CREATE VERTEX %s SET name = :name;
        LET $edge = CREATE EDGE E from $a to $b;
        COMMIT;
        RETURN $edge;
        """.formatted(className, className);

    HashMap<String, Object> map = new HashMap<>();
    map.put("name", "bozo");
    map.put("_name2", "bozi");

    ResultSet rs = database.command("sqlscript", script, map);
    rs.close();

    rs = database.query("sql", "SELECT FROM " + className + " WHERE name = ?", "bozo");

    assertThat(rs.hasNext()).isTrue();
    rs.next();
    rs.close();
  }

  @Test
  public void testPositionalParameters() {
    String className = "testPositionalParameters";
    database.getSchema().createVertexType(className);
    database.getSchema().createEdgeType("E");
    String script = """
        BEGIN;
        LET $a = CREATE VERTEX %s SET name = ?;
        LET $b = CREATE VERTEX %s SET name = ?;
        LET $edge = CREATE EDGE E from $a to $b;
        COMMIT;
        RETURN $edge;
        """.formatted(className, className);

    ResultSet rs = database.command("SQLScript", script, "bozo", "bozi");
    rs.close();

    rs = database.query("sql", "SELECT FROM " + className + " WHERE name = ?", "bozo");

    assertThat(rs.hasNext()).isTrue();
    rs.next();
    rs.close();
  }

  @Test
  public void testInsertJsonNewLines() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("doc");

      final ResultSet result = database.command("sqlscript", """
          INSERT INTO doc CONTENT {
          "head" : {
            "vars" : [ "item", "itemLabel" ]
          },
          "results" : {
            "bindings" : [ {
              "item" : {
                    "type" : "uri",
                        "value" : "http://www.wikidata.org/entity/Q113997665"
                  },
                  "itemLabel" : {
                    "xml:lang" : "en",
                        "type" : "literal",
                        "value" : "ArcadeDB"
                  }
                }, {
                  "item" : {
                    "type" : "uri",
                        "value" : "http://www.wikidata.org/entity/Q808716"
                  },
                  "itemLabel" : {
                    "xml:lang" : "en",
                        "type" : "literal",
                        "value" : "OrientDB"
                  }
                } ]
              }
          }""");

      assertThat(result.hasNext()).isTrue();
      final Result res = result.next();
      assertThat(res.hasProperty("head")).isTrue();
      assertThat(res.hasProperty("results")).isTrue();
    });
  }
}
