package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleComparisonTest extends TestHelper {

  @Test
  void simpleComparison() {
    final String script = """
        IF (1 > 0) {
            RETURN "yes";
        }
        RETURN "no";
        """;

    ResultSet result = database.command("SQLScript", script);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("value")).isEqualTo("yes");
  }

  @Test
  void variableComparison() {
    final String script = """
        LET $num = 5;
        IF ($num > 0) {
            RETURN "yes";
        }
        RETURN "no";
        """;

    ResultSet result = database.command("SQLScript", script);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("value")).isEqualTo("yes");
  }

  @Test
  void methodCallComparison() {
    final String script = """
        LET $list = [1, 2, 3];
        IF ($list.size() > 0) {
            RETURN "yes";
        }
        RETURN "no";
        """;

    ResultSet result = database.command("SQLScript", script);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("value")).isEqualTo("yes");
  }
}
