package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 */
public class ConsoleStatementExecutionTest extends TestHelper {

  @Test
  public void testError() {
    ResultSet result = database.command("sqlscript", "console.`error` 'foo bar'");
    assertThat(Optional.ofNullable(result)).isNotNull();
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("level")).isEqualTo("error");
    assertThat(item.<String>getProperty("message")).isEqualTo("foo bar");
  }

  @Test
  public void testLog() {
    ResultSet result = database.command("sqlscript", "console.log 'foo bar'");
    assertThat(Optional.ofNullable(result)).isNotNull();
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("level")).isEqualTo("log");
    assertThat(item.<String>getProperty("message")).isEqualTo("foo bar");
  }

  @Test
  public void testInvalidLevel() {
    try {
      database.command("sqlscript", "console.bla 'foo bar'");
      fail("");
    } catch (CommandExecutionException x) {
      // EXPECTED
    } catch (Exception x2) {
      fail("");
    }
  }
}
