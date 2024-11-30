package com.arcadedb.query.sql.function.time;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SQLFunctionDurationTest {

  private SQLFunctionDuration function;

  @BeforeEach
  public void setup() {
    function = new SQLFunctionDuration();
  }

  @Test
  public void testDurationWithValidParameters() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      db.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");
      db.command("sql", "alter database `arcadedb.dateImplementation` `java.time.LocalDate`");

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      Object result = function.execute(null, null, null, new Object[] { 5, "second" }, context);
      assertThat(result).isEqualTo(Duration.ofSeconds(5));

      result = function.execute(null, null, null, new Object[] { 2, "minute" }, null);
      assertThat(result).isEqualTo(Duration.ofMinutes(2));
    });

  }

  @Test
  public void testDurationWithInvalidAmount() {

    assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { "invalid", "SECONDS" }, null)).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("invalid");

  }

  @Test
  public void testDurationWithInvalidTimeUnit() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { 5, "INVALID_UNIT" }, null)).isInstanceOf(
        SerializationException.class).hasMessageContaining("Unsupported datetime precision 'INVALID_UNIT'");
  }

  @Test
  public void testDurationWithIncorrectNumberOfParameters() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { 5 }, null)).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("duration() function expected 2 parameters: amount and time-unit");
  }
}
