package com.arcadedb.query.sql.function.conversion;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodAsDecimalTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodAsDecimal();
    }

    @Test
    void testNulIsReturnedAsNull() {
        Object result = method.execute(null, null, null, null, null);
        assertThat(result).isNull();
    }

    @Test
    void testStringToDecimal() {
        Object result = method.execute("10.0", null, null, "10.0", null);
        assertThat(result).isInstanceOf(BigDecimal.class);
        assertThat(result).isEqualTo(new BigDecimal("10.0"));
    }

    @Test
    void testLongToDecimal() {
        Object result = method.execute(10l, null, null, 10l, null);
        assertThat(result).isInstanceOf(BigDecimal.class);
        assertThat(result).isEqualTo(new BigDecimal("10"));
    }

    @Test
    void testDecimalToDecimal() {
        Object result = method.execute(10.0f, null, null, 10.0f, null);
        assertThat(result).isInstanceOf(BigDecimal.class);
        assertThat(result).isEqualTo(new BigDecimal("10.0"));
    }

    @Test
    void testIntegerToDecimal() {
        Object result = method.execute(10, null, null, 10, null);
        assertThat(result).isInstanceOf(BigDecimal.class);
        assertThat(result).isEqualTo(new BigDecimal("10"));
    }

}
