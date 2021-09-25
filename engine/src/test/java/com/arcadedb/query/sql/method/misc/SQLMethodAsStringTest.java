package com.arcadedb.query.sql.method.misc;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodAsStringTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodAsString();

    }

    @Test
    void testNulIsReturnedAsNull() {
        Object result = method.execute(null, null, null, null, null);
        assertThat(result).isNull();
    }

    @Test
    void testStringIsReturnedAsString() {
        Object result = method.execute(null, null, null, "a string", null);
        assertThat(result).isEqualTo("a string");
    }

    @Test
    void testNumberIsReturnedAsString() {
        Object result = method.execute(null, null, null, 100, null);
        assertThat(result).isEqualTo("100");

        result = method.execute(null, null, null, 100.0, null);
        assertThat(result).isEqualTo("100.0");
    }

}
