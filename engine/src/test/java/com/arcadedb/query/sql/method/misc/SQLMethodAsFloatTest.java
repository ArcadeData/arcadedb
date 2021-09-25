package com.arcadedb.query.sql.method.misc;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodAsFloatTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodAsFloat();
    }

    @Test
    void testNulIsReturnedAsNull() {
        Object result = method.execute(null, null, null, null, null);
        assertThat(result).isNull();
    }

    @Test
    void testStringToFloat() {
        Object result = method.execute(null, null, null, "10.0", null);
        assertThat(result).isInstanceOf(Float.class);
        assertThat(result).isEqualTo(10.0f);
    }

    @Test
    void testLongToFloat() {
        Object result = method.execute(null, null, null, 10l, null);
        assertThat(result).isInstanceOf(Float.class);
        assertThat(result).isEqualTo(10.0f);
    }

    @Test
    void testFloatToFloat() {
        Object result = method.execute(null, null, null, 10.0f, null);
        assertThat(result).isInstanceOf(Float.class);
        assertThat(result).isEqualTo(10.0f);
    }

    @Test
    void testIntegerToFloat() {
        Object result = method.execute(null, null, null, 10, null);
        assertThat(result).isInstanceOf(Float.class);
        assertThat(result).isEqualTo(10.0f);
    }

}
