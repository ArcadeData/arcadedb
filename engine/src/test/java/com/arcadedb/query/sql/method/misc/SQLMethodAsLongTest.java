package com.arcadedb.query.sql.method.misc;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodAsLongTest {
    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodAsLong();
    }

    @Test
    void testNulIsReturnedAsNull() {
        Object result = method.execute(null, null, null, null, null);
        assertThat(result).isNull();
    }

    @Test
    void testStringToLong() {
        Object result = method.execute(null, null, null, "10", null);
        assertThat(result).isInstanceOf(Long.class);
        assertThat(result).isEqualTo(10l);
    }

    @Test
    void testLongToLong() {
        Object result = method.execute(null, null, null, 10l, null);
        assertThat(result).isInstanceOf(Long.class);
        assertThat(result).isEqualTo(10l);
    }

    @Test
    void testIntegerToLong() {
        Object result = method.execute(null, null, null, 10, null);
        assertThat(result).isInstanceOf(Long.class);
        assertThat(result).isEqualTo(10l);
    }

    @Test
    void testDateToLong() {
        Date now = new Date();
        Object result = method.execute(null, null, null, now, null);
        assertThat(result).isInstanceOf(Long.class);
        assertThat(result).isEqualTo(now.getTime());
    }

}
