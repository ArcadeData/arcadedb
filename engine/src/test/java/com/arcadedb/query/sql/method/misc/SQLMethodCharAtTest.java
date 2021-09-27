package com.arcadedb.query.sql.method.misc;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class SQLMethodCharAtTest {
    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodCharAt();

    }

    @Test
    void testNulIsReturnedAsNull() {
        Object result = method.execute(null, null, null, null, new Object[]{null});
        assertThat(result).isNull();
    }

    @Test
    void testChartAt() {
        Object result = method.execute("chars", null, null, null, new Object[]{3});
        assertThat(result).isEqualTo("r");
    }

}
