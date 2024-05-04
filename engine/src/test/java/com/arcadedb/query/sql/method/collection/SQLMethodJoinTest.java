package com.arcadedb.query.sql.method.collection;

import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.method.string.SQLMethodSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class SQLMethodJoinTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodJoin();
    }

    @Test
    void testNullValue() {
        Object result = method.execute(null, null, null, null);
        assertThat(result).isNull();
    }

    @Test
    void testJoinEmptyList() {
        final Object result = method.execute(Collections.emptyList(), null, null, null);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("");
    }

    @Test
    void testJoinAnInteger() {
        final Object result = method.execute(10, null, null, null);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("10");
    }

    @Test
    void testJoinByDefaultSeparator() {
        final Object result = method.execute(List.of("first", "second"), null, null, null);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("first,second");
    }

    @Test
    void testJoinByDefaultSeparatorWithNullParams() {
        Object result = method.execute(List.of("first", "second"), null, null, new Object[]{null});
        assertThat(result).isEqualTo("first,second");
    }

    @Test
    void testJoinByProvidedSeparator() {
        final Object result = method.execute(List.of("first", "second"), null, null, new String[]{";"});
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("first;second");
    }
}
