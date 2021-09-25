package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodSizeTest {
    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodSize();
    }

    @Test
    void testNulIsReturnedAsZero() {
        Object result = method.execute(null, null, null, null, null);
        assertThat(result).isInstanceOf(Number.class);
        assertThat(result).isEqualTo(0);
    }

    @Test
    void testSizeOfIdentifiable() {
        Database db = Mockito.mock(Database.class);
        RID rid = new RID(db, 1, 1);
        Object result = method.execute(null, null, null, rid, null);
        assertThat(result).isInstanceOf(Number.class);
        assertThat(result).isEqualTo(1);
    }

    @Test
    void testSizeOfMultiValue() {
        Database db = Mockito.mock(Database.class);
        RID rid = new RID(db, 1, 1);
        Collection<RID> multiValue = List.of(rid, rid, rid);
        Object result = method.execute(null, null, null, multiValue, null);
        assertThat(result).isInstanceOf(Number.class);
        assertThat(result).isEqualTo(3);
    }

}
