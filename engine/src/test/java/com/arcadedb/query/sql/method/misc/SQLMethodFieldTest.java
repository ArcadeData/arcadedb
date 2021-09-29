package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodFieldTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodField();

    }

    @Test
    void testNulIParamsReturnedAsNull() {
        Object result = method.execute(null, null, null, null, new Object[]{null});
        assertThat(result).isNull();
    }

    @Test
    void testFieldValue() {

        Database database = Mockito.mock(Database.class);

        DocumentType type = Mockito.mock(DocumentType.class);

//        MutableDocument doc = new MutableDocument(database, type, null);
//        doc.set("name", "Foo");
//        doc.set("surname", "Bar");

//        Object result = method.execute(null, doc, null, null, new Object[]{"name"});
//        assertThat(result).isNotNull();
//        assertThat(result).isEqualTo("Foo");

    }
}
