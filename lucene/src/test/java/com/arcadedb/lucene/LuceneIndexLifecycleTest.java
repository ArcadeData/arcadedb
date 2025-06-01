package com.arcadedb.lucene;

import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class LuceneIndexLifecycleTest extends ArcadeLuceneTestBase {

    @Test
    public void testCreateIndex() {
        database.transaction(() -> {
            DocumentType type = database.getSchema().createDocumentType("Doc");
            type.createProperty("name", Type.STRING);
            type.createProperty("text", Type.STRING);
            // Use the fully qualified name for the index type if not importing LuceneIndexFactory.LUCENE_ENGINE_NAME
            Index index = type.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, true, "text");
            assertEquals("text", index.getPropertyNames().get(0));
            assertEquals(Schema.INDEX_TYPE.FULL_TEXT.name(), index.getType());
            // Check if the engine name is Lucene (this might require accessing internal index properties or specific API if available)
            // For now, just check existence and type
        });
        // Verify index exists outside transaction, after commit
        database.scanType("Doc", true, record -> { /* just to ensure schema is loaded */ return true; });
        Index retrievedIndex = database.getSchema().getType("Doc").getIndexByProperties("text");
        assertNotNull(retrievedIndex, "Index should exist after transaction commit");
        assertEquals("text", retrievedIndex.getPropertyNames().get(0));
        assertEquals(Schema.INDEX_TYPE.FULL_TEXT.name(), retrievedIndex.getType());
    }

    @Test
    public void testCreateAndDropIndex() {
        String typeName = "DocToDrop";
        String propertyName = "content";
        String indexName = typeName + "[" + propertyName + "]"; // default name format
        database.transaction(() -> {
            DocumentType type = database.getSchema().createDocumentType(typeName);
            type.createProperty(propertyName, Type.STRING);
            type.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, true, propertyName);
        });
        Index retrievedIndex = database.getSchema().getType(typeName).getIndexByProperties(propertyName);
        assertNotNull(retrievedIndex, "Index should exist after creation");
        database.command("sql", "DROP INDEX `" + indexName + "`"); // Enclose index name in backticks
        Index droppedIndex = database.getSchema().getType(typeName).getIndexByProperties(propertyName);
        assertNull(droppedIndex, "Index should not exist after drop");
    }
}
