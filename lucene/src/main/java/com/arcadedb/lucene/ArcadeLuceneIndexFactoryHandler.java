package com.arcadedb.lucene;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex;
import java.util.Map;

public class ArcadeLuceneIndexFactoryHandler implements IndexFactoryHandler {

    @Override
    public IndexInternal create(IndexBuilder builder) {
        DatabaseInternal database = builder.getDatabase();
        String indexName = builder.getIndexName();
        boolean unique = builder.isUnique();
        // Schema.INDEX_TYPE indexType = builder.getIndexType(); // This is implicitly "FULL_TEXT" for this handler
        Type[] keyTypes = builder.getKeyTypes();
        Map<String, String> properties = builder.getProperties();
        String filePath = builder.getFilePath();


        String analyzerClassName = org.apache.lucene.analysis.standard.StandardAnalyzer.class.getName();
        if (properties != null && properties.containsKey("analyzer")) {
           analyzerClassName = properties.get("analyzer");
        }

        // The actual ArcadeLuceneFullTextIndex will need to be instantiated here.
        // Its constructor will need to be defined to accept these parameters.
        // Adding filePath and keyTypes to the constructor call.
        return new com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex(database, indexName, unique, analyzerClassName, filePath, keyTypes);
    }
}
