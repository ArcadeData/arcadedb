package com.arcadedb.lucene;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex;
import java.util.Map;

public class ArcadeLuceneIndexFactoryHandler implements IndexFactoryHandler {

    public static final String LUCENE_FULL_TEXT_ALGORITHM = "LUCENE"; // Or just "LUCENE"
    public static final String LUCENE_CROSS_CLASS_ALGORITHM = "LUCENE_CROSS_CLASS";


    @Override
    public IndexInternal create(IndexBuilder builder) {
        DatabaseInternal database = builder.getDatabase();
        String indexName = builder.getIndexName();
        // boolean unique = builder.isUnique(); // Unique is part of IndexDefinition
        // Type[] keyTypes = builder.getKeyTypes(); // Key types are part of IndexDefinition

        // The IndexDefinition is the primary source of truth for index properties.
        IndexDefinition definition = builder.getIndexDefinition();
        if (definition == null) {
            // This case should ideally be prevented by the schema/builder logic before reaching here.
            // If it can happen, we might need to construct a minimal definition.
            // For now, assuming builder provides a valid definition or enough info to create one.
            // If builder.build() is called before this, definition should be set.
            // If this factory *is* part of builder.build(), then builder has all components.
            throw new IllegalArgumentException("IndexDefinition is required to create a Lucene index.");
        }

        // Algorithm is now part of IndexDefinition
        // String algorithm = definition.getAlgorithm() != null ? definition.getAlgorithm() : LUCENE_FULL_TEXT_ALGORITHM;
        // The factory is usually registered for a specific algorithm, so this check might be redundant
        // if this factory is only invoked for "LUCENE" or "LUCENE_CROSS_CLASS".

        // The constructor for ArcadeLuceneFullTextIndex is:
        // (DatabaseInternal db, String name, String typeName, IndexDefinition definition,
        //  String filePath, PaginatedFile metadataFile, PaginatedFile[] dataFiles,
        //  PaginatedFile[] treeFiles, int fileId, int pageSize,
        //  TransactionContext.AtomicOperation atomicOperation)
        // The IndexBuilder provides most of these.
        // typeName here is the schema type name the index is on, not the index type/algorithm.

        // filePath should be determined by the system, often databasePath + indexFileName
        String filePath = builder.getFilePath();
        if (filePath == null) {
             filePath = database.getDatabasePath() + java.io.File.separator + builder.getFileName();
        }


        // For PaginatedFile parameters, they are usually managed by the Storage engine.
        // For a Lucene index, it might not directly use these ArcadeDB PaginatedFile structures
        // for its main data, but it might have a metadata file.
        // The IndexBuilder should provide these if they are standard.
        // If Lucene manages its own files in 'filePath', some of these might be null or placeholders.

        // Let's assume the builder provides what's needed for the generic parts of an index.
        // The specific engine (Lucene) will manage its own data files within its directory (filePath).

        // The old constructor of ArcadeLuceneFullTextIndex took:
        // (DatabaseInternal database, String name, boolean unique, String analyzerClassName, String filePath, Type[] keyTypes)
        // This has been changed to the standard one.
        // We need to ensure that IndexDefinition within builder has all necessary info (like analyzer).
        // Analyzer is typically stored in definition.getOptions().get("analyzer")

        return new ArcadeLuceneFullTextIndex(
            database,
            indexName,
            definition.getTypeName(), // Class/Type name this index is on
            definition,
            filePath,
            builder.getMetadataFile(), // from IndexBuilder
            builder.getDataFiles(),    // from IndexBuilder
            builder.getTreeFiles(),    // from IndexBuilder (likely null/unused for Lucene)
            builder.getFileId(),       // from IndexBuilder
            builder.getPageSize(),     // from IndexBuilder (might be less relevant for Lucene)
            null                       // AtomicOperation: build is usually outside a TX or handles its own.
        );
    }
}
