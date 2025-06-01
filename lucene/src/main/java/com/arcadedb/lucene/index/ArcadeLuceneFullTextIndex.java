package com.arcadedb.lucene.index;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.document.Document;
import com.arcadedb.engine.PaginatedFile; // For constructor, might not be directly used by Lucene
import com.arcadedb.engine.Storage;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.RangeIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.engine.IndexEngine;
import com.arcadedb.lucene.engine.ArcadeLuceneFullTextIndexEngine; // Changed from OLuceneFullTextIndexEngine
import com.arcadedb.lucene.engine.LuceneIndexEngine; // The refactored interface
import com.arcadedb.lucene.query.LuceneKeyAndMetadata; // FIXME: Needs refactoring
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexDefinition;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

public class ArcadeLuceneFullTextIndex implements IndexInternal {

    private final DatabaseInternal database;
    private final String name;
    private IndexDefinition definition;
    private String filePath; // Path where Lucene index files are stored
    private int fileId; // ArcadeDB fileId, might not be directly used by Lucene files themselves
    private PaginatedFile metadataFile; // For ArcadeDB metadata about this index

    private LuceneIndexEngine engine; // Changed type to interface
    private STATUS status = STATUS.OFFLINE;

    // Moved constants to ArcadeLuceneIndexFactoryHandler
    // public static final String LUCENE_ALGORITHM = "LUCENE";


    // Constructor matching AbstractIndex an IndexFactory might call
    public ArcadeLuceneFullTextIndex(DatabaseInternal db, String name, String typeName, IndexDefinition definition,
                                     String filePath, PaginatedFile metadataFile, PaginatedFile[] dataFiles,
                                     PaginatedFile[] treeFiles, int fileId, int pageSize,
                                     TransactionContext.AtomicOperation atomicOperation) {
        this.database = db;
        this.name = name;
        this.definition = definition;
        this.filePath = filePath; // Should be directory for Lucene
        this.metadataFile = metadataFile; // ArcadeDB own metadata for this index
        this.fileId = fileId;
        // pageSize, dataFiles, treeFiles might be less relevant for Lucene which manages its own files.

        // Engine initialization is deferred to lazyInit or build/load
    }

    private void lazyInit() {
        if (engine == null) {
            // Determine if this is part of an active transaction and if an engine instance already exists for this TX.
            if (database.isTransactionActive() && database.getTransaction().getInvolvedIndexEngine(getName()) instanceof LuceneIndexEngine) {
                 this.engine = (LuceneIndexEngine) database.getTransaction().getInvolvedIndexEngine(getName());
                 if (this.engine == null) { // Should not happen if getInvolvedIndexEngine returned one
                     throw new IndexException("Cannot find transactional Lucene engine for index " + getName() + " though it was marked as involved.");
                 }
            } else {
                String algorithm = getAlgorithm(); // Uses the overridden getAlgorithm()
                com.arcadedb.document.Document engineMetadataDoc = new com.arcadedb.document.Document(database);
                if (this.definition != null && this.definition.getOptions() != null) {
                    engineMetadataDoc.fromMap(this.definition.getOptions());
                }

                if (com.arcadedb.lucene.ArcadeLuceneIndexFactoryHandler.LUCENE_CROSS_CLASS_ALGORITHM.equalsIgnoreCase(algorithm)) {
                    ArcadeLuceneCrossClassIndexEngine crossEngine = new ArcadeLuceneCrossClassIndexEngine(this.fileId, database.getStorage(), this.name);

                    // Construct IndexMetadata Pojo for crossEngine.init()
                    // OLuceneCrossClassIndexEngine.init takes IndexMetadata.
                    // IndexMetadata needs: name, typeName (class this index is on, can be null for cross-class marker), List<String> propertyNames, Type[] keyTypes, String algorithm, boolean isAutomatic, Map<String,String> options
                    IndexMetadata im = new IndexMetadata(
                        this.name,
                        this.definition.getPropertyNames(),
                        this.definition.getKeyTypes(),
                        this.definition.getOptions()
                    );
                    im.setTypeName(this.definition.getTypeName()); // May be null if truly cross-class and not bound to a type
                    im.setAlgorithm(algorithm);
                    im.setIsAutomatic(this.isAutomatic());
                    im.setUnique(this.isUnique());
                    im.setNullStrategy(this.getNullStrategy());
                    // Add other relevant properties from 'this.definition' to 'im' if needed by crossEngine.init()

                    crossEngine.init(im);
                    this.engine = crossEngine;
                } else { // Default to LUCENE_FULL_TEXT_ALGORITHM
                    ArcadeLuceneFullTextIndexEngine ftEngine = new ArcadeLuceneFullTextIndexEngine(database.getStorage(), name);
                    // OLuceneIndexEngineAbstract.init expects: String indexName, String indexType(algorithm), IndexDefinition, boolean isAutomatic, Document metadata
                    ftEngine.init(getName(), algorithm, definition, isAutomatic(), engineMetadataDoc);
                    this.engine = ftEngine;
                }
            }
            this.status = STATUS.ONLINE;
        }
    }


    @Override
    public String getAssociatedFileName() {
        return filePath;
    }

    @Override
    public void build(IndexBuilder builder) {
        this.definition = builder.getIndexDefinition();
        // filePath might be set by IndexBuilder or derived, ensure it's correct for Lucene (a directory path)
        this.filePath = builder.getFilePath() != null ? builder.getFilePath() : database.getDatabasePath() + "/" + builder.getFileName();
        this.fileId = builder.getFileId(); // Get fileId from builder

        lazyInit(); // Initialize engine
        try {
            Document engineMetadata = new Document(database);
            if (this.definition.getOptions() != null) {
                engineMetadata.fromMap(this.definition.getOptions());
            }

            // Parameters for engine.create:
            // valueSerializer, keySerializer: null for Lucene as it handles its own types.
            // keyTypes: from definition
            // nullPointerSupport: from definition
            // propertyNames.size(): as keySize (number of indexed fields)
            // clustersToIndex: from definition
            // options: from definition
            engine.create(
                null, // valueSerializer
                this.isAutomatic(),
                this.getKeyTypes(),
                this.getDefinition().isNullStrategyNode(), // nullPointerSupport
                null, // keySerializer
                this.getDefinition().getPropertyNames() != null ? this.getDefinition().getPropertyNames().size() : 0, // keySize
                this.getDefinition().getClustersToIndex(), // clustersToIndex (might be null)
                this.getDefinition().getOptions(), // engineProperties
                engineMetadata // metadata Document for engine
            );
            this.status = STATUS.ONLINE;
        } catch (Exception e) {
            throw new IndexException("Error during Lucene index build for index '" + getName() + "'", e);
        }
    }

    @Override
    public void setMetadata(IndexDefinition definition, String filePath, int pageSize, byte nullStrategy) {
        this.definition = definition;
        this.filePath = filePath;
        // pageSize and nullStrategy are part of definition or handled by Lucene engine differently.
        // This method is usually for loading existing index metadata.
        // We might need to re-init or load the engine here.
        if (engine != null) {
            engine.close(); // Close existing engine if any
        }
        engine = null; // Reset engine
        lazyInit(); // Re-initialize with new metadata
        // engine.load(...) might be relevant here if this implies loading an existing index.
    }

    @Override
    public STATUS getStatus() {
        return status;
    }

    @Override
    public void setStatus(STATUS status) {
        this.status = status;
        // Potentially pass this to the engine if it has its own status
    }

    @Override
    public void close() {
        if (engine != null) {
            engine.close();
            engine = null;
        }
        status = STATUS.OFFLINE;
    }

    @Override
    public void drop() {
        if (engine != null) {
            engine.delete(); // Engine handles file deletion
            engine = null;
        }
        // Additional cleanup of ArcadeDB metadata files if any (e.g., this.metadataFile)
        // This is usually handled by Schema.dropIndex calling this.
        status = STATUS.OFFLINE;
    }

    @Override
    public int getFileId() {
        return fileId; // Or a specific ID for Lucene structure if different
    }

    @Override
    public <T> T getComponent(String name, Class<T> type) {
        if (type.isAssignableFrom(engine.getClass())) {
            return type.cast(engine);
        }
        return null;
    }

    @Override
    public Type[] getKeyTypes() {
        return definition != null ? definition.getKeyTypes() : null;
    }

    @Override
    public byte[] getBinaryKeyTypes() {
        // Lucene doesn't use this in the same way as binary comparable keys.
        return null;
    }

    @Override
    public void setTypeIndex(TypeIndex typeIndex) {
        // Associated with schema type's index list. Store if needed.
    }

    @Override
    public TypeIndex getTypeIndex() {
        return null; // Retrieve if stored
    }

    @Override
    public void scheduleCompaction() {
        // Lucene has IndexWriter.forceMerge or IndexWriter.maybeMerge.
        // This could be a trigger for that.
        lazyInit();
        // engine.forceMerge(); // FIXME: Add such a method to engine interface if needed
    }

    @Override
    public String getMostRecentFileName() {
        return null; // Not directly applicable
    }

    @Override
    public Map<String, Object> toJSON() {
        // Serialize index configuration/stats to JSON.
        // Include name, type, definition, engine stats.
        Map<String, Object> json = new java.util.HashMap<>();
        json.put("name", getName());
        json.put("typeName", getTypeName());
        json.put("algorithm", getAlgorithm());
        if (definition != null) {
            json.put("definition", definition.getOptions()); // Or more detailed definition
        }
        if (engine != null) {
            // FIXME: engine should provide some stats or config
            // json.put("engineStats", engine.getStats());
        }
        return json;
    }

    @Override
    public Index getAssociatedIndex() {
        return null;
    }

    // --- Index Methods ---

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTypeName() { // This should be the Type's name this index is on, not algorithm
        return definition != null ? definition.getTypeName() : null;
    }

    @Override
    public String getAlgorithm() {
        // Return the actual algorithm from the definition if available
        return (definition != null && definition.getAlgorithm() != null) ?
               definition.getAlgorithm() :
               com.arcadedb.lucene.ArcadeLuceneIndexFactoryHandler.LUCENE_FULL_TEXT_ALGORITHM;
    }


    @Override
    public IndexDefinition getDefinition() {
        return definition;
    }

    @Override
    public boolean isUnique() {
        return definition != null && definition.isUnique(); // Lucene full-text usually not unique
    }

    @Override
    public List<String> getPropertyNames() {
        return definition != null ? definition.getPropertyNames() : Collections.emptyList();
    }

    @Override
    public long countEntries() {
        lazyInit();
        // engine.size(null) or engine.sizeInTx(null)
        // The ValuesTransformer is for OrientDB's SBTree based indexes. For Lucene, it's just a doc count.
        return engine.size(null);
    }

    public long getRecordCount() { // From OLuceneFullTextIndex
        return countEntries();
    }


    @Override
    public IndexCursor get(Object[] keys) {
        lazyInit();
        if (keys == null || keys.length == 0 || keys[0] == null) {
            throw new IllegalArgumentException("Lucene query key cannot be null.");
        }
        // Assuming keys[0] is the query string or a LuceneKeyAndMetadata object
        // FIXME: This needs to adapt to how LuceneKeyAndMetadata is structured and if options are passed
        Object queryKey = keys[0];
        Document metadata = null;
        if (keys.length > 1 && keys[1] instanceof Map) {
            metadata = new Document(database, (Map<String,Object>) keys[1]);
        } else if (keys.length > 1 && keys[1] instanceof Document) {
            metadata = (Document) keys[1];
        }

        // The engine's get method: Set<Identifiable> getInTx(Object key, LuceneTxChanges changes)
        // This needs to be wrapped in an IndexCursor.
        // The key for engine.getInTx is likely LuceneKeyAndMetadata
        // FIXME: Construct LuceneKeyAndMetadata correctly
        LuceneKeyAndMetadata keyAndMeta = new LuceneKeyAndMetadata(queryKey, metadata, null); // Assuming CommandContext can be null here

        Set<Identifiable> results = engine.getInTx(keyAndMeta, null); // Passing null for changes if not in tx or tx changes not used
        return new LuceneIndexCursor(results.iterator()); // FIXME: LuceneIndexCursor needs to be implemented
    }

    @Override
    public IndexCursor get(Object[] keys, int limit) {
        // FIXME: Implement limit. Lucene TopDocs can handle this.
        // This will require engine.getInTx or a similar method to accept a limit.
        lazyInit();
         if (keys == null || keys.length == 0 || keys[0] == null) {
            throw new IllegalArgumentException("Lucene query key cannot be null.");
        }
        Object queryKey = keys[0];
        Document metadata = new Document(database); // Default empty metadata
        if (keys.length > 1 && keys[1] instanceof Map) {
            metadata.fromMap((Map<String,Object>) keys[1]);
        } else if (keys.length > 1 && keys[1] instanceof Document) {
            metadata = (Document) keys[1];
        }
        if (limit > 0) {
            metadata.set("limit", limit); // Pass limit via metadata
        }
        LuceneKeyAndMetadata keyAndMeta = new LuceneKeyAndMetadata(queryKey, metadata, null);
        Set<Identifiable> results = engine.getInTx(keyAndMeta, null);
        return new LuceneIndexCursor(results.iterator()); // FIXME: LuceneIndexCursor
    }


    @Override
    public Stream<RID> getRidsStream(Object[] keys) {
        IndexCursor cursor = get(keys);
        return cursor.ridsStream();
    }

    public Set<Identifiable> get(Object key) { // From OLuceneFullTextIndex, matching engine's getInTx
        lazyInit();
        // This 'key' is likely LuceneKeyAndMetadata or the raw query string.
        return engine.getInTx(key, null); // Assuming null for LuceneTxChanges if not in a tx context for this call
    }

    public Set<RID> getRids(Object key) { // New method, if useful
        lazyInit();
        // This 'key' is likely LuceneKeyAndMetadata or the raw query string.
        // engine.getInTx returns Set<Identifiable>
        return engine.getInTx(key, null).stream().map(Identifiable::getIdentity).collect(Collectors.toSet());
    }


    @Override
    public RangeIndexCursor range(boolean ascendingOrder, Object[] beginKeys, boolean beginKeysIncluded, Object[] endKeys, boolean endKeysIncluded) {
        throw new UnsupportedOperationException("Range queries are not directly supported by Lucene full-text index in this manner. Use Lucene query syntax.");
    }

    @Override
    public RangeIndexCursor range(boolean ascendingOrder, Object[] beginKeys, boolean beginKeysIncluded, Object[] endKeys, boolean endKeysIncluded, int limit) {
        throw new UnsupportedOperationException("Range queries are not directly supported by Lucene full-text index in this manner. Use Lucene query syntax.");
    }

    @Override
    public IndexCursor iterator(boolean ascendingOrder) {
        throw new UnsupportedOperationException("Full iteration is not typically efficient for Lucene. Use a match_all query if needed.");
    }

    @Override
    public IndexCursor iterator(boolean ascendingOrder, Object[] fromKey, boolean fromKeyInclusive) {
        throw new UnsupportedOperationException("Full iteration is not typically efficient for Lucene.");
    }

    @Override
    public IndexCursor descendingIterator() {
        throw new UnsupportedOperationException("Full iteration is not typically efficient for Lucene.");
    }

    @Override
    public IndexCursor descendingIterator(Object[] fromKey, boolean fromKeyInclusive) {
        throw new UnsupportedOperationException("Full iteration is not typically efficient for Lucene.");
    }

    @Override
    public boolean supportsOrderedIterations() {
        return false; // Lucene orders by relevance score by default, not by key.
    }

    @Override
    public boolean isAutomatic() {
        return definition != null && definition.isAutomatic();
    }

    @Override
    public void setRebuilding(boolean rebuilding) {
        // Could set a flag or inform the engine
    }

    @Override
    public IndexEngine getEngine() {
        lazyInit();
        return engine;
    }

    @Override
    public boolean isValid() {
        // Check if engine is initialized and Lucene index is readable
        lazyInit();
        // FIXME: engine needs an isValid() or similar check
        return engine != null;
    }

    @Override
    public Map<String, String> getStats() {
        // FIXME: engine should provide stats (num docs, etc.)
        return Collections.emptyMap();
    }

    @Override
    public void setStats(Map<String, String> stats) {
        // Not typically set from outside
    }

    @Override
    public void compact() throws IOException {
        lazyInit();
        // engine.forceMerge(); // FIXME: Add to engine if needed
    }

    @Override
    public boolean isCompacting() {
        return false; // FIXME: engine should report this
    }

    @Override
    public List<Integer> getFileIds() {
        return Collections.singletonList(fileId); // Main metadata file ID
    }

    @Override
    public int getPageSize() {
         return -1; // Not page-based like ArcadeDB native
    }

    @Override
    public void setPageSize(int pageSize) {
        // No-op for Lucene
    }

    @Override
    public byte getNullStrategy() {
        return definition != null ? definition.getNullStrategy().getValue() : Index.NULL_STRATEGY.ERROR.getValue();
    }

    @Override
    public void setNullStrategy(byte nullStrategy) {
        // Usually immutable
    }

    @Override
    public void set(TransactionContext tx, Object[] keys, RID[] rids) throws IndexException {
        lazyInit();
        // This is for unique indexes usually. Lucene full-text is not typically unique.
        // If used, it implies key -> RID mapping.
        // For Lucene, it's document (derived from RID's record) -> indexed.
        // This method needs careful interpretation for Lucene.
        // Assuming keys[0] is the "key" to index (could be a document itself or fields)
        // and rids[0] is the value.
        if (keys == null || keys.length == 0 || rids == null || rids.length == 0) {
            throw new IndexException("Keys and RIDs must be provided for Lucene set operation for index '" + getName() + "'.");
        }
        // Engine methods (put, remove) were refactored to take TransactionContext directly.
        engine.put(tx, keys[0], rids[0]);
    }

    @Override
    public void remove(TransactionContext tx, Object[] keys, Identifiable rid) throws IndexException {
        lazyInit();
        if (keys == null || keys.length == 0) {
             throw new IndexException("Keys must be provided for Lucene remove operation for index '" + getName() + "'.");
        }
        // Engine methods (put, remove) were refactored to take TransactionContext directly.
        if (rid != null) {
            engine.remove(tx, keys[0], rid);
        } else {
            engine.remove(tx, keys[0]); // Remove all documents matching key
        }
    }

    @Override
    public void remove(TransactionContext tx, Object[] keys) throws IndexException {
        remove(tx, keys, null); // Remove all RIDs associated with these keys
    }

    @Override
    public IndexCursor range(boolean ascendingOrder) {
         throw new UnsupportedOperationException("Range queries without keys are not directly supported. Use a match_all query.");
    }

    @Override
    public IndexCursor range(boolean ascendingOrder, Object[] beginKeys, boolean beginKeysIncluded, Object[] endKeys, boolean endKeysIncluded, int limit, int skip) {
        throw new UnsupportedOperationException("Range queries are not directly supported by Lucene full-text index in this manner. Use Lucene query syntax.");
    }

    @Override
    public int getAssociatedBucketId() {
        if (definition == null) return -1;
        List<Integer> bucketIds = definition.getBucketIds();
        return bucketIds != null && !bucketIds.isEmpty() ? bucketIds.get(0) : -1;
    }

    // --- Lucene Specific Accessors ---
    public IndexSearcher searcher() {
        lazyInit();
        return engine.searcher();
    }

    public Analyzer indexAnalyzer() {
        lazyInit();
        return engine.indexAnalyzer();
    }

    public Analyzer queryAnalyzer() {
        lazyInit();
        return engine.queryAnalyzer();
    }
}
