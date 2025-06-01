package com.arcadedb.lucene.query;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordId; // ArcadeDB RecordId for context
import com.arcadedb.index.IndexCursor;
import com.arcadedb.lucene.engine.LuceneIndexEngine; // Assumed engine interface
import com.arcadedb.lucene.index.ArcadeLuceneIndexType; // For RID field name
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document; // Lucene Document
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

public class LuceneIndexCursor implements IndexCursor {

    private static final Logger logger = Logger.getLogger(LuceneIndexCursor.class.getName());

    private final LuceneQueryContext queryContext;
    private final LuceneIndexEngine engine; // Engine for callbacks
    private final com.arcadedb.document.Document metadata; // ArcadeDB Document for query metadata

    private ScoreDoc[] scoreDocs;
    private IndexSearcher searcher;
    private int currentIndex = -1; // Before the first element
    private RID currentRID;
    private float currentScore;
    private Map<String, Object> currentProximityInfo; // For contextual data like highlights

    private TopDocs topDocs;


    public LuceneIndexCursor(LuceneQueryContext queryContext,
                             LuceneIndexEngine engine,
                             com.arcadedb.document.Document metadata) {
        this.queryContext = queryContext;
        this.engine = engine;
        this.metadata = metadata;
        this.searcher = queryContext.getSearcher(); // Get the potentially transactional searcher

        executeSearch();
    }

    // Constructor for when results (Set<Identifiable>) are already fetched, e.g. from engine.getInTx()
    // This is a simplified cursor that iterates over pre-fetched RIDs without scores or Lucene docs.
    private Iterator<Identifiable> preFetchedResultsIterator;
    private Identifiable currentPreFetched;
    private int preFetchedCount;

    public LuceneIndexCursor(Set<Identifiable> preFetchedResults) {
        this.queryContext = null; // Not applicable
        this.engine = null; // Not applicable
        this.metadata = null; // Not applicable
        if (preFetchedResults != null) {
            this.preFetchedResultsIterator = preFetchedResults.iterator();
            this.preFetchedCount = preFetchedResults.size();
        } else {
            this.preFetchedResultsIterator = Collections.emptyIterator();
            this.preFetchedCount = 0;
        }
    }


    private void executeSearch() {
        if (queryContext == null) return; // Should not happen if not using pre-fetched constructor

        try {
            int limit = queryContext.getContext() != null ? queryContext.getContext().getLimit() : Integer.MAX_VALUE;
            if (limit == -1) limit = Integer.MAX_VALUE; // SQL limit -1 means no limit

            if (queryContext.getSort() != null) {
                this.topDocs = searcher.search(queryContext.getQuery(), limit, queryContext.getSort());
            } else {
                this.topDocs = searcher.search(queryContext.getQuery(), limit);
            }
            this.scoreDocs = topDocs.scoreDocs;
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error executing Lucene search", e);
            this.scoreDocs = new ScoreDoc[0]; // Empty results on error
            this.topDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        }
    }

    @Override
    public Object[] getKeys() {
        // For Lucene, the "keys" are the search terms. This is not usually returned per document.
        // If queryContext.getQuery() is available, one could try to extract terms, but it's complex.
        if (currentRID != null) {
            // Could potentially store the query that led to this hit if needed.
            // For now, returning null as it's not a natural fit.
            return null;
        }
        throw new NoSuchElementException("No current element or keys not applicable");
    }

    @Override
    public Identifiable getRecord() {
        // In ArcadeDB, IndexCursor usually returns RIDs. The record is loaded by the caller.
        // If this cursor *must* return the full record, a DB lookup is needed.
        // For now, consistent with returning RID via next() and getRID().
        // This method could load and cache it if frequently used.
        if (currentRID != null && queryContext != null && queryContext.getContext() != null) {
            return queryContext.getContext().getDatabase().lookupByRID(currentRID, true);
        }
        if (currentPreFetched != null) {
            return currentPreFetched;
        }
        return null;
    }

    public RID getRID() {
        if (currentRID != null) {
            return currentRID;
        }
        if (currentPreFetched != null) {
            return currentPreFetched.getIdentity();
        }
        return null;
    }


    @Override
    public Map<String, Object> getProperties() {
        // This could return highlights and score if structured appropriately.
        // The currentProximityInfo is designed for this.
        return currentProximityInfo != null ? currentProximityInfo : Collections.emptyMap();
    }

    @Override
    public float getScore() { // Changed from int to float to match Lucene score
        return currentScore;
    }

    @Override
    public boolean hasNext() {
        if (preFetchedResultsIterator != null) {
            return preFetchedResultsIterator.hasNext();
        }
        if (scoreDocs == null) {
            return false;
        }
        return (currentIndex + 1) < scoreDocs.length;
    }

    @Override
    public Identifiable next() {
        if (preFetchedResultsIterator != null) {
            if (!preFetchedResultsIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            currentPreFetched = preFetchedResultsIterator.next();
            this.currentRID = currentPreFetched.getIdentity(); // Store RID
            this.currentScore = 1.0f; // Pre-fetched results usually don't carry Lucene score directly
            this.currentProximityInfo = Collections.singletonMap("$score", this.currentScore);
            return currentPreFetched; // Or just currentRID if API prefers that
        }

        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        currentIndex++;
        ScoreDoc scoreDoc = scoreDocs[currentIndex];
        try {
            // Using storedFields().document() is preferred in modern Lucene for retrieving stored fields.
            // searcher.doc(scoreDoc.doc) retrieves all (including non-stored if they were indexed in a certain way, but generally for stored).
            Document luceneDoc = searcher.storedFields().document(scoreDoc.doc);
            String ridString = luceneDoc.get(ArcadeLuceneIndexType.RID); // Use constant from ArcadeLuceneIndexType

            if (ridString == null) {
                // Fallback or try another RID field if there are multiple conventions (e.g. from older data)
                // For now, log and skip if primary RID field is missing.
                logger.log(Level.WARNING, "Lucene document " + scoreDoc.doc + " is missing RID field (" + ArcadeLuceneIndexType.RID + ")");
                // Try to advance to next valid document or return null/throw
                return next(); // Recursive call to try next, or could throw. Be careful with recursion.
            }

            Database currentDb = queryContext != null && queryContext.getContext() != null ? queryContext.getContext().getDatabase() : null;
            this.currentRID = new RID(currentDb, ridString); // Pass database if available for cluster info
            this.currentScore = scoreDoc.score;

            // Prepare contextual data (score, highlights)
            this.currentProximityInfo = new HashMap<>();
            this.currentProximityInfo.put("$score", this.currentScore);

            if (queryContext != null && queryContext.isHighlightingEnabled()) {
                if (engine != null && engine.queryAnalyzer() != null) { // Ensure we have an analyzer for highlighting
                    queryContext.setHighlightingAnalyzer(engine.queryAnalyzer()); // Use engine's query analyzer

                    // We need an IndexReader to pass to getHighlights if it needs one.
                    // The searcher in queryContext already has one.
                    IndexReader reader = queryContext.getSearcher().getIndexReader();
                    Map<String, String> highlights = queryContext.getHighlights(luceneDoc, reader);
                    if (highlights != null && !highlights.isEmpty()) {
                        this.currentProximityInfo.putAll(highlights);
                    }
                } else {
                    logger.warning("Highlighting enabled but no queryAnalyzer available from engine to set on LuceneQueryContext.");
                }
            }

            // The engine.onRecordAddedToResultSet callback is now less critical for highlights,
            // but can be kept if it serves other purposes (e.g. security, logging, complex context data).
            // For now, let's assume its primary highlight-related role is superseded.
            if (engine != null && queryContext != null) {
                 RecordId contextualRid = new RecordId(this.currentRID);
                 engine.onRecordAddedToResultSet(queryContext, contextualRid, luceneDoc, scoreDoc);
            }


            // IndexCursor traditionally returns Identifiable (which can be just the RID)
            // If the caller needs the full record, they call getRecord().
            return this.currentRID;

        } catch (IOException e) {
            throw new RuntimeException("Error fetching document from Lucene index", e);
        }
    }

    @Override
    public void close() {
        // Release Lucene resources if this cursor specifically acquired them.
        // If searcher is managed by engine (e.g. via SearcherManager),
        // this cursor typically doesn't close/release the searcher.
        scoreDocs = null;
        // searcher = null; // Don't nullify if it's shared from engine/queryContext
    }

    @Override
    public long getCount() { // Changed from size() to match typical usage for total hits
        if (preFetchedResultsIterator != null) {
            return preFetchedCount;
        }
        return topDocs != null && topDocs.totalHits != null ? topDocs.totalHits.value : 0;
    }

    @Override
    public long size() { // Kept for IndexCursor interface if it uses size() for current iteration count
        return getCount();
    }


    @Override
    public void setLimit(int limit) {
        // Limit should be applied during the search execution.
        throw new UnsupportedOperationException("Limit must be set before search execution via CommandContext or metadata.");
    }

    @Override
    public int getLimit() {
        // Return the limit that was applied to this cursor's search
        if (queryContext != null && queryContext.getContext() != null) {
            return queryContext.getContext().getLimit();
        }
        return -1;
    }

    @Override
    public boolean isPaginated() {
        // Lucene TopDocs inherently supports pagination if the search is re-executed with 'searchAfter'.
        // This simple cursor iterates a fixed set of top N docs. So, it's "paginated" in the sense
        // that it represents one page of results.
        return true;
    }
}
