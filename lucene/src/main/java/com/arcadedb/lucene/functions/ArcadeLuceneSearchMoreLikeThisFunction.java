package com.arcadedb.lucene.functions;

import com.arcadedb.database.Database; // Changed ODatabaseSession to Database
import com.arcadedb.database.DatabaseContext; // For context access
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID; // Changed
import com.arcadedb.document.Document; // Changed
import com.arcadedb.document.Element; // Changed
import com.arcadedb.exception.ArcadeDBException; // Changed
import com.arcadedb.index.Index; // Changed
import com.arcadedb.lucene.collections.LuceneCompositeKey; // FIXME: Needs refactoring
import com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex; // FIXME: Needs refactoring
import com.arcadedb.lucene.index.ArcadeLuceneIndexType; // For RID field name
import com.arcadedb.lucene.query.LuceneKeyAndMetadata; // FIXME: Needs refactoring
import com.arcadedb.query.sql.executor.CommandContext; // Changed
import com.arcadedb.query.sql.executor.Result; // Changed
import com.arcadedb.query.sql.function.IndexableSQLFunction; // Assuming
import com.arcadedb.query.sql.function.SQLFunctionAbstract; // Assuming
import com.arcadedb.query.sql.parser.BinaryCompareOperator; // Changed
import com.arcadedb.query.sql.parser.Expression; // Changed
import com.arcadedb.query.sql.parser.FromClause; // Changed
import com.arcadedb.query.sql.parser.FromItem; // Changed
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.queryparser.classic.QueryParser; // Used for escape
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery; // Directly use BooleanQuery.Builder
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/** Created by frank on 15/01/2017. */
public class ArcadeLuceneSearchMoreLikeThisFunction extends ArcadeLuceneSearchFunctionTemplate // Changed base
    implements IndexableSQLFunction { // Assuming from template

  private static final Logger logger =
      Logger.getLogger(ArcadeLuceneSearchMoreLikeThisFunction.class.getName()); // Changed

  public static final String NAME = "search_more_like_this"; // Changed name

  public ArcadeLuceneSearchMoreLikeThisFunction() {
    super(NAME, 1, 2); // params: rids, [metadata]
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Object execute( // FIXME: Signature might change
      Object iThis,
      Identifiable iCurrentRecord, // Changed
      Object iCurrentResult,
      Object[] params,
      CommandContext ctx) { // Changed

    // This function's logic in OrientDB was to check if iCurrentRecord is similar to records identified by RIDs in params[0].
    // This seems more like a filter for a WHERE clause rather than a direct result-producing function.
    // The return type 'boolean' suggests this.

    if (!(iCurrentRecord instanceof Document)) { // Changed
      return false;
    }
    String className = ((Document) iCurrentRecord).getTypeName(); // Changed
    ArcadeLuceneFullTextIndex index = this.searchForIndex(ctx, className); // FIXME

    if (index == null) return false; // Cannot perform MLT without an index

    IndexSearcher searcher = index.searcher(); // FIXME
    if (searcher == null) return false;

    Document metadata = getMetadataDoc(params, 1); // metadata is params[1] // Changed

    List<String> ridsAsString = parseRidsObj(ctx, params[0]);
    if (ridsAsString.isEmpty()) return false;

    List<Identifiable> others = // Changed ORecord to Identifiable
        ridsAsString.stream()
            .map(ridStr -> (Identifiable) new RID(ctx.getDatabase(), ridStr)) // Changed ORecordId
            .map(id -> ctx.getDatabase().lookupByRID(id.getIdentity(), true).getRecord()) // Load record // Changed
            .filter(r -> r instanceof Element) // Ensure it's an element
            .collect(Collectors.toList());

    MoreLikeThis mlt = buildMoreLikeThis(index, searcher, metadata); // FIXME

    BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder(); // Changed

    // The MLT query should be built against the content of 'others'
    // And then we check if iCurrentRecord matches this mltQuery.
    // This is different from how 'searchFromTarget' works.

    // This part seems to generate a query based on the 'others' documents
    addLikeQueries(others, mlt, queryBuilder, ctx.getDatabase()); // Changed

    Query mltQuery = queryBuilder.build();
    if (mltQuery.toString().isEmpty()) { // No terms generated if documents are empty or too common/rare
        return false;
    }

    // Now, check if iCurrentRecord matches the mltQuery.
    // This requires indexing iCurrentRecord in-memory.
    MemoryIndex memoryIndex = ArcadeLuceneFunctionsUtils.getOrCreateMemoryIndex(ctx);
    org.apache.lucene.document.Document luceneDoc = index.buildDocument(null, iCurrentRecord); // FIXME: Key might be needed or different buildDocument signature
    if (luceneDoc != null) {
        for (org.apache.lucene.index.IndexableField field : luceneDoc.getFields()) {
            memoryIndex.addField(field.name(), field.stringValue(), index.indexAnalyzer()); // FIXME
        }
    } else {
        return false;
    }
    return memoryIndex.search(mltQuery) > 0.0f;
  }

  @Override
  public String getSyntax() {
    return NAME + "( <rids>, [ <metadata> ] )"; // Corrected syntax
  }

  @Override
  public Iterable<Identifiable> searchFromTarget( // Changed
      FromClause target, // Changed
      BinaryCompareOperator operator, // Changed
      Object rightValue,
      CommandContext ctx, // Changed
      Expression... args) { // Changed

    ArcadeLuceneFullTextIndex index = this.searchForIndex(target, ctx, args); // FIXME

    if (index == null) return Collections.emptySet();

    IndexSearcher searcher = index.searcher(); // FIXME
    if (searcher == null) return Collections.emptySet();


    Expression ridExpression = args[0];
    Document metadata = getMetadataFromExpression(args, ctx, 1); // metadata is args[1] // Changed

    List<String> ridsAsString = parseRids(ctx, ridExpression);
    if (ridsAsString.isEmpty()) return Collections.emptySet();

    List<Identifiable> others = // Changed
        ridsAsString.stream()
            .map(ridStr -> (Identifiable) new RID(ctx.getDatabase(), ridStr)) // Changed
            .map(id -> ctx.getDatabase().lookupByRID(id.getIdentity(), true).getRecord()) // Load record // Changed
            .filter(r -> r instanceof Element)
            .collect(Collectors.toList());

    MoreLikeThis mlt = buildMoreLikeThis(index, searcher, metadata); // FIXME

    BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder(); // Changed

    excludeOtherFromResults(ridsAsString, queryBuilder); // Keep input RIDs out of results

    addLikeQueries(others, mlt, queryBuilder, ctx.getDatabase()); // Changed

    Query mltQuery = queryBuilder.build();
    if (mltQuery.toString().isEmpty()) return Collections.emptySet();


    // Execute the mltQuery against the main index
    // FIXME: index.getInternal().getRids() needs to be replaced
    // FIXME: LuceneCompositeKey and LuceneKeyAndMetadata need refactoring
    // This part is highly dependent on how ArcadeLuceneFullTextIndex exposes search capabilities
    try (Stream<RID> rids = // Changed
        index
            .getAssociatedIndex() // Assuming
            .getRids( // This method might not exist
                new LuceneKeyAndMetadata( // FIXME
                    new LuceneCompositeKey(Arrays.asList(mltQuery.toString())).setContext(ctx), // FIXME
                    metadata))) {
      return rids.map(rid -> (Identifiable) rid).collect(Collectors.toSet()); // Changed
    } catch (Exception e) {
        logger.log(Level.SEVERE, "Error executing MoreLikeThis query via getRids", e);
        return Collections.emptySet();
    }
  }

  private List<String> parseRids(CommandContext ctx, Expression expression) { // Changed
    Object expResult = expression.execute((Result) null, ctx); // Changed
    return parseRidsObj(ctx, expResult);
  }

  private List<String> parseRidsObj(CommandContext ctx, Object expResult) { // Changed
    if (expResult instanceof Identifiable) { // Changed
      return Collections.singletonList(((Identifiable) expResult).getIdentity().toString());
    }

    Iterator<?> iter; // Wildcard for iterator type
    if (expResult instanceof Iterable) {
      iter = ((Iterable<?>) expResult).iterator();
    } else if (expResult instanceof Iterator) {
      iter = (Iterator<?>) expResult;
    } else {
      return Collections.emptyList();
    }

    List<String> rids = new ArrayList<>();
    while (iter.hasNext()) {
      Object item = iter.next();
      if (item instanceof Result) { // Changed
        if (((Result) item).isElement()) {
          ((Result) item).getIdentity().ifPresent(id -> rids.add(id.toString())); // Changed
        } else {
          Set<String> properties = ((Result) item).getPropertyNames();
          if (properties.size() == 1) {
            Object val = ((Result) item).getProperty(properties.iterator().next());
            if (val instanceof Identifiable) { // Changed
              rids.add(((Identifiable) val).getIdentity().toString());
            }
          }
        }
      } else if (item instanceof Identifiable) { // Changed
        rids.add(((Identifiable) item).getIdentity().toString());
      }
    }
    return rids;
  }

  private Document getMetadataDoc(Object[] params, int metadataParamIndex) { // Changed
    if (params.length > metadataParamIndex) {
      if (params[metadataParamIndex] instanceof Map) {
        return new Document().fromMap((Map<String, ?>) params[metadataParamIndex]);
      } else if (params[metadataParamIndex] instanceof String) {
        return new Document().fromJSON((String) params[metadataParamIndex]);
      }
      return new Document().fromJSON(params[metadataParamIndex].toString());
    }
    return new Document(); // Empty if not present
  }

  private Document getMetadataFromExpression(Expression[] args, CommandContext ctx, int metadataParamIndex) { // Changed
    if (args.length > metadataParamIndex) {
      return getMetadata(args[metadataParamIndex], ctx); // Calls method from ArcadeLuceneSearchFunctionTemplate
    }
    return new Document(); // Empty if not present
  }


  private MoreLikeThis buildMoreLikeThis( // Changed
      ArcadeLuceneFullTextIndex index, IndexSearcher searcher, Document metadata) { // FIXME

    try {
      MoreLikeThis mlt = new MoreLikeThis(searcher.getIndexReader());

      mlt.setAnalyzer(index.queryAnalyzer()); // FIXME

      // FIXME: index.getDefinition() might be different
      mlt.setFieldNames(
          Optional.ofNullable(metadata.<List<String>>getProperty("fieldNames"))
              .orElse(index.getDefinition().getFields())
              .toArray(new String[] {}));

      mlt.setMaxQueryTerms(
          Optional.ofNullable(metadata.<Integer>getProperty("maxQueryTerms"))
              .orElse(MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));
      // ... (rest of MoreLikeThis setters, ensure getProperty types match)
       mlt.setMinTermFreq(
          Optional.ofNullable(metadata.<Integer>getProperty("minTermFreq"))
              .orElse(MoreLikeThis.DEFAULT_MIN_TERM_FREQ));
      mlt.setMaxDocFreq(
          Optional.ofNullable(metadata.<Integer>getProperty("maxDocFreq"))
              .orElse(MoreLikeThis.DEFAULT_MAX_DOC_FREQ));
      mlt.setMinDocFreq(
          Optional.ofNullable(metadata.<Integer>getProperty("minDocFreq"))
              .orElse(MoreLikeThis.DEFAULT_MIN_DOC_FREQ)); // Corrected from DEFAULT_MAX_DOC_FREQ
      mlt.setBoost(
          Optional.ofNullable(metadata.<Boolean>getProperty("boost"))
              .orElse(MoreLikeThis.DEFAULT_BOOST));
      mlt.setBoostFactor(
          Optional.ofNullable(metadata.<Float>getProperty("boostFactor")).orElse(1f));
      mlt.setMaxWordLen(
          Optional.ofNullable(metadata.<Integer>getProperty("maxWordLen"))
              .orElse(MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));
      mlt.setMinWordLen(
          Optional.ofNullable(metadata.<Integer>getProperty("minWordLen"))
              .orElse(MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));
      // setMaxNumTokensParsed was removed in later Lucene versions, check alternatives if needed.
      // mlt.setMaxNumTokensParsed(
      // Optional.ofNullable(metadata.<Integer>getProperty("maxNumTokensParsed"))
      // .orElse(MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));
      mlt.setStopWords(
          (Set<?>)
              Optional.ofNullable(metadata.get("stopWords")) // Simpler get for Set
                  .orElse(MoreLikeThis.DEFAULT_STOP_WORDS));


      return mlt;
    } catch (IOException e) {
      throw ArcadeDBException.wrapException(new ArcadeDBException("Lucene IO Exception"), e); // Changed
    }
  }

  private void addLikeQueries( // Changed
      List<Identifiable> others, MoreLikeThis mlt, BooleanQuery.Builder queryBuilder, Database database) { // Changed
    others.stream()
        .filter(id -> id instanceof Element) // ensure it's an element to get properties
        .map(id -> (Element) id)
        .forEach(
            element ->
                Arrays.stream(mlt.getFieldNames()) // These are the fields to check for similarity
                    .forEach(
                        fieldName -> {
                          Object propertyValue = element.getProperty(fieldName);
                          if (propertyValue != null) {
                            try {
                              // MoreLikeThis.like() can take a String directly for a field's content
                              Query fieldQuery = mlt.like(fieldName, new StringReader(propertyValue.toString()));
                              if (!fieldQuery.toString().isEmpty()) // Check if anything was generated
                                queryBuilder.add(fieldQuery, Occur.SHOULD);
                            } catch (IOException e) {
                              logger.log(Level.SEVERE, "Error during Lucene MoreLikeThis query generation for field " + fieldName, e);
                            }
                          }
                        }));
  }

  private void excludeOtherFromResults(List<String> ridsAsString, BooleanQuery.Builder queryBuilder) { // Changed
    ridsAsString.stream()
        .forEach(
            rid ->
                queryBuilder.add( // Use ArcadeLuceneIndexType.RID for consistency
                    new TermQuery(new Term(ArcadeLuceneIndexType.RID, QueryParser.escape(rid))), Occur.MUST_NOT));
  }

  // searchForIndex from OLuceneSearchFunctionTemplate should be used or overridden if different logic needed for target.
  // The private helpers here were specific to how OLuceneSearchMoreLikeThisFunction determined its index.
  // For now, relying on the overridden searchForIndex from ArcadeLuceneSearchFunctionTemplate.
  // If this function *always* uses class name from context (iThis) for 'execute' and target for 'searchFromTarget',
  // then the template's searchForIndex might need to be made non-abstract or this class needs its own.
  // The original OLuceneSearchMoreLikeThisFunction had its own searchForIndex.

  @Override
  protected ArcadeLuceneFullTextIndex searchForIndex( // Changed
      FromClause target, CommandContext ctx, Expression... args) { // FIXME
    FromItem item = target.getItem(); // Changed
    Identifier identifier = item.getIdentifier(); // Changed
    String className = identifier.getStringValue();
    return searchForIndex(ctx, className); // Calls private helper
  }

  private ArcadeLuceneFullTextIndex searchForIndex(CommandContext ctx, String className) { // Changed
    DatabaseInternal database = (DatabaseInternal) ((DatabaseContext) ctx).getDatabase(); // FIXME: Verify
    // database.activateOnCurrentThread(); // May not be needed

    Schema schema = database.getSchema(); // Changed
    DocumentType docType = schema.getType(className); // Changed

    if (docType == null) {
      return null;
    }

    List<ArcadeLuceneFullTextIndex> indices = // Changed
        docType.getIndexes(true).stream()
            .filter(idx -> idx instanceof ArcadeLuceneFullTextIndex) // FIXME
            .map(idx -> (ArcadeLuceneFullTextIndex) idx) // FIXME
            .collect(Collectors.toList());

    if (indices.size() > 1) {
      // Consider if a more specific index selection is needed, e.g. one covering certain fields if provided in metadata
      throw new IllegalArgumentException("Too many full-text Lucene indices on class: " + className + ". Disambiguate or configure.");
    }
    return indices.size() == 0 ? null : indices.get(0);
  }


  // estimate, canExecuteInline, allowsIndexedExecution, shouldExecuteAfterSearch
  // are inherited from ArcadeLuceneSearchFunctionTemplate.
  // Their default implementations in the template might need review for this specific function's behavior.
  // E.g., allowsIndexedExecution for MLT depends on finding *an* index on the class to get an IndexReader.
}
