package com.arcadedb.lucene.functions;

// import static com.arcadedb.lucene.OLuceneCrossClassIndexFactory.LUCENE_CROSS_CLASS; // FIXME Define or import
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID; // Changed
import com.arcadedb.document.Document; // Changed
import com.arcadedb.index.Index; // Changed
import com.arcadedb.lucene.builder.LuceneQueryBuilder; // FIXME: Needs refactoring
import com.arcadedb.lucene.collections.LuceneCompositeKey; // FIXME: Needs refactoring
import com.arcadedb.lucene.engine.ArcadeLuceneCrossClassIndexEngine; // Changed
import com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex; // FIXME: Needs refactoring (used as type in old code, though engine is likely target)
import com.arcadedb.lucene.query.LuceneKeyAndMetadata; // FIXME: Needs refactoring
import com.arcadedb.query.sql.executor.CommandContext; // Changed
import com.arcadedb.query.sql.executor.Result; // Changed
import com.arcadedb.query.sql.parser.BinaryCompareOperator; // Changed
import com.arcadedb.query.sql.parser.Expression; // Changed
import com.arcadedb.query.sql.parser.FromClause; // Changed
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger; // Changed
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This function uses the CrossClassIndex to search documents across all the Lucene indexes defined in a database
 * <p>
 * Created by frank on 19/02/2016.
 */
public class ArcadeLuceneCrossClassSearchFunction extends ArcadeLuceneSearchFunctionTemplate { // Changed base class
  private static final Logger logger =
      Logger.getLogger(ArcadeLuceneCrossClassSearchFunction.class.getName()); // Changed

  public static final String NAME = "search_cross"; // Changed from SEARCH_CROSS
  private static final String LUCENE_CROSS_CLASS_ALGORITHM = "LUCENE_CROSS_CLASS"; // Placeholder

  public ArcadeLuceneCrossClassSearchFunction() {
    super(NAME, 1, 2); // query, [metadata]
  }

  // searchForIndex in the template expects args for index name. This class doesn't use that.
  // It finds a specific *kind* of index (cross class).
  // So, the searchForIndex from the template is not suitable.
  // This function might not be a good fit for ArcadeLuceneSearchFunctionTemplate if it cannot provide a single index.
  // However, if ArcadeLuceneCrossClassIndexEngine is treated as *the* index, it could work.

  @Override
  public Iterable<Identifiable> searchFromTarget( // Changed
      FromClause target, // Target is ignored by this function as it's cross-class
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx, // Changed
      Expression... args) { // Changed

    ArcadeLuceneCrossClassIndexEngine engine = getCrossClassEngine(ctx); // FIXME: Needs robust way to get this engine

    if (engine == null) {
        logger.warning("Lucene Cross Class Index Engine not found.");
        return Collections.emptySet();
    }

    Expression expression = args[0];
    String query = (String) expression.execute((Result) null, ctx); // Changed

    Document metadata = getMetadata(args, ctx, 1); // Changed, metadata is args[1]

    // The engine's 'get' method should return Iterable<Identifiable> or similar
    // FIXME: LuceneCompositeKey and LuceneKeyAndMetadata need refactoring
    Object result = engine.get(
        new LuceneKeyAndMetadata(
            new LuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata));

    if (result instanceof Iterable) {
        return (Iterable<Identifiable>) result;
    }
    return Collections.emptySet();
  }

  @Override
  public Object execute(
      Object iThis,
      Identifiable currentRecord, // Changed
      Object currentResult,
      Object[] params,
      CommandContext ctx) { // Changed

    ArcadeLuceneCrossClassIndexEngine engine = getCrossClassEngine(ctx); // FIXME

    if (engine == null) {
        logger.warning("Lucene Cross Class Index Engine not found for execute.");
        return Collections.emptySet();
    }

    String query = (String) params[0];
    Document metadata = getMetadata(params, 1); // Changed

    // FIXME: LuceneCompositeKey and LuceneKeyAndMetadata need refactoring
    Object result = engine.get(
        new LuceneKeyAndMetadata(
            new LuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata));

    return result;
  }

  private Document getMetadata(Expression[] args, CommandContext ctx, int metadataParamIndex) { // Changed
    if (args.length > metadataParamIndex) {
      // Assuming getMetadata from ArcadeLuceneSearchFunctionTemplate is suitable
      return super.getMetadata(args[metadataParamIndex], ctx);
    }
    // FIXME: LuceneQueryBuilder.EMPTY_METADATA
    return new Document(); // LuceneQueryBuilder.EMPTY_METADATA;
  }

  private Document getMetadata(Object[] params, int metadataParamIndex) { // Changed
    if (params.length > metadataParamIndex) {
      if (params[metadataParamIndex] instanceof Map) {
        return new Document().fromMap((Map<String, ?>) params[metadataParamIndex]);
      } else if (params[metadataParamIndex] instanceof String) {
        return new Document().fromJSON((String) params[metadataParamIndex]);
      } else if (params[metadataParamIndex] instanceof Document) {
        return (Document) params[metadataParamIndex];
      }
      // Fallback or error if type is not recognized
      try {
        return new Document().fromJSON(params[metadataParamIndex].toString());
      } catch (Exception e) {
        // ignore
      }
    }
    // FIXME: LuceneQueryBuilder.EMPTY_METADATA
    return new Document(); // LuceneQueryBuilder.EMPTY_METADATA;
  }

  // This method is problematic as the template expects an ArcadeLuceneFullTextIndex.
  // This function uses a different kind of engine.
  // Returning null tells the template that direct indexed execution (via that specific index type) is not possible.
  @Override
  protected ArcadeLuceneFullTextIndex searchForIndex( // FIXME: This signature might not be appropriate for this class
      FromClause target, CommandContext ctx, Expression... args) {
    return null; // This function doesn't use a single, standard Lucene full-text index from the target.
                 // It uses the ArcadeLuceneCrossClassIndexEngine.
  }

  // Helper to get the specific cross-class engine instance
  // This assumes there's a way to identify and retrieve this engine.
  // It might be registered with a specific name or type.
  private ArcadeLuceneCrossClassIndexEngine getCrossClassEngine(CommandContext ctx) { // FIXME
    DatabaseInternal database = (DatabaseInternal) ((DatabaseContext) ctx).getDatabase(); // FIXME: Verify
    Collection<? extends Index> indexes = database.getSchema().getIndexes();
    for (Index index : indexes) {
      // FIXME: Need a reliable way to identify the CrossClassEngine.
      // This could be by a specific name, or if the engine itself is registered as an Index.
      // The original code checked index.getAlgorithm().equalsIgnoreCase(LUCENE_CROSS_CLASS)
      // and then cast index.getInternal() to OLuceneFullTextIndex, which seems problematic as the
      // cross class engine is not a typical "full text index" on a specific class.
      // For now, assuming the ArcadeLuceneCrossClassIndexEngine might be registered as an Index itself
      // with a specific algorithm name.
      if (index.getAlgorithm().equalsIgnoreCase(LUCENE_CROSS_CLASS_ALGORITHM) && index instanceof ArcadeLuceneCrossClassIndexEngine) {
         return (ArcadeLuceneCrossClassIndexEngine) index;
      }
      // Alternative: if the engine is not an Index, how is it accessed?
      // Perhaps it's a global component or registered differently.
      // The original code `(OLuceneFullTextIndex) index.getInternal()` suggests the index itself was a shell.
    }
    // Fallback: Try to find an index whose *engine* is the cross-class one.
    // This is speculative.
     for (Index index : indexes) {
        if (index.getAssociatedIndex() instanceof ArcadeLuceneCrossClassIndexEngine) { // getAssociatedIndex might be getEngine()
             return (ArcadeLuceneCrossClassIndexEngine) index.getAssociatedIndex();
        }
     }
    return null;
  }


  @Override
  public String getSyntax() {
    // logger.debug("syntax"); // Logging in getSyntax is unusual
    return NAME + "('<lucene query>', [ <metadata> ])";
  }

  // Other overrides from OIndexableSQLFunction (estimate, canExecuteInline, etc.)
  // The original class had specific implementations for these.
  // If extending ArcadeLuceneSearchFunctionTemplate, these might be inherited or need specific overrides.
  // For now, relying on template's (which has FIXMEs) or needing specific ones here.

  @Override
  public long estimate(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) {
    // Cross-class estimation is complex. Returning a default or trying to get a count from the engine.
    ArcadeLuceneCrossClassIndexEngine engine = getCrossClassEngine(ctx);
    if (engine != null) {
        // FIXME: The engine might need a size estimation method
        // return engine.sizeEstimate(args...);
    }
    return super.estimate(target, operator, rightValue, ctx, args); // Fallback to template's estimate
  }

  @Override
  public boolean allowsIndexedExecution(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) {
    // This function *always* uses its specialized engine, so it's "indexed" in that sense.
    return getCrossClassEngine(ctx) != null;
  }
   @Override
  public boolean canExecuteInline(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) {
    return false; // Cross class search is likely too complex for simple inline execution
  }

  @Override
  public boolean shouldExecuteAfterSearch(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) {
    return false;
  }

}
