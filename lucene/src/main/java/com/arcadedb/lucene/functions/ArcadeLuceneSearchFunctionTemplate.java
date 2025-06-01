package com.arcadedb.lucene.functions;

import com.arcadedb.database.Identifiable; // Changed
import com.arcadedb.document.Document; // Changed
import com.arcadedb.lucene.collections.LuceneResultSet; // FIXME: Needs refactoring
import com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex; // FIXME: Needs refactoring
import com.arcadedb.query.sql.executor.CommandContext; // Changed
import com.arcadedb.query.sql.executor.Result; // Changed
import com.arcadedb.query.sql.function.SQLFunction; // Standard ArcadeDB SQLFunction if SQLFunctionAbstract is not public or is different
import com.arcadedb.query.sql.parser.BinaryCompareOperator; // Changed
import com.arcadedb.query.sql.parser.Expression; // Changed
import com.arcadedb.query.sql.parser.FromClause; // Changed
import java.util.Map;

/** Created by frank on 25/05/2017. */
// Changed base class and removed IndexableSQLFunction interface
public abstract class ArcadeLuceneSearchFunctionTemplate implements SQLFunction {

  protected final String name;

  public ArcadeLuceneSearchFunctionTemplate(final String name) {
    this.name = name;
    // Parameter count checks will be done in each concrete class's execute method
  }

  @Override
  public String getName() {
    return name;
  }

  // The following methods are from the old IndexableSQLFunction interface and will be removed.
  // If ArcadeDB has a new way for functions to declare index usability, that would be a separate implementation.
  // public abstract boolean canExecuteInline(...);
  // public abstract boolean allowsIndexedExecution(...);
  // public abstract boolean shouldExecuteAfterSearch(...);
  // public abstract long estimate(...);
  // public abstract Iterable<Identifiable> searchFromTarget(...); // This logic moves into execute

  // The execute method is abstract in SQLFunction and must be implemented by concrete subclasses.
  // public abstract Object execute(Object self, Identifiable currentRecord, Object currentResult, Object[] params, CommandContext context);

  protected Document getMetadata(Expression metadataExpression, CommandContext ctx) {
    if (metadataExpression == null) return new Document(ctx.getDatabase());
    final Object md = metadataExpression.execute((Result) null, ctx);
    if (md instanceof Document) {
      return (Document) md;
    } else if (md instanceof Map) {
      return new Document().fromMap((Map<String, ?>) md); // Changed ODocument
    } else if (md instanceof String) {
      try {
        return new Document().fromJSON((String) md); // Changed ODocument
      } catch (Exception e) {
        // It might not be a JSON string, but the raw metadata string itself (e.g. analyzer class name)
        // This part needs careful review based on how metadata is actually passed and used.
        // For now, returning a document with a field containing the string.
        Document doc = new Document();
        doc.set("metadata", (String) md); // FIXME: Review this fallback for non-JSON metadata strings
        return doc;
      }
    } else if (metadata != null) {
       // Fallback if metadata is not null but not a recognized type, try its string representation as JSON
      try {
        return new Document().fromJSON(metadata.toString()); // Changed ODocument
      } catch (Exception e) {
        Document doc = new Document();
        doc.set("metadata", metadata.toString()); // FIXME: Review this fallback
        return doc;
      }
    }
    return new Document(); // Empty document if null or unparseable
  }

  // Changed OLuceneFullTextIndex, OFromClause, OCommandContext, OExpression
  protected abstract ArcadeLuceneFullTextIndex searchForIndex( // FIXME
      FromClause target, CommandContext ctx, Expression... args);
}
