package com.arcadedb.lucene.functions;

import static com.arcadedb.lucene.functions.OLuceneFunctionsUtils.getOrCreateMemoryIndex;

import com.arcadedb.lucene.builder.OLuceneQueryBuilder;
import com.arcadedb.lucene.collections.OLuceneCompositeKey;
import com.arcadedb.lucene.index.OLuceneFullTextIndex;
import com.arcadedb.lucene.query.OLuceneKeyAndMetadata;
import com.arcadedb.database.OCommandContext;
import com.arcadedb.database.OIdentifiable;
import com.arcadedb.database.id.ORID;
import com.arcadedb.database.metadata.OMetadataInternal;
import com.arcadedb.database.record.OElement;
import com.arcadedb.database.record.impl.ODocument;
import com.arcadedb.database.sql.executor.OResult;
import com.arcadedb.database.sql.executor.OResultInternal;
import com.arcadedb.database.sql.parser.OBinaryCompareOperator;
import com.arcadedb.database.sql.parser.OExpression;
import com.arcadedb.database.sql.parser.OFromClause;
import com.arcadedb.database.sql.parser.OFromItem;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 15/01/2017. */
public class OLuceneSearchOnClassFunction extends OLuceneSearchFunctionTemplate {

  public static final String NAME = "search_class";

  public OLuceneSearchOnClassFunction() {
    super(NAME, 1, 2);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean canExecuteInline(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return true;
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] params,
      OCommandContext ctx) {

    OResult result;
    if (iThis instanceof OResult) {
      result = (OResult) iThis;
    } else {
      result = new OResultInternal((OIdentifiable) iThis);
    }

    if (!result.getElement().isPresent()) return false;
    OElement element = result.getElement().get();
    if (!element.getSchemaType().isPresent()) return false;

    String className = element.getSchemaType().get().getName();

    OLuceneFullTextIndex index = searchForIndex(ctx, className);

    if (index == null) return false;

    String query = (String) params[0];

    MemoryIndex memoryIndex = getOrCreateMemoryIndex(ctx);

    List<Object> key =
        index.getDefinition().getFields().stream()
            .map(s -> element.getProperty(s))
            .collect(Collectors.toList());

    for (IndexableField field : index.buildDocument(key, iCurrentRecord).getFields()) {
      memoryIndex.addField(field, index.indexAnalyzer());
    }

    ODocument metadata = getMetadata(params);
    OLuceneKeyAndMetadata keyAndMetadata =
        new OLuceneKeyAndMetadata(
            new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata);

    return memoryIndex.search(index.buildQuery(keyAndMetadata)) > 0.0f;
  }

  private ODocument getMetadata(Object[] params) {

    if (params.length == 2) {
      return new ODocument().fromMap((Map<String, ?>) params[1]);
    }

    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX( indexName, [ metdatada {} ] )";
  }

  @Override
  public boolean filterResult() {
    return true;
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = searchForIndex(target, ctx);

    OExpression expression = args[0];
    String query = (String) expression.execute((OResult) null, ctx);

    if (index != null) {

      ODocument metadata = getMetadata(args, ctx);

      List<OIdentifiable> luceneResultSet;
      try (Stream<ORID> rids =
          index
              .getInternal()
              .getRids(
                  new OLuceneKeyAndMetadata(
                      new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata))) {
        luceneResultSet = rids.collect(Collectors.toList());
      }

      return luceneResultSet;
    }
    return Collections.emptySet();
  }

  private ODocument getMetadata(OExpression[] args, OCommandContext ctx) {
    if (args.length == 2) {
      return getMetadata(args[1], ctx);
    }
    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  protected OLuceneFullTextIndex searchForIndex(
      OFromClause target, OCommandContext ctx, OExpression... args) {
    OFromItem item = target.getItem();

    String className = item.getIdentifier().getStringValue();

    return searchForIndex(ctx, className);
  }

  private OLuceneFullTextIndex searchForIndex(OCommandContext ctx, String className) {
    OMetadataInternal dbMetadata =
        (OMetadataInternal) ctx.getDatabase().activateOnCurrentThread().getMetadata();

    List<OLuceneFullTextIndex> indices =
        dbMetadata.getImmutableSchemaSnapshot().getClass(className).getIndexes().stream()
            .filter(idx -> idx instanceof OLuceneFullTextIndex)
            .map(idx -> (OLuceneFullTextIndex) idx)
            .collect(Collectors.toList());

    if (indices.size() > 1) {
      throw new IllegalArgumentException("too many full-text indices on given class: " + className);
    }

    return indices.size() == 0 ? null : indices.get(0);
  }
}
