package com.arcadedb.lucene.functions;

import com.arcadedb.lucene.builder.OLuceneQueryBuilder;
import com.arcadedb.lucene.collections.OLuceneCompositeKey;
import com.arcadedb.lucene.index.OLuceneFullTextIndex;
import com.arcadedb.lucene.query.OLuceneKeyAndMetadata;
import com.arcadedb.database.OCommandContext;
import com.arcadedb.database.ODatabaseDocumentInternal;
import com.arcadedb.database.OIdentifiable;
import com.arcadedb.database.id.ORID;
import com.arcadedb.database.index.OIndex;
import com.arcadedb.database.record.impl.ODocument;
import com.arcadedb.database.sql.executor.OResult;
import com.arcadedb.database.sql.executor.OResultInternal;
import com.arcadedb.database.sql.parser.OBinaryCompareOperator;
import com.arcadedb.database.sql.parser.OExpression;
import com.arcadedb.database.sql.parser.OFromClause;
import com.arcadedb.database.sql.parser.OFromItem;
import com.arcadedb.database.sql.parser.OIdentifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 15/01/2017. */
public class OLuceneSearchOnIndexFunction extends OLuceneSearchFunctionTemplate {

  public static final String MEMORY_INDEX = "_memoryIndex";

  public static final String NAME = "search_index";

  public OLuceneSearchOnIndexFunction() {
    super(NAME, 2, 3);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] params,
      OCommandContext ctx) {
    if (iThis instanceof ORID) {
      iThis = ((ORID) iThis).getRecord();
    }
    if (iThis instanceof OIdentifiable) {
      iThis = new OResultInternal((OIdentifiable) iThis);
    }
    OResult result = (OResult) iThis;

    String indexName = (String) params[0];

    OLuceneFullTextIndex index = searchForIndex(ctx, indexName);

    if (index == null) return false;

    String query = (String) params[1];

    MemoryIndex memoryIndex = getOrCreateMemoryIndex(ctx);

    List<Object> key =
        index.getDefinition().getFields().stream()
            .map(s -> result.getProperty(s))
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

    if (params.length == 3) {
      return new ODocument().fromMap((Map<String, ?>) params[2]);
    }

    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  private MemoryIndex getOrCreateMemoryIndex(OCommandContext ctx) {
    MemoryIndex memoryIndex = (MemoryIndex) ctx.getVariable(MEMORY_INDEX);
    if (memoryIndex == null) {
      memoryIndex = new MemoryIndex();
      ctx.setVariable(MEMORY_INDEX, memoryIndex);
    }

    memoryIndex.reset();
    return memoryIndex;
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

    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);

    OExpression expression = args[1];
    String query = (String) expression.execute((OResult) null, ctx);
    if (index != null && query != null) {

      ODocument meta = getMetadata(args, ctx);

      List<OIdentifiable> luceneResultSet;
      try (Stream<ORID> rids =
          index
              .getInternal()
              .getRids(
                  new OLuceneKeyAndMetadata(
                      new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), meta))) {
        luceneResultSet = rids.collect(Collectors.toList());
      }

      return luceneResultSet;
    }
    return Collections.emptyList();
  }

  private ODocument getMetadata(OExpression[] args, OCommandContext ctx) {
    if (args.length == 3) {
      return getMetadata(args[2], ctx);
    }
    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  protected OLuceneFullTextIndex searchForIndex(
      OFromClause target, OCommandContext ctx, OExpression... args) {

    OFromItem item = target.getItem();
    OIdentifier identifier = item.getIdentifier();
    return searchForIndex(identifier.getStringValue(), ctx, args);
  }

  private OLuceneFullTextIndex searchForIndex(
      String className, OCommandContext ctx, OExpression... args) {

    String indexName = (String) args[0].execute((OResult) null, ctx);

    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, className, indexName);

    if (index != null && index.getInternal() instanceof OLuceneFullTextIndex) {
      return (OLuceneFullTextIndex) index;
    }

    return null;
  }

  private OLuceneFullTextIndex searchForIndex(OCommandContext ctx, String indexName) {
    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);

    if (index != null && index.getInternal() instanceof OLuceneFullTextIndex) {
      return (OLuceneFullTextIndex) index;
    }

    return null;
  }

  @Override
  public Object getResult(OCommandContext ctx) {
    return super.getResult(ctx);
  }
}
