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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 15/01/2017. */
public class OLuceneSearchOnFieldsFunction extends OLuceneSearchFunctionTemplate {

  public static final String NAME = "search_fields";

  public OLuceneSearchOnFieldsFunction() {
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

    if (!result.getElement().isPresent()) return false;
    OElement element = result.getElement().get();
    if (!element.getSchemaType().isPresent()) return false;
    String className = element.getSchemaType().get().getName();
    List<String> fieldNames = (List<String>) params[0];

    OLuceneFullTextIndex index = searchForIndex(className, ctx, fieldNames);

    if (index == null) return false;

    String query;
    if (params[1] == null) {
      query = null;
    } else {
      query = (String) params[1].toString();
    }

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

    if (params.length == 3) {
      return new ODocument().fromMap((Map<String, ?>) params[2]);
    }

    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX( indexName, [ metdatada {} ] )";
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
    Object query = expression.execute((OResult) null, ctx);
    if (index != null) {

      ODocument meta = getMetadata(args, ctx);
      Set<OIdentifiable> luceneResultSet;
      try (Stream<ORID> rids =
          index
              .getInternal()
              .getRids(
                  new OLuceneKeyAndMetadata(
                      new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), meta))) {
        luceneResultSet = rids.collect(Collectors.toSet());
      }

      return luceneResultSet;
    }
    throw new RuntimeException();
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
    List<String> fieldNames = (List<String>) args[0].execute((OResult) null, ctx);
    OFromItem item = target.getItem();
    String className = item.getIdentifier().getStringValue();

    return searchForIndex(className, ctx, fieldNames);
  }

  private OLuceneFullTextIndex searchForIndex(
      String className, OCommandContext ctx, List<String> fieldNames) {
    OMetadataInternal dbMetadata =
        (OMetadataInternal) ctx.getDatabase().activateOnCurrentThread().getMetadata();

    List<OLuceneFullTextIndex> indices =
        dbMetadata.getImmutableSchemaSnapshot().getClass(className).getIndexes().stream()
            .filter(idx -> idx instanceof OLuceneFullTextIndex)
            .map(idx -> (OLuceneFullTextIndex) idx)
            .filter(idx -> intersect(idx.getDefinition().getFields(), fieldNames))
            .collect(Collectors.toList());

    if (indices.size() > 1) {
      throw new IllegalArgumentException(
          "too many indices matching given field name: " + String.join(",", fieldNames));
    }

    return indices.size() == 0 ? null : indices.get(0);
  }

  public <T> List<T> intersection(List<T> list1, List<T> list2) {
    List<T> list = new ArrayList<T>();

    for (T t : list1) {
      if (list2.contains(t)) {
        list.add(t);
      }
    }

    return list;
  }

  public <T> boolean intersect(List<T> list1, List<T> list2) {

    for (T t : list1) {
      if (list2.contains(t)) {
        return true;
      }
    }

    return false;
  }
}
