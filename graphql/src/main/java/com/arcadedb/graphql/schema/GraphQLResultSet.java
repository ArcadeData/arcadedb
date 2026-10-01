/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.graphql.schema;

import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graphql.parser.AbstractField;
import com.arcadedb.graphql.parser.Argument;
import com.arcadedb.graphql.parser.Directive;
import com.arcadedb.graphql.parser.Directives;
import com.arcadedb.graphql.parser.FieldDefinition;
import com.arcadedb.graphql.parser.ObjectTypeDefinition;
import com.arcadedb.graphql.parser.Selection;
import com.arcadedb.graphql.parser.SelectionSet;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphQLResultSet implements ResultSet {
  /**
   * The meta-field every object type answers (spec 4.4.1, "Type Name Introspection"). Names starting with {@code __} are
   * reserved for introspection, so it is never read from a record property of the same name. See issue #8745.
   */
  private static final String TYPENAME_FIELD = "__typename";

  private final GraphQLSchema        schema;
  private final ResultSet            resultSet;
  private final List<Selection>      projections;
  private final ObjectTypeDefinition returnType;

  /**
   * The variable values of the operation, for the directives written in the query document. Never used for a
   * directive declared in the schema: see {@link #evaluateDirectives}.
   */
  private final Map<String, Object> variables;

  /**
   * The fragments of the query document, which the selections of every level are expanded through before they are
   * resolved: see {@link #mapBySelections}.
   */
  private final GraphQLFragments fragments;

  /**
   * The projections built for a selection list, by identity, reused for every record that list is resolved against
   * (issue #8623). What they can depend on is the outcome of the fragment type conditions evaluated while the list was
   * expanded (the {@code @skip} and {@code @include} conditions read the operation's variables, the same for every
   * record), and that outcome depends only on the schema type the list is written against and on the database type of
   * the record. So the projections are cached once for all records when every condition evaluated was decided by the
   * schema type alone, and once per database type otherwise: a document written with fragments, the default output of
   * Apollo, Relay and codegen clients, no longer expands a level again for every record.
   * <p>
   * Only a list reached through a chain of cached lists is a key: a list merged for one record only (see
   * {@link #mapBySelections}) never is, so the cache is bounded by the size of the document times the number of
   * database types, not by the number of records.
   */
  private final IdentityHashMap<List<Selection>, CachedProjections> projectionCache = new IdentityHashMap<>();

  /**
   * The projections of one selection list written against {@code parentType}: {@code shared} when they are the same for
   * every record, otherwise one entry per database type of the records met, {@code null} standing for a record that has
   * no database type.
   * <p>
   * The same list can be resolved against more than one schema type - a named fragment spread under fields of different
   * types shares its selection lists - so the entries for the other types are chained through {@code next}. Replacing
   * the entry instead made the two types evict each other on every record, and every rebuild minted new merged lists that
   * the level below then cached as new keys.
   */
  private static final class CachedProjections {
    private final ObjectTypeDefinition                            parentType;
    private final CachedProjections                               next;
    private       List<Projection>                                shared;
    private       IdentityHashMap<DocumentType, List<Projection>> byRecordType;

    private CachedProjections(final ObjectTypeDefinition parentType, final CachedProjections next) {
      this.parentType = parentType;
      this.next = next;
    }

    private CachedProjections forParentType(final ObjectTypeDefinition type) {
      for (CachedProjections entry = this; entry != null; entry = entry.next)
        if (entry.parentType == type)
          return entry;
      return null;
    }

    private List<Projection> get(final Result record) {
      if (shared != null)
        return shared;
      return byRecordType != null ? byRecordType.get(recordTypeOf(record)) : null;
    }

    private void put(final Result record, final boolean dependsOnRecord, final List<Projection> projections) {
      if (!dependsOnRecord) {
        shared = projections;
        byRecordType = null;
        return;
      }
      if (byRecordType == null)
        byRecordType = new IdentityHashMap<>(4);
      byRecordType.put(recordTypeOf(record), projections);
    }
  }

  /**
   * The type conditions of the level being expanded, evaluated against its record. One instance serves every level:
   * the expansion of a level completes, and {@link #dependsOnRecord} is read, before the level below is expanded.
   */
  private final TypeConditions typeConditions = new TypeConditions();

  /**
   * The {@code __typename} of a record by its database type, see {@link #declaredObjectTypeOf}: it depends only on the
   * type and on the SDL, fixed for the life of the result set, so a client that selects {@code __typename} everywhere
   * (Apollo does) walks the type hierarchy once per database type rather than once per record. {@link #NO_DECLARED_TYPE}
   * stands for a type with no declared ancestor.
   */
  private final IdentityHashMap<DocumentType, String> declaredObjectTypes = new IdentityHashMap<>(4);
  private static final String                         NO_DECLARED_TYPE    = "";

  /**
   * How many times the projections of a level were built rather than taken from {@link #projectionCache}, for tests.
   */
  private long projectionBuilds;

  /**
   * The types currently being expanded from the schema by {@link #mapByReturnType}, innermost last. It guards the
   * automatic expansion against a cyclic schema (e.g. {@code Book.authors -> Author.wrote -> Book}), which would
   * otherwise recurse until the stack overflows once directives are resolved against the right type. It is only
   * touched by {@code mapByReturnType}, always in a push/pop pair, so it is empty again at the end of every
   * {@link #next()}: an explicit selection set states its own depth and is never limited by it.
   */
  private final List<ObjectTypeDefinition> expansionPath = new ArrayList<>(4);

  /**
   * @param name        output key: the alias when present, otherwise the field name
   * @param fieldName   real field/property name to resolve, ignoring any alias
   * @param field       the field as written in the query document, carrying any inline directive
   * @param schemaField the field of the schema type this selection belongs to, carrying any schema-declared
   *                    directive. Resolved against the type of the enclosing selection, not against the top-level
   *                    query return type: see issue #6833
   * @param type        the object type this field returns, when the schema declares one
   * @param set         the sub-selections written in the query document, if any
   * @param cacheable   whether {@code set} is the same list for every record, so its projections can be cached
   * @param typeName    whether the field is the {@code __typename} meta-field, resolved from the type of the object
   *                    rather than from a property: see issue #8745
   */
  private record Projection(String name, String fieldName, AbstractField field, FieldDefinition schemaField,
                            ObjectTypeDefinition type, List<Selection> set, boolean cacheable, boolean typeName) {
  }

  public GraphQLResultSet(final GraphQLSchema schema, final ResultSet resultSet, final List<Selection> projections,
      final ObjectTypeDefinition returnType, final Map<String, Object> variables, final GraphQLFragments fragments) {
    if (resultSet == null)
      throw new IllegalArgumentException("NULL resultSet");

    this.schema = schema;
    this.resultSet = resultSet;
    this.projections = projections;
    this.returnType = returnType;
    this.variables = variables;
    this.fragments = fragments;
  }

  @Override
  public boolean hasNext() {
    return resultSet.hasNext();
  }

  @Override
  public Result next() {
    return projections != null ?
        mapBySelections(resultSet.next(), projections, returnType, true) :
        mapByReturnType(resultSet.next(), returnType);
  }

  private GraphQLResult mapByReturnType(final Result current, final ObjectTypeDefinition type) {
    expansionPath.add(type);
    try {
      final List<Projection> projections = new ArrayList<>(type.getFieldDefinitions().size());
      // ADD ALL THE TYPE FIELDS AUTOMATICALLY
      for (final FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
        final ObjectTypeDefinition subType = schema.getTypeFromField(fieldDefinition);
        if (subType != null && isBeingExpanded(subType))
          // THE SCHEMA IS CYCLIC ON THIS FIELD: STOP THE AUTOMATIC EXPANSION HERE RATHER THAN RECURSE FOREVER.
          // ONLY A QUERY THAT ASKS FOR THE FIELD EXPLICITLY GETS IT, AND THEN AT THE DEPTH IT ASKS FOR
          continue;

        projections.add(
            new Projection(fieldDefinition.getName(), fieldDefinition.getName(), null, fieldDefinition, subType, null, false,
                false));
      }
      return mapProjections(current, projections, type);
    } finally {
      expansionPath.removeLast();
    }
  }

  /**
   * @param parentType the schema type the selections are written against - the type of the enclosing field, not the
   *                   top-level query return type, so a schema directive declared two levels deep is found (#6833)
   */
  private GraphQLResult mapBySelections(final Result current, final List<Selection> definedProjections,
      final ObjectTypeDefinition parentType, final boolean cacheable) {
    CachedProjections cached = null;
    if (cacheable) {
      final CachedProjections head = projectionCache.get(definedProjections);
      cached = head != null ? head.forParentType(parentType) : null;
      if (cached == null) {
        cached = new CachedProjections(parentType, head);
        projectionCache.put(definedProjections, cached);
      } else {
        final List<Projection> projections = cached.get(current);
        if (projections != null)
          return mapProjections(current, projections, parentType);
      }
    }

    // A FRAGMENT SPREAD OR AN INLINE FRAGMENT HAS NO FIELD NAME OF ITS OWN: IT IS REPLACED BY THE FIELDS IT SELECTS, IF
    // ITS TYPE CONDITION APPLIES TO THIS RECORD. LEFT IN, IT DROPPED THOSE FIELDS AND TURNED INTO A NULL RESPONSE KEY
    // THAT NO SERIALIZER CAN RENDER. SEE ISSUE #7770
    typeConditions.reset(current, parentType);
    final List<Selection> selections = fragments.expand(definedProjections, typeConditions);
    final boolean dependsOnRecord = typeConditions.dependsOnRecord;
    ++projectionBuilds;

    final List<Projection> projections = new ArrayList<>(selections.size());
    for (final Selection selection : selections) {
      // A selection written as `alias: field` parses into fieldWithAlias (name = the real field,
      // alias carried by Selection.getName()); an unaliased selection parses into field instead.
      final AbstractField field = selection.getAnyField();
      final String        fieldName = selection.getFieldName();
      final SelectionSet  set = selection.getSelectionSet();
      final String        responseKey = selection.getName();

      final int existing = indexOf(projections, responseKey);
      if (existing > -1) {
        // THE SAME RESPONSE KEY SELECTED TWICE, WHICH A FRAGMENT MAKES ORDINARY (`{ authors { a } ...F }` WITH
        // `F { authors { b } }`): THE SPECIFICATION MERGES THE SUB-SELECTIONS INTO ONE FIELD RATHER THAN LETTING THE
        // LAST ONE WIN. THE MERGED LIST CAN REPEAT A KEY IN TURN, WHICH THE NEXT LEVEL MERGES THE SAME WAY
        //
        // TWO SELECTIONS UNDER ONE KEY THAT DO NOT RESOLVE TO THE SAME FIELD (`a: name` AND `a: id`) ARE A VALIDATION
        // ERROR IN THE SPECIFICATION, WHICH THIS MODULE DOES NOT PERFORM: THE FIRST ONE WRITTEN IS KEPT. SO IT IS WHEN ONLY
        // ONE OF THE TWO HAS A SUB-SELECTION, THE SAME INVALID SHAPE
        final Projection first = projections.get(existing);
        if (set != null && first.set() != null) {
          final List<Selection> merged = new ArrayList<>(first.set().size() + set.getSelections().size());
          merged.addAll(first.set());
          merged.addAll(set.getSelections());
          projections.set(existing, new Projection(first.name(), first.fieldName(), first.field(), first.schemaField(),
              first.type(), merged, cacheable, first.typeName()));
        }
        continue;
      }

      final FieldDefinition schemaField = parentType != null ? parentType.getFieldDefinitionByName(fieldName) : null;
      final ObjectTypeDefinition subType = schemaField != null ? schema.getTypeFromField(schemaField) : null;

      // THE PROJECTIONS OF A CACHED LEVEL ARE BUILT ONCE PER CACHE ENTRY, SO THE LISTS THEY HOLD, A MERGED ONE INCLUDED,
      // ARE THE SAME OBJECTS FOR EVERY RECORD THAT ENTRY SERVES: THE LEVEL BELOW IS CACHED BY THEIR IDENTITY TOO
      projections.add(new Projection(responseKey, fieldName, field, schemaField, subType,
          set != null ? set.getSelections() : null, cacheable, TYPENAME_FIELD.equals(fieldName)));
    }

    if (cached != null)
      cached.put(current, dependsOnRecord, projections);

    return mapProjections(current, projections, parentType);
  }

  /**
   * The fragment type conditions of one level, against one record. Records whether any of them had to look at the
   * record, rather than being decided by the schema type the level is written against: only then can the outcome differ
   * for a record of another database type.
   */
  private final class TypeConditions implements Predicate<String> {
    private Result               current;
    private ObjectTypeDefinition parentType;
    private boolean              dependsOnRecord;

    private void reset(final Result current, final ObjectTypeDefinition parentType) {
      this.current = current;
      this.parentType = parentType;
      this.dependsOnRecord = false;
    }

    @Override
    public boolean test(final String typeCondition) {
      if (parentType != null && typeCondition.equals(parentType.getName()))
        return true;
      dependsOnRecord = true;
      return typeConditionApplies(typeCondition, recordTypeOf(current), parentType);
    }
  }

  /** The database type of the record a result wraps, or null when it wraps none (a map, a projection) or it has none. */
  private static DocumentType recordTypeOf(final Result result) {
    final Document element = result.isElement() ? result.toElement() : null;
    return element != null ? element.getType() : null;
  }

  /** @see #projectionBuilds */
  long getProjectionBuilds() {
    return projectionBuilds;
  }

  /**
   * Whether a fragment written {@code on typeCondition} applies to a record of {@code recordType}: it does when it names
   * the schema type the selections are written against, or the database type of the record or one of its super types.
   * <p>
   * Otherwise it is refuted only when the condition names a concrete type this module can reason about - an object type
   * of the SDL or a type of the database - and the type of what is being resolved is known. A condition on anything
   * else, such as an SDL {@code interface} or {@code union}, which this module does not model and so cannot check
   * membership of, is applied rather than silently dropping the fields it selects. So is any condition when neither the
   * schema type nor the record type is known.
   */
  private boolean typeConditionApplies(final String typeCondition, final DocumentType recordType,
      final ObjectTypeDefinition parentType) {
    boolean typeKnown = parentType != null;
    if (typeKnown && typeCondition.equals(parentType.getName()))
      return true;

    if (recordType != null) {
      if (recordType.instanceOf(typeCondition))
        return true;
      typeKnown = true;
    }

    if (!typeKnown)
      return true;

    return !schema.isObjectType(typeCondition) && !schema.isDatabaseType(typeCondition);
  }

  private static int indexOf(final List<Projection> projections, final String responseKey) {
    for (int i = 0; i < projections.size(); i++)
      if (Objects.equals(projections.get(i).name(), responseKey))
        return i;
    return -1;
  }

  /**
   * Identity lookup over the (at most a handful of entries deep) automatic-expansion path. The schema keeps one
   * {@link ObjectTypeDefinition} instance per type name, so reference equality is the right comparison and costs
   * nothing.
   */
  private boolean isBeingExpanded(final ObjectTypeDefinition type) {
    for (int i = 0; i < expansionPath.size(); i++)
      if (expansionPath.get(i) == type)
        return true;
    return false;
  }

  @Override
  public void close() {
    resultSet.close();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  /**
   * @param variables the operation's variable values when {@code fieldDefinition} is a field of the query document,
   *                  whose inline directives can reference them; {@code null} for a field of the schema, whose
   *                  directives are authored in the SDL and have no operation in scope to take a variable from
   */
  private Object evaluateDirectives(final Result current, final AbstractField fieldDefinition,
      final Map<String, Object> variables) {
    Object projectionValue = null;

    if (fieldDefinition != null) {
      final Directives directives = fieldDefinition.getDirectives();
      if (directives != null) {
        for (final Directive directive : directives.getDirectives()) {
          if ("relationship".equals(directive.getName())) {
            if (directive.getArguments() != null) {
              String type = null;
              Vertex.DIRECTION direction = Vertex.DIRECTION.BOTH;
              for (final Argument argument : directive.getArguments().getList()) {
                if ("type".equals(argument.getName())) {
                  final Object value = GraphQLSchema.resolveValue(argument.getValueWithVariable(), variables);
                  type = value != null ? value.toString() : null;
                } else if ("direction".equals(argument.getName())) {
                  final Object value = GraphQLSchema.resolveValue(argument.getValueWithVariable(), variables);
                  if (value != null)
                    direction = Vertex.DIRECTION.valueOf(value.toString());
                }
              }

              if (current.getElement().isPresent()) {
                final Vertex vertex = current.getElement().get().asVertex();
                final Iterable<Vertex> connected =
                    type != null ? vertex.getVertices(direction, type) : vertex.getVertices(direction);
                projectionValue = connected;
              } else if (current.getIdentity().isPresent()) {
                final Vertex vertex = current.getIdentity().get().asVertex();
                final Iterable<Vertex> connected =
                    type != null ? vertex.getVertices(direction, type) : vertex.getVertices(direction);
                projectionValue = connected;
              }
            }
          }
        }
      }
    }
    return projectionValue;
  }

  /**
   * The value of {@code __typename} for a record resolved against {@code parentType}: the most specific object type of
   * the SDL the record is an instance of - its database type, or the nearest super type of it, that the SDL declares -
   * so a record of a database sub type of the type the field returns reports its own type when the SDL declares it.
   * Otherwise the schema type the selections are written against. With neither known, the database type of the record
   * is the only type there is to report.
   * <p>
   * The declared type of the record wins even when it is unrelated to {@code parentType}, as when a native query
   * directive returns records of another type: {@code __typename} reports what the object is, not what was expected.
   */
  private String typeNameOf(final Result current, final ObjectTypeDefinition parentType) {
    final DocumentType recordType = recordTypeOf(current);
    if (recordType != null) {
      final String declared = declaredObjectTypeOf(recordType);
      if (declared != null)
        return declared;
      if (parentType == null)
        return recordType.getName();
    }
    return parentType != null ? parentType.getName() : null;
  }

  /**
   * The name of {@code type}, or of its nearest super type, that the SDL declares as an object type; null if none. The
   * hierarchy is walked level by level, so with multiple inheritance a declared direct parent wins over a declared
   * grandparent reached through another parent; within one level the order of the super types decides.
   */
  private String declaredObjectTypeOf(final DocumentType type) {
    String declared = declaredObjectTypes.get(type);
    if (declared == null) {
      declared = NO_DECLARED_TYPE;
      final List<DocumentType> level = new ArrayList<>(2);
      level.add(type);
      for (int i = 0; i < level.size(); i++) {
        final DocumentType candidate = level.get(i);
        if (schema.isObjectType(candidate.getName())) {
          declared = candidate.getName();
          break;
        }
        for (final DocumentType superType : candidate.getSuperTypes())
          if (!level.contains(superType))
            level.add(superType);
      }
      declaredObjectTypes.put(type, declared);
    }
    return declared != NO_DECLARED_TYPE ? declared : null;
  }

  /**
   * @param parentType the schema type the projections are resolved against, or {@code null} when the SDL declares none
   */
  private GraphQLResult mapProjections(final Result current, final List<Projection> projections,
      final ObjectTypeDefinition parentType) {
    final Map<String, Object> map = new HashMap<>();

    if (current.getElement().isPresent()) {
      final Document element = current.getElement().get();
      final RID rid = element.getIdentity();
      if (rid != null)
        map.put(RID_PROPERTY, rid);
      map.put(TYPE_PROPERTY, element.getTypeName());
      map.put(CAT_PROPERTY, element instanceof Vertex ? "v" : element instanceof Edge ? "e" : "d");
    }

    for (final Projection entry : projections) {
      final String projName = entry.name();
      final String realName = entry.fieldName();

      if (entry.typeName()) {
        map.put(projName, typeNameOf(current, parentType));
        continue;
      }

      Object projectionValue = current.getProperty(realName);

      if (projectionValue == null && current.getElement().isPresent())
        // PROPERTY NOT FOUND IN PROJECTION, TRY DIRECTLY FROM THE ELEMENT (E.G. CYPHER RETURN)
        projectionValue = current.getElement().get().get(realName);

      if (projectionValue == null) {
        // TRY THE FIELD FIRST
        // AN INLINE DIRECTIVE IS WRITTEN IN THE QUERY DOCUMENT, SO IT CAN REFERENCE THE OPERATION'S VARIABLES
        projectionValue = evaluateDirectives(current, entry.field(), variables);
        if (projectionValue == null)
          // SEARCH IN THE SCHEMA, IN THE TYPE THIS SELECTION BELONGS TO. A DIRECTIVE DECLARED THERE IS PART OF THE
          // SDL, NOT OF THE OPERATION, SO NO VARIABLE IS IN SCOPE FOR IT
          projectionValue = evaluateDirectives(current, entry.schemaField(), null);
      }

      final AbstractField field = entry.field();
      if (projectionValue == null && field != null) {
        if (field.getDirectives() != null) {
          for (final Directive directive : field.getDirectives().getDirectives()) {
            if ("rid".equals(directive.getName())) {
              if (current.getElement().isPresent())
                projectionValue = current.getElement().get().getIdentity();
            } else if ("type".equals(directive.getName())) {
              if (current.getElement().isPresent())
                projectionValue = current.getElement().get().getTypeName();
            }
          }
        }
      }

      final List<Selection> selectionSet = entry.set();
      final boolean cacheable = entry.cacheable();
      final ObjectTypeDefinition projectionType = entry.type();

      if (selectionSet != null) {
        switch (projectionValue) {
        case Map m -> projectionValue = mapBySelections(new ResultInternal(m), selectionSet, projectionType, cacheable);
        case EmbeddedDocument emb -> projectionValue = mapBySelections(new ResultInternal(emb), selectionSet, projectionType, cacheable);
        case Result result -> projectionValue = mapBySelections(result, selectionSet, projectionType, cacheable);
        case Iterable iterable -> {
          final List<Result> subResults = new ArrayList<>();
          for (final Object o : iterable) {
            final Result item;
            if (o instanceof Document document)
              item = mapBySelections(new ResultInternal(document), selectionSet, projectionType, cacheable);
            else if (o instanceof Result result)
              item = mapBySelections(result, selectionSet, projectionType, cacheable);
            else
              continue;

            subResults.add(item);
          }
          projectionValue = subResults;
        }
        case null, default -> {
          continue;
        }
        }
      } else if (projectionType != null) {
        switch (projectionValue) {
        case Map m -> projectionValue = mapByReturnType(new ResultInternal(m), projectionType);
        // MIRRORS THE Map/Result ARMS: THIS BRANCH IS THE ONE WHERE selectionSet IS NULL BY CONSTRUCTION, SO
        // DELEGATING TO mapBySelections() WITH IT WAS A GUARANTEED NPE. SEE ISSUE #6835
        case EmbeddedDocument emb -> projectionValue = mapByReturnType(new ResultInternal(emb), projectionType);
        case Result result -> projectionValue = mapByReturnType(result, projectionType);
        case Iterable iterable -> {
          final List<Result> subResults = new ArrayList<>();
          for (final Object o : iterable) {
            final Result item;
            if (o instanceof Document document)
              item = mapByReturnType(new ResultInternal(document), projectionType);
            else if (o instanceof Result result)
              item = mapByReturnType(result, projectionType);
            else
              continue;

            subResults.add(item);
          }
          projectionValue = subResults;
        }
        case null, default -> {
          continue;
        }
        }
      }

      map.put(projName, projectionValue);
    }

    return new GraphQLResult(map);
  }
}
