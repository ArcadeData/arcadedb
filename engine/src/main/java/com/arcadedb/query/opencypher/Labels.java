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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utility class for multi-label support in ArcadeDB.
 * <p>
 * Provides methods to manage composite types for vertices with multiple labels.
 * When a vertex has multiple labels (e.g., Person, Developer), a composite type
 * is automatically created (Developer~Person) that extends all label types.
 * <p>
 * Labels are sorted alphabetically to ensure consistent naming regardless of
 * the order in which labels are specified.
 * <p>
 * The tilde (~) separator was chosen because it:
 * <ul>
 *   <li>Is rarely used in type/class names by users</li>
 *   <li>Is valid in SQL identifiers (can be quoted with backticks if needed)</li>
 *   <li>Visually suggests "combining" or "together"</li>
 *   <li>Does not conflict with common naming conventions (unlike underscore)</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class Labels {

  /**
   * Separator used between label names in composite type names.
   * Uses tilde (~) to avoid conflicts with user type names that may contain underscores.
   */
  public static final String LABEL_SEPARATOR = "~";

  /**
   * The type a vertex created with no labels lands in, and the one type a supertype walk never reports as a label.
   * <p>
   * Before this, "no labels" was represented by a type named {@code V} or {@code Vertex} - names drawn from the
   * same namespace users write labels from, so the two meanings collided. A node whose only label was genuinely
   * {@code V} matched {@code (n:V)} (the type system does not distinguish "this vertex's type is called V" from
   * "this vertex was tagged V") and then reported {@code labels(n) = []}, because {@link #getLabels} filtered the
   * name to answer the other question. Adding a label to such a node rebuilt its composite from the label set
   * {@link #getOwnLabels} computed, which the same filter had emptied, so {@code SET n:Extra} silently dropped the
   * original label instead of keeping it alongside the new one (issue #6395, the corner issue #6363 deliberately
   * left alone).
   * <p>
   * Reserved by construction rather than by convention: {@link #LABEL_SEPARATOR} already cannot appear inside a
   * single label (it is how a composite name is told apart from one), so wrapping the sentinel in it - as opposed
   * to picking an ordinary-looking word like {@code V} - means no label a query writes can ever equal this name,
   * and the filter in {@link #isBaseVertexTypeName} needs no exception for a real label happening to share it.
   * <p>
   * Also the type the imported-from-Neo4j graph's unlabelled nodes land in, and the common supertype every
   * imported label type extends (see {@code Neo4jImporter}): a shared root lets one polymorphic query reach
   * every vertex the import produced regardless of its label, which is worth keeping - the mistake was giving
   * that root a name ({@code Node}) that a real schema might also want to use as an ordinary label, and that the
   * label walk did not know to filter away from an ancestor position. Filtering this sentinel at every depth of
   * the walk, not only at the vertex's own type, is what makes a shared root safe to keep: {@code Person EXTENDS
   * ~NO_LABEL~} reports {@code ["Person"]}, not {@code ["NO_LABEL", "Person"]}.
   */
  public static final String NO_LABEL_TYPE = LABEL_SEPARATOR + "NO_LABEL" + LABEL_SEPARATOR;

  /**
   * Private constructor to prevent instantiation.
   */
  private Labels() {
  }

  /**
   * Generates composite type name from labels (deduplicated and sorted alphabetically).
   * <p>
   * Duplicate labels are automatically removed, as labels represent set membership
   * (a node either has a label or it doesn't - specifying the same label multiple
   * times is redundant). This matches Neo4j's behavior.
   * <p>
   * Examples:
   * <ul>
   *   <li>["Person"] → "Person"</li>
   *   <li>["Person", "Developer"] → "Developer~Person"</li>
   *   <li>["Developer", "Person"] → "Developer~Person" (same, sorted)</li>
   *   <li>["A", "B", "C"] → "A~B~C"</li>
   *   <li>["Person", "Kebab", "Person"] → "Kebab~Person" (duplicate removed)</li>
   *   <li>["Kebab", "Kebab"] → "Kebab" (all duplicates removed)</li>
   * </ul>
   *
   * @param labels list of label names (duplicates are ignored)
   * @return composite type name, or {@link #NO_LABEL_TYPE} if labels is null/empty
   */
  public static String getCompositeTypeName(final List<String> labels) {
    if (labels == null || labels.isEmpty())
      return NO_LABEL_TYPE;
    if (labels.size() == 1)
      return labels.get(0);

    // Use TreeSet to both deduplicate and sort alphabetically
    final Set<String> uniqueSorted = new TreeSet<>(labels);

    // After deduplication, if only one unique label, return it directly
    if (uniqueSorted.size() == 1)
      return uniqueSorted.iterator().next();

    return String.join(LABEL_SEPARATOR, uniqueSorted);
  }

  /**
   * Every label a vertex answers to: its own type name and each of its ancestors', sorted alphabetically, with the
   * synthetic composite names and {@link #NO_LABEL_TYPE} left out. A node whose own type is the sentinel carries
   * no label at all and answers with an empty list.
   * <p>
   * The set this returns is exactly the set {@link #hasLabel} says yes to, which is the invariant Cypher promises and
   * the one this used to break (issue #6363): the rule was "supertypes are the labels, unless there are none", written
   * for the {@code A~B} composite - whose own name is an implementation detail and whose supertypes really are its
   * labels - and applied to every other shape as well. A vertex of a type declared {@code Manager EXTENDS Employee}
   * reported {@code [Employee]}, missing the very label {@code MATCH (n:Manager)} had just matched it by, and a type
   * extending a composite reported the composite's synthetic name instead of the two labels it encodes.
   * <p>
   * A composite name never leaves this method: it is an encoding of the labels below it, so the walk goes through it
   * to its supertypes rather than reporting it. Which types those are is decided structurally, by
   * {@link #isLabelComposite} - a type somebody created whose name merely contains the separator keeps its name and
   * is a label like any other.
   *
   * @param vertex the vertex to get labels from
   *
   * @return the vertex's labels, sorted alphabetically, empty for an unlabelled node
   */
  public static List<String> getLabels(final Vertex vertex) {
    return getLabels(vertex.getType());
  }

  /**
   * Type-level form of {@link #getLabels(Vertex)}.
   */
  public static List<String> getLabels(final DocumentType type) {
    final String typeName = type.getName();

    // A vertex created without labels lands in NO_LABEL_TYPE, and in Cypher an unlabelled node has an empty
    // label list.
    if (isBaseVertexTypeName(typeName))
      return List.of();

    final List<DocumentType> superTypes = type.getSuperTypes();
    // The overwhelmingly common case - a vertex of an ordinary type that extends nothing - answers without
    // allocating a set.
    if (superTypes.isEmpty())
      return List.of(typeName);

    final Set<String> labels = new TreeSet<>();
    collectLabels(type, labels);
    return List.copyOf(labels);
  }

  /**
   * Adds {@code type}'s label to {@code out} unless the type is a synthetic composite or the sentinel, then
   * recurses into its supertypes.
   * <p>
   * Unlike {@code V}/{@code Vertex} before issue #6395, {@link #NO_LABEL_TYPE} is filtered at every depth, not
   * only at the vertex's own type: it is reserved by construction (its name can never be a real label, see the
   * field's own javadoc), so there is no ordinary-label reading to preserve for it the way there was for a
   * genuine supertype named {@code V} - the openCypher TCK writes exactly that in {@code (b:U:V:W:X:Y:Z)}, and
   * with {@code V} no longer a sentinel it is reported like any other label.
   */
  private static void collectLabels(final DocumentType type, final Set<String> out) {
    final List<DocumentType> superTypes = type.getSuperTypes();
    if (isBaseVertexTypeName(type.getName())) {
      // The sentinel is never a label, however deep in the hierarchy it appears - unlike V/Vertex before it, it
      // is not a name a query can ever have written, so there is no ordinary-label reading to preserve here the
      // way there is one for a genuine supertype named V (issue #6395). Recurse past it rather than stopping: a
      // type that extends it may still extend something else worth reporting.
      for (int i = 0; i < superTypes.size(); i++)
        collectLabels(superTypes.get(i), out);
      return;
    }
    if (!isLabelComposite(type, superTypes))
      out.add(type.getName());
    for (int i = 0; i < superTypes.size(); i++)
      collectLabels(superTypes.get(i), out);
  }

  /**
   * The labels a relabelling has to carry over, which is not the same question as {@link #getLabels}: an inherited
   * label comes back on its own through the type hierarchy, so naming it again in the new composite would flatten the
   * hierarchy instead of extending it.
   * <p>
   * For a vertex of type {@code Manager EXTENDS Employee} this is {@code [Manager]}, so {@code SET n:Extra} builds
   * {@code Extra~Manager} extending both - the vertex stays a {@code Manager} and therefore stays an {@code Employee}
   * too. Deriving the composite from the full label set instead would have built {@code Employee~Extra} and dropped
   * the subtype, which is what issue #6363 reports. For a composite the answer is the labels it encodes, reached
   * through its supertypes exactly as before, and a type that only looks like one by name keeps its name.
   *
   * @param vertex the vertex about to be relabelled
   *
   * @return the minimal label set that reproduces the vertex's current type, sorted alphabetically
   */
  public static List<String> getOwnLabels(final Vertex vertex) {
    final DocumentType type = vertex.getType();
    final String typeName = type.getName();

    if (isBaseVertexTypeName(typeName))
      return List.of();
    if (!isLabelComposite(type, type.getSuperTypes()))
      return List.of(typeName);

    final Set<String> labels = new TreeSet<>();
    collectOwnLabels(type, labels);
    return List.copyOf(labels);
  }

  /**
   * Walks down through composite types only: an ordinary type stops the walk and contributes its own name, because
   * its ancestors are reached from it and do not have to be listed alongside it.
   */
  private static void collectOwnLabels(final DocumentType type, final Set<String> out) {
    final List<DocumentType> superTypes = type.getSuperTypes();
    if (isBaseVertexTypeName(type.getName())) {
      // Symmetric with collectLabels: a composite that happens to inherit the sentinel through one of its own
      // supertypes (the Neo4j importer's shared root, reached through a label type) must not have it show
      // through a relabelling's own-label set either.
      for (int i = 0; i < superTypes.size(); i++)
        collectOwnLabels(superTypes.get(i), out);
      return;
    }
    if (!isLabelComposite(type, superTypes)) {
      out.add(type.getName());
      return;
    }
    for (int i = 0; i < superTypes.size(); i++)
      collectOwnLabels(superTypes.get(i), out);
  }

  /**
   * Whether a type is one this class built to carry several labels, as opposed to a type somebody created whose name
   * merely contains the separator.
   * <p>
   * Asked structurally rather than by name: a composite's name is exactly the deduplicated, sorted, separator-joined
   * names of its own supertypes, which is what {@link #ensureCompositeType} writes and what nothing else produces.
   * The name alone cannot answer it - {@code isCompositeTypeName} is a heuristic, and under it a user type called
   * {@code a~b} that extends anything would have had its own name dropped from both the label list and, worse, from
   * the set a relabelling rebuilds the type out of.
   *
   * @param type       the type to classify
   * @param superTypes {@code type}'s direct supertypes, passed in because every caller already has them
   *
   * @return true when the type is a label composite and its name is therefore an encoding, not a label
   */
  private static boolean isLabelComposite(final DocumentType type, final List<DocumentType> superTypes) {
    // A composite always encodes at least two labels: one label is the type itself, never a composite.
    if (superTypes.size() < 2 || !isCompositeTypeName(type.getName()))
      return false;
    final List<String> superTypeNames = new ArrayList<>(superTypes.size());
    for (int i = 0; i < superTypes.size(); i++)
      superTypeNames.add(superTypes.get(i).getName());
    return type.getName().equals(getCompositeTypeName(superTypeNames));
  }

  /**
   * Whether a type name is {@link #NO_LABEL_TYPE}, the type a node lands in when it carries no label at all: such
   * a node is a Cypher node with an empty label list.
   */
  private static boolean isBaseVertexTypeName(final String typeName) {
    return NO_LABEL_TYPE.equals(typeName);
  }

  /**
   * The labels a vertex keeps after a {@code REMOVE n:A:B}, expressed the way {@link #ensureCompositeType} wants
   * them: the most specific of the labels the vertex still answers to, with every label one of those already
   * implies left out.
   * <p>
   * The set has to be computed from the FULL label set ({@link #getLabels}) and not from the own-label set, which
   * is what issue #6843 reports: for a vertex of a type declared {@code Cust_Agent EXTENDS Entity}, the own labels
   * are {@code [Cust_Agent]} alone, so removing {@code Cust_Agent} left nothing behind and the vertex was moved to
   * {@link #NO_LABEL_TYPE} - losing {@code Entity}, a label the removal never named and one the node kept
   * answering to a moment earlier. Starting from every label it answers to keeps {@code Entity} in the picture.
   * <p>
   * Reducing that set back to its most specific members is what keeps the hierarchy intact, and is the reason the
   * own-label set was used in the first place (issue #6363): a vertex of type {@code Extra~Manager}, with
   * {@code Manager EXTENDS Employee}, answers to {@code [Employee, Extra, Manager]}, and rebuilding its type from
   * all three after {@code REMOVE n:Extra} would build {@code Employee~Manager} - a type extending both, which
   * flattens the very {@code EXTENDS} it was supposed to preserve. {@code Manager} already implies
   * {@code Employee}, so {@code Employee} is dropped and the vertex simply becomes a {@code Manager} again.
   * <p>
   * With nothing removed, this returns exactly {@link #getOwnLabels}, which is what makes a {@code REMOVE} of a
   * label the vertex does not carry leave its type untouched.
   *
   * @param schema         the database schema
   * @param vertex         the vertex the labels are being taken from
   * @param labelsToRemove the labels named by the clause
   *
   * @return the minimal label set to rebuild the vertex's type from, sorted alphabetically, possibly empty
   */
  public static List<String> remainingLabels(final Schema schema, final Vertex vertex, final List<String> labelsToRemove) {
    final List<String> candidates = new ArrayList<>(getLabels(vertex));
    candidates.removeAll(labelsToRemove);
    if (candidates.size() < 2)
      return candidates;

    // Keep a label only when no other surviving label already implies it: an implied one is reached through its
    // subtype and naming it alongside would extend both instead of only the subtype.
    final List<String> minimal = new ArrayList<>(candidates.size());
    for (int i = 0; i < candidates.size(); i++) {
      final String label = candidates.get(i);
      boolean implied = false;
      for (int j = 0; j < candidates.size() && !implied; j++) {
        if (i == j)
          continue;
        final DocumentType other = schema.getTypeOrNull(candidates.get(j));
        // The second half of the condition is unreachable on any schema the engine can build: two DISTINCT
        // candidate names (the set comes from a TreeSet, so there are no repeats) that are each other's instance
        // require a cycle in the type hierarchy, which the schema refuses to create. It is kept as a guard rather
        // than as live logic - without it a cycle would mark both members implied and drop them BOTH, silently
        // costing the vertex a label it still answers to, which is the exact failure this method exists to stop.
        // The index comparison breaks such a tie so that exactly one of the pair survives.
        if (other != null && other.instanceOf(label) && (!isImpliedBy(schema, label, candidates.get(j)) || j < i))
          implied = true;
      }
      if (!implied)
        minimal.add(label);
    }
    return minimal;
  }

  private static boolean isImpliedBy(final Schema schema, final String label, final String other) {
    final DocumentType type = schema.getTypeOrNull(label);
    return type != null && type.instanceOf(other);
  }

  /**
   * Whether a label is still carried by a set of labels, which is what decides if a {@code REMOVE n:Label} can be
   * honoured: an inherited label cannot be taken away on its own, because every type answering to the subtype that
   * implies it answers to it too (issue #6363).
   *
   * @param schema          the database schema
   * @param remainingLabels the labels the vertex would keep
   * @param label           the label being taken away
   *
   * @return true when one of the remaining labels names a type that is still an instance of {@code label}
   */
  public static boolean impliedBy(final Schema schema, final List<String> remainingLabels, final String label) {
    for (int i = 0; i < remainingLabels.size(); i++) {
      final DocumentType type = schema.getTypeOrNull(remainingLabels.get(i));
      if (type != null && type.instanceOf(label))
        return true;
    }
    return false;
  }

  /**
   * Checks if a vertex has a specific label.
   * <p>
   * Uses type inheritance (instanceOf) for checking, so a vertex with
   * composite type Developer~Person will return true for both "Developer"
   * and "Person" labels.
   *
   * @param vertex the vertex to check
   * @param label  the label to look for
   * @return true if the vertex has the specified label
   */
  public static boolean hasLabel(final Vertex vertex, final String label) {
    return vertex.getType().instanceOf(label);
  }

  /**
   * Checks a vertex against a pattern's label list, with the meaning the pattern gave it: a disjunction
   * {@code (n:A|B)} is satisfied by any one of the labels, a conjunction {@code (n:A:B)} by all of them, and an
   * empty list by everything.
   * <p>
   * This is the one place that knows what a disjunction means for a matched record. Every step that has to decide
   * whether a vertex satisfies a node pattern routes through it - the anchor of a pattern, the far endpoint of a
   * single hop, and the end of a variable-length path - because they used to decide it separately and disagreed:
   * before issue #6338 a disjunction written on a node a relationship expands into ANDed its alternatives and so
   * rejected every row, silently, while the same disjunction on the anchor matched.
   *
   * @param vertex      the vertex to test
   * @param labels      the labels written on the pattern (already resolved, dynamic labels included)
   * @param disjunction whether the labels were written as alternatives ({@code A|B}) rather than as a conjunction
   *
   * @return true when the vertex satisfies the label constraint
   */
  public static boolean matches(final Vertex vertex, final List<String> labels, final boolean disjunction) {
    return matches(vertex.getType(), labels, disjunction);
  }

  /**
   * Type-level form of {@link #matches(Vertex, List, boolean)}, for the paths that resolve a record's type from its
   * bucket id and must not pay for loading the record.
   */
  public static boolean matches(final DocumentType type, final List<String> labels, final boolean disjunction) {
    if (labels == null || labels.isEmpty())
      return true;
    if (type == null)
      return false;
    if (disjunction) {
      for (int i = 0; i < labels.size(); i++)
        if (type.instanceOf(labels.get(i)))
          return true;
      return false;
    }
    for (int i = 0; i < labels.size(); i++)
      if (!type.instanceOf(labels.get(i)))
        return false;
    return true;
  }

  /**
   * Whether an index resolved for {@code label} can hand back records that do not carry it: true exactly when the
   * index is declared on a supertype. An inherited index is a single logical index over the whole hierarchy - the
   * schema gives it a sub-index for every bucket of every subtype - so a seek on it from a child type also walks
   * the parent's own records and every sibling child's, and has to filter them out. That filter is what the SQL
   * plan for the same query spells as {@code FILTER ITEMS BY TYPE} (issue #7021).
   * <p>
   * A seek on the type's own index needs no filter, which is why this is asked before wrapping a cursor rather
   * than filtering unconditionally.
   */
  public static boolean isInheritedIndex(final Index index, final String label) {
    return index != null && label != null && !label.equals(index.getTypeName());
  }

  /**
   * Whether the record {@code identifiable} names carries {@code label}, answered from the bucket its RID names
   * so a record of another type in the hierarchy is rejected without ever being loaded. This is the row-level
   * form of the filter an inherited-index seek owes (see {@link #isInheritedIndex}); a cursor that can be walked
   * as a plain iterator gets it applied by {@link #filterByLabel} instead.
   */
  public static boolean carriesLabel(final Schema schema, final Identifiable identifiable, final String label) {
    final DocumentType type = schema.getTypeByBucketId(identifiable.getIdentity().getBucketId());
    return type != null && type.instanceOf(label);
  }

  /**
   * Filters an index cursor down to the records that carry {@code label}, for the seeks that read an inherited
   * index (see {@link #isInheritedIndex}).
   */
  public static Iterator<Identifiable> filterByLabel(final Iterator<Identifiable> source, final Database database,
      final String label) {
    final Schema schema = database.getSchema();
    return new Iterator<>() {
      private Identifiable next = advance();

      private Identifiable advance() {
        while (source.hasNext()) {
          final Identifiable candidate = source.next();
          if (carriesLabel(schema, candidate, label))
            return candidate;
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public Identifiable next() {
        if (next == null)
          throw new NoSuchElementException();
        final Identifiable current = next;
        next = advance();
        return current;
      }
    };
  }

  /**
   * Whether the schema can still produce a record satisfying the label constraint, used to skip work rather than to
   * decide a row: a conjunction needs every label to name an existing type, a disjunction only needs one of them.
   * An alternative naming a type nobody ever created is an alternative that matches nothing, not a filter that
   * rejects everything (issue #6338).
   */
  public static boolean canMatchInSchema(final Schema schema, final List<String> labels, final boolean disjunction) {
    if (labels == null || labels.isEmpty())
      return true;
    if (disjunction) {
      for (int i = 0; i < labels.size(); i++)
        if (schema.existsType(labels.get(i)))
          return true;
      return false;
    }
    for (int i = 0; i < labels.size(); i++)
      if (!schema.existsType(labels.get(i)))
        return false;
    return true;
  }

  /**
   * The vertex types whose records can satisfy a node pattern's label constraint: every declared vertex type
   * {@link #matches(DocumentType, List, boolean)} accepts, so a disjunction {@code (n:A|B)} selects the types of
   * <b>both</b> alternatives and a conjunction {@code (n:A:B)} only the types carrying all of them. An empty label
   * list selects every vertex type.
   * <p>
   * This is the scan-side companion of {@code matches}: deciding a row and deciding what to scan have to say the
   * same thing about a disjunction, and when they did not, an unbound start node scanned only the first
   * alternative's type and the alternatives after it silently went missing from the answer (issue #6352).
   * <p>
   * The returned types are meant to be iterated <b>non-polymorphically</b>: the list already contains every matching
   * subtype, so a polymorphic scan of each would visit a subtype once per matching ancestor.
   * <p>
   * The cost model sums its estimate over this same list, so what a disjunction anchor is costed at and what it
   * actually visits stay the same set (issue #6363).
   *
   * @param schema      the database schema
   * @param labels      the labels written on the pattern (already resolved, dynamic labels included)
   * @param disjunction whether the labels were written as alternatives ({@code A|B}) rather than as a conjunction
   *
   * @return the vertex types to scan, empty when nothing in the schema can match
   */
  public static List<DocumentType> matchingVertexTypes(final Schema schema, final List<String> labels,
      final boolean disjunction) {
    final Collection<? extends DocumentType> allTypes = schema.getTypes();
    final List<DocumentType> matching = new ArrayList<>(allTypes.size());
    for (final DocumentType type : allTypes)
      if (type instanceof VertexType && matches(type, labels, disjunction))
        matching.add(type);
    return matching;
  }

  /**
   * Walks every vertex that can satisfy a node pattern's label constraint, in one lazy pass and without visiting a
   * vertex twice, over the types {@code matchingVertexTypes} selects.
   * <p>
   * A single label is served by one polymorphic scan of that type, which is the same set of records at the price of
   * one schema lookup instead of a walk of the whole type list.
   *
   * @param database    the database to scan
   * @param labels      the labels written on the pattern (already resolved, dynamic labels included)
   * @param disjunction whether the labels were written as alternatives ({@code A|B}) rather than as a conjunction
   *
   * @return an iterator over the candidate vertices, empty when nothing in the schema can match
   */
  public static Iterator<Record> iterateMatchingVertices(final Database database, final List<String> labels,
      final boolean disjunction) {
    final Schema schema = database.getSchema();

    if (labels != null && labels.size() == 1) {
      final String label = labels.get(0);
      // A type name that names an edge or document type is not a label: labels and relationship types are separate
      // namespaces in Cypher, so such a pattern matches no node rather than yielding records that are not vertices.
      if (!(schema.getTypeOrNull(label) instanceof VertexType))
        return Collections.emptyIterator();
      return database.iterateType(label, true);
    }

    final List<DocumentType> types = matchingVertexTypes(schema, labels, disjunction);
    if (types.isEmpty())
      return Collections.emptyIterator();
    if (types.size() == 1)
      return database.iterateType(types.get(0).getName(), false);
    return new ChainedTypeIterator(database, types);
  }

  /**
   * Walks a list of types one after the other, opening each type's iterator only when the previous one is drained.
   */
  private static final class ChainedTypeIterator implements Iterator<Record> {
    private final Database           database;
    private final List<DocumentType> types;
    private       int                nextType = 0;
    private       Iterator<Record>   current  = Collections.emptyIterator();

    private ChainedTypeIterator(final Database database, final List<DocumentType> types) {
      this.database = database;
      this.types = types;
    }

    @Override
    public boolean hasNext() {
      while (!current.hasNext() && nextType < types.size())
        current = database.iterateType(types.get(nextType++).getName(), false);
      return current.hasNext();
    }

    @Override
    public Record next() {
      if (!hasNext())
        throw new NoSuchElementException();
      return current.next();
    }
  }

  /**
   * Ensures composite type exists, creating it if necessary.
   * Returns the type name to use for creating vertices.
   * <p>
   * If a single label is provided (or after deduplication only one unique label remains),
   * creates/returns that type directly. If multiple unique labels are provided,
   * ensures all base types exist and creates a composite type that extends all of them.
   * <p>
   * Duplicate labels are automatically ignored, matching Neo4j's behavior
   * (GitHub issue #3264).
   * <p>
   * A label literally equal to {@link #NO_LABEL_TYPE} is refused rather than created: {@link #LABEL_SEPARATOR}
   * makes the sentinel unwritable as one label among several (it can never survive {@link #isLabelComposite}'s
   * name match), but a single-label {@code CREATE (:`~NO_LABEL~`)} bypasses that entirely and would otherwise
   * land the vertex on the sentinel type itself - the exact {@code V}/{@code Vertex} collision this class exists
   * to close, reopened under a name a query merely has to spell correctly instead of guess (issue #6395 review).
   * Every write path that can introduce a new label - {@code CreateStep}, {@code MergeStep}, {@code SetStep}'s
   * {@code SET n:Label}, and the {@code merge.node} procedure - creates its type through this one method, so the
   * guard here closes all of them at once.
   *
   * @param schema the database schema
   * @param labels list of labels for the vertex (duplicates are ignored)
   * @return the type name to use (composite type name if multiple unique labels)
   *
   * @throws CommandSemanticException when {@code labels} contains {@link #NO_LABEL_TYPE}
   */
  public static String ensureCompositeType(final Schema schema, final List<String> labels) {
    if (labels == null || labels.isEmpty())
      return NO_LABEL_TYPE;

    // Deduplicate and sort labels using TreeSet
    final Set<String> uniqueLabels = new TreeSet<>(labels);

    if (uniqueLabels.contains(NO_LABEL_TYPE))
      throw new CommandSemanticException(
          "'" + NO_LABEL_TYPE + "' is a reserved type name and cannot be used as a label");

    // Handle single label case (including after deduplication)
    if (uniqueLabels.size() == 1) {
      final String label = uniqueLabels.iterator().next();
      schema.getOrCreateVertexType(label);
      return label;
    }

    final String compositeTypeName = String.join(LABEL_SEPARATOR, uniqueLabels);

    // Always ensure all base types exist first, regardless of whether
    // the composite type already exists. This fixes GitHub issue #3266
    // where pre-existing composite types would prevent label type creation.
    for (final String label : uniqueLabels) {
      schema.getOrCreateVertexType(label);
    }

    if (!schema.existsType(compositeTypeName)) {
      // Create composite type extending all labels
      // Use the builder to add multiple supertypes
      var builder = schema.buildVertexType()
          .withName(compositeTypeName);

      // Add each unique label as a supertype
      for (final String label : uniqueLabels) {
        builder = builder.withSuperType(label);
      }

      builder.create();
    } else {
      // Composite type already exists - ensure it has correct supertypes
      // This handles the case where the type was created manually without inheritance
      final var existingType = schema.getType(compositeTypeName);
      for (final String label : uniqueLabels) {
        if (!existingType.instanceOf(label)) {
          existingType.addSuperType(label);
        }
      }
    }

    return compositeTypeName;
  }

  /**
   * Checks if a type name appears to be a composite type name.
   * <p>
   * This is a heuristic check based on the presence of the label separator (~).
   * Type names with underscores are no longer falsely detected as composite types.
   *
   * @param typeName the type name to check
   * @return true if the type name contains the label separator
   */
  public static boolean isCompositeTypeName(final String typeName) {
    return typeName != null && typeName.contains(LABEL_SEPARATOR);
  }
}
