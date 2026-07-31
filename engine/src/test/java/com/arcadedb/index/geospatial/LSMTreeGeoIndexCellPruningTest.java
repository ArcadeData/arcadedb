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
package com.arcadedb.index.geospatial;

import com.arcadedb.function.sql.geo.GeoUtils;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5600 (3): the FRONTIER tokenizer collapses a complete set of sibling frontier cells into their parent, the
 * same reduction Lucene's {@code RecursivePrefixTreeStrategy.pruneLeafyBranches} applies by default on its indexing
 * path - but streaming, instead of materialising the whole decomposition in a list.
 * <p>
 * The reference here IS Lucene: its buffered implementation is invoked by reflection and the two token sets must agree
 * on every shape.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeGeoIndexCellPruningTest {

  private static final String[][] SHAPES = { //
      { "point", "POINT (12.5 41.9)" }, //
      { "small square", "POLYGON ((12.4 41.8, 12.6 41.8, 12.6 42.0, 12.4 42.0, 12.4 41.8))" }, //
      { "wide rectangle", "POLYGON ((7 36, 18 36, 18 47, 7 47, 7 36))" }, //
      { "jagged outline", jagged() }, //
      { "linestring", "LINESTRING (12.4 41.8, 12.6 41.9, 12.8 41.7, 13.0 42.0, 13.4 41.5)" }, //
      { "thin sliver", "POLYGON ((12.0 41.0, 14.0 41.0001, 14.0 41.0002, 12.0 41.0003, 12.0 41.0))" }, //
      { "polygon with hole",
          "POLYGON ((12 41, 14 41, 14 43, 12 43, 12 41), (12.5 41.5, 13.5 41.5, 13.5 42.5, 12.5 42.5, 12.5 41.5))" }, //
      { "antimeridian", "POLYGON ((179 -1, 180 -1, 180 1, 179 1, 179 -1))" } };

  @Test
  void streamingPruneMatchesLucene() throws Exception {
    for (final int precision : new int[] { 6, 8, 11, 12 })
      for (final String[] shape : SHAPES)
        assertThat(arcadeTokens(precision, shape[1]))
            .as("%s at precision %d", shape[0], precision)
            .isEqualTo(lucenePrunedFrontierTokens(precision, shape[1]));
  }

  /**
   * The equality above compares SETS, so it would say nothing about a token emitted twice - which in production, where
   * the tokens go into a List and then one put() each, is a duplicate index entry. Assert on the raw stream instead.
   */
  @Test
  void noTokenIsEmittedTwice() {
    for (final int precision : new int[] { 6, 8, 11, 12 })
      for (final String[] shape : SHAPES) {
        final List<String> emitted = arcadeTokenStream(precision, shape[1]);
        assertThat(emitted).as("%s at precision %d", shape[0], precision).doesNotHaveDuplicates();
      }
  }

  /**
   * Pruning may only ENLARGE the covered area, never shrink it: every unpruned frontier cell must still be covered by
   * a pruned token, i.e. have one of them as a prefix of itself (or be one of them).
   */
  @Test
  void pruningNeverDropsCoverage() throws Exception {
    for (final int precision : new int[] { 6, 8, 11 })
      for (final String[] shape : SHAPES) {
        final TreeSet<String> pruned = arcadeTokens(precision, shape[1]);
        for (final String cell : unprunedFrontierTokens(precision, shape[1])) {
          boolean covered = false;
          for (final String token : pruned)
            if (cell.startsWith(token)) {
              covered = true;
              break;
            }
          assertThat(covered).as("%s at precision %d: cell '%s' lost its cover", shape[0], precision, cell).isTrue();
        }
      }
  }

  /**
   * A point is a single chain of one-child cells, so it can never have a complete set of siblings to collapse: the
   * one-token-per-point guarantee of #5478 is untouched.
   */
  @Test
  void aPointStillCostsExactlyOneToken() throws Exception {
    for (final int precision : new int[] { 6, 8, 11, 12 })
      assertThat(arcadeTokens(precision, "POINT (12.5 41.9)")).hasSize(1);
  }

  private static TreeSet<String> arcadeTokens(final int precision, final String wkt) {
    return new TreeSet<>(arcadeTokenStream(precision, wkt));
  }

  /** The tokens exactly as the index receives them, duplicates and all. */
  private static List<String> arcadeTokenStream(final int precision, final String wkt) {
    final GeohashPrefixTree grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    final Shape shape = GeoUtils.parseGeometry(wkt);
    final List<String> tokens = new ArrayList<>();
    LSMTreeGeoIndex.forEachPrunedFrontierCell(grid, shape, detailLevel(grid, shape), tokens::add);
    return tokens;
  }

  private static TreeSet<String> unprunedFrontierTokens(final int precision, final String wkt) {
    final GeohashPrefixTree grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    final Shape shape = GeoUtils.parseGeometry(wkt);
    return frontierTokens(walk(grid.getTreeCellIterator(shape, detailLevel(grid, shape))));
  }

  @SuppressWarnings("unchecked")
  private static TreeSet<String> lucenePrunedFrontierTokens(final int precision, final String wkt) throws Exception {
    final GeohashPrefixTree grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    final RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, "geo");
    final Shape shape = GeoUtils.parseGeometry(wkt);

    final Method createCellIteratorToIndex = RecursivePrefixTreeStrategy.class.getDeclaredMethod(
        "createCellIteratorToIndex", Shape.class, int.class, Iterator.class);
    createCellIteratorToIndex.setAccessible(true);

    final Iterator<Cell> cells = (Iterator<Cell>) createCellIteratorToIndex.invoke(strategy, shape,
        detailLevel(grid, shape), null);
    return frontierTokens(walk(cells));
  }

  private static int detailLevel(final GeohashPrefixTree grid, final Shape shape) {
    return grid.getLevelForDistance(SpatialArgs.calcDistanceFromErrPct(shape,
        new RecursivePrefixTreeStrategy(grid, "geo").getDistErrPct(), GeoUtils.getSpatialContext()));
  }

  private static List<Cell> walk(final Iterator<Cell> it) {
    final List<Cell> out = new ArrayList<>();
    while (it.hasNext())
      out.add(it.next());
    return out;
  }

  private static List<Cell> walk(final CellIterator it) {
    final List<Cell> out = new ArrayList<>();
    while (it.hasNext())
      out.add(it.next());
    return out;
  }

  /** Same rule the index uses: a cell is a frontier when the cell after it in the pre-order walk is not deeper. */
  private static TreeSet<String> frontierTokens(final List<Cell> cells) {
    final TreeSet<String> out = new TreeSet<>();
    for (int i = 0; i < cells.size(); i++) {
      final boolean frontier = i == cells.size() - 1 || cells.get(i + 1).getLevel() <= cells.get(i).getLevel();
      final String token = cells.get(i).getTokenBytesNoLeaf(null).utf8ToString();
      if (frontier && !token.isEmpty())
        out.add(token);
    }
    return out;
  }

  private static String jagged() {
    final StringBuilder sb = new StringBuilder("POLYGON ((");
    for (int i = 0; i <= 200; i++)
      sb.append(12.0 + i * 0.005).append(' ').append(41.0 + (i % 2 == 0 ? 0.0 : 0.004)).append(", ");
    return sb.append("13.0 41.5, 12.0 41.5, 12.0 41.0))").toString();
  }
}
