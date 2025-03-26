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
package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.select.SelectIterator;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Check graph integrity. If fix mode is enabled, it tries to fix the graph by removing corrupted records and
 * reconnecting edges.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 */
public class GraphDatabaseChecker {
  private final DatabaseInternal database;
  private final GraphEngine      graphEngine;

  public GraphDatabaseChecker(DatabaseInternal database) {
    this.database = database;
    this.graphEngine = database.getGraphEngine();
  }

  public Map<String, Object> checkVertices(final String typeName, final boolean fix, final int verboseLevel) {
    final AtomicLong autoFix = new AtomicLong();
    final AtomicLong invalidLinks = new AtomicLong();
    final LinkedHashSet<RID> corruptedRecords = new LinkedHashSet<>();
    final List<String> warnings = new ArrayList<>();
    final Set<RID> reconnectOutEdges = new HashSet<>();
    final Set<RID> reconnectInEdges = new HashSet<>();

    final Map<String, Object> stats = new HashMap<>();

    database.begin();
    try {
      // CHECK RECORD IS OF THE RIGHT TYPE
      final DocumentType type = database.getSchema().getType(typeName);
      for (final Bucket b : type.getBuckets(false)) {
        b.scan((rid, view) -> {
          try {
            final Record record = database.getRecordFactory().newImmutableRecord(database, type, rid, view, null);
            record.asVertex(true);
          } catch (Exception e) {
            warnings.add("vertex " + rid + " cannot be loaded, removing it");
            corruptedRecords.add(rid);
          }
          return true;
        }, null);
      }

      database.scanType(typeName, false, (record) -> {
        try {
          Vertex vertex = record.asVertex(true);

          final RID vertexIdentity = vertex.getIdentity();

          vertex = checkOutgoingEdges(fix, vertex, warnings, vertexIdentity, invalidLinks, corruptedRecords, reconnectOutEdges);

          checkIncomingEdges(fix, vertex, warnings, vertexIdentity, invalidLinks, corruptedRecords, reconnectInEdges);

        } catch (final Throwable e) {
          warnings.add("vertex " + record.getIdentity() + " cannot be loaded (error: " + e.getMessage() + ")");
          corruptedRecords.add(record.getIdentity());
        }

        return true;
      }, (rid, exception) -> {
        warnings.add("vertex " + rid + " cannot be loaded (error: " + exception.getMessage() + ")");
        corruptedRecords.add(rid);
        return true;
      });

      if (fix) {
        if (!reconnectOutEdges.isEmpty() || !reconnectInEdges.isEmpty())
          reconnectEdges(reconnectOutEdges, reconnectInEdges, warnings, stats);

        for (final RID rid : corruptedRecords) {
          if (rid == null)
            continue;

          autoFix.incrementAndGet();
          try {
            database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT
          } catch (final Throwable e) {
            warnings.add("Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
        }
      }

      if (verboseLevel > 0)
        for (final String warning : warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      stats.put("autoFix", autoFix.get());
      stats.put("corruptedRecords", corruptedRecords);
      stats.put("invalidLinks", invalidLinks.get());
      stats.put("warnings", warnings);
    }

    return stats;
  }

  private void reconnectEdges(Set<RID> reconnectOutEdges, Set<RID> reconnectInEdges, List<String> warnings,
      Map<String, Object> stats) {
    // BROWSE ALL THE EDGES AND COLLECT THE ONES PART OF THE RECONNECTION
    final List<EdgeType> edgeTypes = new ArrayList<>();
    for (DocumentType schemaType : database.getSchema().getTypes()) {
      if (schemaType instanceof EdgeType t)
        edgeTypes.add(t);
    }

    final List<Edge> outEdgesToReconnect = new ArrayList<>();
    final List<Edge> inEdgesToReconnect = new ArrayList<>();

    for (EdgeType edgeType : edgeTypes) {
      final SelectIterator<Edge> edges = database.select().fromType(edgeType.getName()).edges();
      while (edges.hasNext()) {
        final Edge e = edges.next();
        if (reconnectOutEdges.contains(e.getOut()))
          outEdgesToReconnect.add(e);
        if (reconnectInEdges.contains(e.getIn()))
          inEdgesToReconnect.add(e);
      }
    }

    if (!outEdgesToReconnect.isEmpty()) {
      for (Edge e : outEdgesToReconnect) {
        final MutableVertex vertex = e.getOutVertex().modify();
        final EdgeSegment outChunk = graphEngine.createOutEdgeChunk(vertex);
        final EdgeLinkedList outLinkedList = new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, outChunk);
        outLinkedList.add(e.getIdentity(), e.getIn());
      }
      warnings.add("reconnected " + outEdgesToReconnect.size() + " outgoing edges");
      stats.put("outEdgesToReconnect", outEdgesToReconnect);
    }

    if (!inEdgesToReconnect.isEmpty()) {
      for (Edge e : inEdgesToReconnect) {
        final MutableVertex vertex = e.getInVertex().modify();
        final EdgeSegment inChunk = graphEngine.createInEdgeChunk(vertex);
        final EdgeLinkedList inLinkedList = new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, inChunk);
        inLinkedList.add(e.getIdentity(), e.getOut());
      }
      warnings.add("reconnected " + inEdgesToReconnect.size() + " incoming edges");
      stats.put("inEdgesToReconnect", inEdgesToReconnect);
    }
  }

  private void checkIncomingEdges(boolean fix, Vertex vertex, List<String> warnings, RID vertexIdentity, AtomicLong invalidLinks,
      LinkedHashSet<RID> corruptedRecords, Set<RID> reconnectInEdges) {
    if (((VertexInternal) vertex).getInEdgesHeadChunk() != null) {
      EdgeLinkedList inEdges = null;
      try {
        inEdges = graphEngine.getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.IN);
      } catch (Exception e) {
        // IGNORE IT
      }

      if (inEdges == null) {
        if (fix) {
          vertex = vertex.modify();
          ((VertexInternal) vertex).setInEdgesHeadChunk(null);
          ((MutableVertex) vertex).save();
          warnings.add("vertex " + vertexIdentity + " in edges record " + ((VertexInternal) vertex).getInEdgesHeadChunk()
              + " is not valid, removing it");
        }
      } else {
        final Iterator<Pair<RID, RID>> in = inEdges.entryIterator();
        while (in.hasNext()) {
          try {
            final Pair<RID, RID> current = in.next();
            final RID edgeRID = current.getFirst();
            final RID vertexRID = current.getSecond();

            boolean removeEntry = false;

            if (edgeRID == null) {
              warnings.add("outgoing edge null from vertex " + vertexIdentity);
              removeEntry = true;
              invalidLinks.incrementAndGet();
            } else if (vertexRID == null) {
              warnings.add("outgoing vertex null from vertex " + vertexIdentity);
              corruptedRecords.add(edgeRID);
              removeEntry = true;
              invalidLinks.incrementAndGet();
            } else {
              if (edgeRID.getPosition() < 0)
                // LIGHTWEIGHT EDGE
                continue;

              try {
                final Edge edge = edgeRID.asEdge(true);

                VertexInternal inVertex = null;

                if (edge.getOut() == null || !edge.getOut().isValid()) {
                  warnings.add("edge " + edgeRID + " has an invalid outgoing link " + edge.getIn());
                  corruptedRecords.add(edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
                } else {
                  try {
                    inVertex = (VertexInternal) edge.getOutVertex().asVertex(true);
                  } catch (final RecordNotFoundException e) {
                    warnings.add("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut()
                        + " that is not found (deleted?)");
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    corruptedRecords.add(edge.getOut());
                    invalidLinks.incrementAndGet();
                  } catch (final Exception e) {
                    // UNKNOWN ERROR ON LOADING
                    warnings.add("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut()
                        + " which cannot be loaded (error: " + e.getMessage() + ")");
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    corruptedRecords.add(edge.getOut());
                  }
                }

                if (!edge.getIn().equals(vertexIdentity)) {
                  warnings.add("edge " + edgeRID + " has an incoming link " + edge.getIn() + " different from expected "
                      + vertexIdentity);

                  // CHECK ALL INCOMING EDGES
                  int totalEdges = 0;
                  int totalEdgesOk = 0;
                  int totalEdgesError = 0;
                  int totalEdgesErrorFromSameVertex = 0;
                  final Iterator<Pair<RID, RID>> inEdgeIterator = inEdges.entryIterator();
                  while (inEdgeIterator.hasNext()) {
                    final Pair<RID, RID> nextEntry = inEdgeIterator.next();

                    ++totalEdges;

                    final RID edgeIn = nextEntry.getFirst().asEdge(true).getIn();
                    if (edgeIn.equals(vertexIdentity))
                      ++totalEdgesOk;
                    else if (edgeIn.equals(edge.getIn()))
                      ++totalEdgesErrorFromSameVertex;
                    else
                      ++totalEdgesError;
                  }
                  warnings.add("edge " + edgeRID + " has an incoming link " + edge.getOut() + " different from expected "
                      + vertexIdentity + ". Found " + totalEdges + " edges, of which " + totalEdgesOk + " are correct, "
                      + totalEdgesErrorFromSameVertex + " are from the same vertex and " + totalEdgesError
                      + " are different");

                  if (totalEdges == totalEdgesErrorFromSameVertex) {
                    // ORIGINAL OUT VERTEX POINTER MUST BE WRONG, CHECKING
                    final VertexInternal wrongInVertex = (VertexInternal) edge.getIn().asVertex(false);
                    if (((VertexInternal) vertex).getInEdgesHeadChunk().equals(wrongInVertex.getInEdgesHeadChunk())) {
                      // CURRENT VERTEX POINTS TO ANOTHER LINKED LIST. SEARCHING FOR ITS CORRECT LINKED LIST LATER
                      reconnectInEdges.add(vertexIdentity);

                      // RESET POINTER TO OUT EDGES
                      vertex = vertex.modify();
                      ((VertexInternal) vertex).setInEdgesHeadChunk(null);
                      ((MutableVertex) vertex).save();

                      // SKIP THE REST OF THE EDGES
                      break;
                    } else {
                      corruptedRecords.add(edgeRID);
                      removeEntry = true;
                      invalidLinks.incrementAndGet();
                    }
                  } else {
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  }

                } else if (!edge.getOut().equals(vertexRID)) {
                  warnings.add(
                      "edge " + edgeRID + " has an outgoing link " + edge.getOut() + " different from expected " + vertexRID);
                  corruptedRecords.add(edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
                }

                if (inVertex != null && !inVertex.isConnectedTo(vertexIdentity, Vertex.DIRECTION.OUT, edge.getTypeName())) {
                  warnings.add(
                      "edge " + edgeRID + " was not connected from the incoming vertex " + edge.getOut() + " to the vertex " +
                          vertexIdentity);
                  if (fix) {
                    inVertex = inVertex.modify();
                    database.getGraphEngine().connectOutgoingEdge(inVertex, vertexIdentity, edge);
                    ((MutableVertex) inVertex).save();
                  }
                }

              } catch (final RecordNotFoundException e) {
                warnings.add("edge " + edgeRID + " not found");
                corruptedRecords.add(edgeRID);
                removeEntry = true;
                invalidLinks.incrementAndGet();
              } catch (final Exception e) {
                // UNKNOWN ERROR ON LOADING
                warnings.add("edge " + edgeRID + " error on loading (error: " + e.getMessage() + ")");
                corruptedRecords.add(edgeRID);
                removeEntry = true;
              }
            }

            if (fix && removeEntry)
              in.remove();
          } catch (Exception e) {
            // UNKNOWN ERROR ON LOADING EDGES
            warnings.add(
                "error on loading incoming edges from vertex " + vertexIdentity + " (error: " + e.getMessage() + ")");

            if (fix) {
              vertex = vertex.modify();
              ((VertexInternal) vertex).setInEdgesHeadChunk(null);
              ((MutableVertex) vertex).save();
              warnings.add("vertex " + vertexIdentity + " in edges record " + ((VertexInternal) vertex).getInEdgesHeadChunk()
                  + " is not valid, removing it");
            }
            break;
          }
        }
      }
    }
  }

  private Vertex checkOutgoingEdges(final boolean fix, Vertex vertex, final List<String> warnings, final RID vertexIdentity,
      final AtomicLong invalidLinks, final LinkedHashSet<RID> corruptedRecords, final Set<RID> reconnectOutEdges) {
    // CHECK THE EDGE IS CONNECTED FROM THE OTHER SIDE
    if (((VertexInternal) vertex).getOutEdgesHeadChunk() != null) {
      EdgeLinkedList outEdges = null;
      try {
        outEdges = graphEngine.getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.OUT);
      } catch (Exception e) {
        // IGNORE IT
      }

      if (outEdges == null) {
        if (fix) {
          vertex = vertex.modify();
          ((VertexInternal) vertex).setOutEdgesHeadChunk(null);
          ((MutableVertex) vertex).save();
          warnings.add("vertex " + vertexIdentity + " out edges record " + ((VertexInternal) vertex).getOutEdgesHeadChunk()
              + " is not valid, removing it");
        }
      } else {

        final Iterator<Pair<RID, RID>> out = outEdges.entryIterator();
        while (out.hasNext()) {
          try {
            final Pair<RID, RID> current = out.next();
            final RID edgeRID = current.getFirst();
            final RID vertexRID = current.getSecond();

            boolean removeEntry = false;

            VertexInternal outVertex = null;

            if (edgeRID == null) {
              warnings.add("outgoing edge null from vertex " + vertexIdentity);
              removeEntry = true;
              invalidLinks.incrementAndGet();
            } else if (vertexRID == null) {
              warnings.add("outgoing vertex null from vertex " + vertexIdentity);
              corruptedRecords.add(edgeRID);
              removeEntry = true;
              invalidLinks.incrementAndGet();
            } else {
              try {
                if (edgeRID.getPosition() < 0)
                  // LIGHTWEIGHT EDGE
                  continue;

                final Edge edge = edgeRID.asEdge(true);

                if (edge.getIn() == null || !edge.getIn().isValid()) {
                  warnings.add("edge " + edgeRID + " has an invalid incoming link " + edge.getIn());
                  corruptedRecords.add(edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
                } else {
                  try {
                    outVertex = (VertexInternal) edge.getInVertex().asVertex(true);
                  } catch (final RecordNotFoundException e) {
                    warnings.add("edge " + edgeRID + " points to the incoming vertex " + edge.getIn()
                        + " that is not found (deleted?)");
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    corruptedRecords.add(edge.getIn());
                    invalidLinks.incrementAndGet();
                  } catch (final Exception e) {
                    // UNKNOWN ERROR ON LOADING
                    warnings.add("edge " + edgeRID + " points to the incoming vertex " + edge.getIn()
                        + " which cannot be loaded (error: " + e.getMessage() + ")");
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    corruptedRecords.add(edge.getIn());
                  }
                }

                if (!edge.getOut().equals(vertexIdentity)) {
                  // CHECK ALL OUT EDGES
                  int totalEdges = 0;
                  int totalEdgesOk = 0;
                  int totalEdgesError = 0;
                  int totalEdgesErrorFromSameVertex = 0;

                  final Iterator<Pair<RID, RID>> outEdgesIterator = outEdges.entryIterator();
                  while (outEdgesIterator.hasNext()) {
                    final Pair<RID, RID> nextEntry = outEdgesIterator.next();

                    ++totalEdges;

                    final RID edgeOut = nextEntry.getFirst().asEdge(true).getOut();
                    if (edgeOut.equals(vertexIdentity))
                      ++totalEdgesOk;
                    else if (edgeOut.equals(edge.getOut()))
                      ++totalEdgesErrorFromSameVertex;
                    else
                      ++totalEdgesError;
                  }
                  warnings.add("edge " + edgeRID + " has an outgoing link " + edge.getOut() + " different from expected "
                      + vertexIdentity + ". Found " + totalEdges + " edges, of which " + totalEdgesOk + " are correct, "
                      + totalEdgesErrorFromSameVertex + " are from the same vertex and " + totalEdgesError
                      + " are different");

                  if (totalEdges == totalEdgesErrorFromSameVertex) {
                    // ORIGINAL OUT VERTEX POINTER MUST BE WRONG, CHECKING
                    final VertexInternal wrongOutVertex = (VertexInternal) edge.getOut().asVertex(false);
                    if (((VertexInternal) vertex).getOutEdgesHeadChunk().equals(wrongOutVertex.getOutEdgesHeadChunk())) {
                      // CURRENT VERTEX POINTS TO ANOTHER LINKED LIST. SEARCHING FOR ITS CORRECT LINKED LIST LATER
                      reconnectOutEdges.add(vertexIdentity);

                      // RESET POINTER TO OUT EDGES
                      vertex = vertex.modify();
                      ((VertexInternal) vertex).setOutEdgesHeadChunk(null);
                      ((MutableVertex) vertex).save();

                      // SKIP THE REST OF THE EDGES
                      break;

                    } else {
                      corruptedRecords.add(edgeRID);
                      removeEntry = true;
                      invalidLinks.incrementAndGet();
                    }
                  } else {
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  }

                } else if (!edge.getIn().equals(vertexRID)) {
                  warnings.add(
                      "edge " + edgeRID + " has an incoming link " + edge.getIn() + " different from expected " + vertexRID);
                  corruptedRecords.add(edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
                }

                // CHECK THE EDGE IS CONNECTED FROM THE OTHER SIDE
                if (outVertex != null && !outVertex.isConnectedTo(vertexIdentity, Vertex.DIRECTION.IN, edge.getTypeName())) {
                  warnings.add(
                      "edge " + edgeRID + " was not connected from the outgoing vertex " + edge.getIn()
                          + " back to the vertex "
                          + vertexIdentity);
                  if (fix) {
                    outVertex = outVertex.modify();
                    database.getGraphEngine().connectIncomingEdge(outVertex, vertexIdentity, edgeRID);
                    ((MutableVertex) outVertex).save();
                  }
                }

              } catch (final RecordNotFoundException e) {
                warnings.add("edge " + edgeRID + " not found");
                corruptedRecords.add(edgeRID);
                removeEntry = true;
                invalidLinks.incrementAndGet();
              } catch (final Exception e) {
                // UNKNOWN ERROR ON LOADING
                warnings.add("edge " + edgeRID + " error on loading (error: " + e.getMessage() + ")");
                corruptedRecords.add(edgeRID);
                removeEntry = true;
              }
            }

            if (fix && removeEntry)
              out.remove();

          } catch (Exception e) {
            // UNKNOWN ERROR ON LOADING EDGES
            warnings.add(
                "error on loading outgoing edges from vertex " + vertexIdentity + " (error: " + e.getMessage() + ")");

            if (fix) {
              vertex = vertex.modify();
              ((VertexInternal) vertex).setOutEdgesHeadChunk(null);
              ((MutableVertex) vertex).save();
              warnings.add(
                  "vertex " + vertexIdentity + " out edges record " + ((VertexInternal) vertex).getOutEdgesHeadChunk()
                      + " is not valid, removing it");
            }
            break;
          }
        }
      }
    }
    return vertex;
  }

  public Map<String, Object> checkEdges(final String typeName, final boolean fix, final int verboseLevel) {
    final AtomicLong autoFix = new AtomicLong();
    final AtomicLong invalidLinks = new AtomicLong();
    final AtomicLong missingReferenceBack = new AtomicLong();
    final List<RID> corruptedRecords = new ArrayList<>();
    final List<String> warnings = new ArrayList<>();

    final Map<String, Object> stats = new HashMap<>();

    database.begin();

    try {
      // CHECK RECORD IS OF THE RIGHT TYPE
      final DocumentType type = database.getSchema().getType(typeName);
      for (final Bucket b : type.getBuckets(false)) {
        b.scan((rid, view) -> {
          try {
            final Record record = database.getRecordFactory().newImmutableRecord(database, type, rid, view, null);
            record.asEdge(true);
          } catch (Exception e) {
            warnings.add("edge " + rid + " cannot be loaded, removing it");
            corruptedRecords.add(rid);
          }
          return true;
        }, null);
      }

      database.scanType(typeName, false, (record) -> {
        final RID edgeRID = record.getIdentity();

        try {
          final Edge edge = record.asEdge(true);

          if (edge == null) {
            warnings.add("edge " + edgeRID + " cannot be loaded");
            corruptedRecords.add(edgeRID);

          } else if (edge.getIn() == null || !edge.getIn().isValid()) {
            warnings.add("edge " + edgeRID + " has an invalid incoming link " + edge.getIn());
            corruptedRecords.add(edgeRID);
            invalidLinks.incrementAndGet();

          } else if (edge.getOut() == null || !edge.getOut().isValid()) {
            warnings.add("edge " + edgeRID + " has an invalid outgoing link " + edge.getOut());
            corruptedRecords.add(edgeRID);
            invalidLinks.incrementAndGet();

          } else {
            try {
              final Vertex vertex = edge.getInVertex().asVertex(true);

              final EdgeLinkedList inEdges = graphEngine.getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.IN);
              if (inEdges == null || !inEdges.containsEdge(edgeRID))
                // UNI DIRECTIONAL EDGE
                missingReferenceBack.incrementAndGet();

            } catch (final RecordNotFoundException e) {
              warnings.add("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " that is not found (deleted?)");
              corruptedRecords.add(edgeRID);
              corruptedRecords.add(edge.getIn());
              invalidLinks.incrementAndGet();
            } catch (final Exception e) {
              // UNKNOWN ERROR ON LOADING
              warnings.add("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " which cannot be loaded (error: "
                  + e.getMessage() + ")");
              corruptedRecords.add(edgeRID);
              corruptedRecords.add(edge.getIn());
            }

            try {
              final Vertex vertex = edge.getOutVertex().asVertex(true);

              final EdgeLinkedList outEdges = graphEngine.getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.OUT);
              if (outEdges == null || !outEdges.containsEdge(edgeRID))
                // UNI DIRECTIONAL EDGE
                missingReferenceBack.incrementAndGet();

            } catch (final RecordNotFoundException e) {
              warnings.add("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " that is not found (deleted?)");
              corruptedRecords.add(edgeRID);
              invalidLinks.incrementAndGet();
            } catch (final Exception e) {
              // UNKNOWN ERROR ON LOADING
              warnings.add(
                  "edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " which cannot be loaded (error: "
                      + e.getMessage() + ")");
              corruptedRecords.add(edgeRID);
              corruptedRecords.add(edge.getOut());
            }
          }

        } catch (final Throwable e) {
          warnings.add("edge " + record.getIdentity() + " cannot be loaded (error: " + e.getMessage() + ")");
          corruptedRecords.add(edgeRID);
        }

        return true;
      }, (rid, exception) -> {
        warnings.add("edge " + rid + " cannot be loaded (error: " + exception.getMessage() + ")");
        corruptedRecords.add(rid);
        return true;
      });

      if (fix) {
        for (final RID rid : corruptedRecords) {
          if (rid == null)
            continue;

          autoFix.incrementAndGet();
          try {
            database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT
          } catch (final Throwable e) {
            warnings.add("Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
        }
      }

      if (verboseLevel > 0)
        for (final String warning : warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      stats.put("autoFix", autoFix.get());
      stats.put("corruptedRecords", corruptedRecords);
      stats.put("invalidLinks", invalidLinks.get());
      stats.put("missingReferenceBack", missingReferenceBack.get());
      stats.put("warnings", warnings);
    }

    return stats;
  }
}
