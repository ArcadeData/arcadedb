package com.arcadedb.gremlin;

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * ArcadeDB's optimized version to count vertices and edges
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ArcadeCountGlobalStep<S extends Element> extends AbstractStep<S, Long> {
  private final Class<S> elementClass;
  private final String   typeName;
  private       boolean  done = false;

  public ArcadeCountGlobalStep(final Traversal.Admin traversal, final Class<S> elementClass, final String typeName) {
    super(traversal);
    this.elementClass = elementClass;
    this.typeName = typeName;
  }

  protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
    if (!this.done) {
      this.done = true;
      final ArcadeGraph graph = (ArcadeGraph) this.getTraversal().getGraph().get();

      long total = 0L;

      boolean startTx = !graph.database.isTransactionActive();
      if (startTx)
        graph.database.begin();

      try {
        if (typeName != null)
          total += graph.database.countType(typeName, false);
        else if (Vertex.class.isAssignableFrom(this.elementClass)) {
          for (DocumentType type : graph.database.getSchema().getTypes()) {
            if (type instanceof VertexType)
              total += graph.database.countType(type.getName(), false);
          }
        } else {
          for (DocumentType type : graph.database.getSchema().getTypes()) {
            if (type instanceof EdgeType)
              total += graph.database.countType(type.getName(), false);
          }
        }
      } finally {
        if (startTx)
          graph.database.commit();
      }

      return this.getTraversal().getTraverserGenerator().generate(total, (Step) this, 1L);
    } else {
      throw FastNoSuchElementException.instance();
    }
  }

  public String toString() {
    return StringFactory.stepString(this, this.elementClass.getSimpleName().toLowerCase());
  }

  public int hashCode() {
    return super.hashCode() ^ this.elementClass.hashCode();
  }

  public void reset() {
    this.done = false;
  }
}
