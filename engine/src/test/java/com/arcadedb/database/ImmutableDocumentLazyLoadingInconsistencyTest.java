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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Test to verify the fix for issue #2560 - ImmutableDocument.checkForLazyLoading() consistency.
 * <p>
 * This test verifies that after the fix, the get() method now behaves consistently with other methods
 * (has(), modify(), toJSON(), toMap(), getPropertyNames()) by properly propagating exceptions thrown
 * during lazy loading instead of catching them and returning null.
 * <p>
 * The fix ensures that permission checking workflows that depend on exceptions being thrown during
 * lazy loading now work correctly with the get() method.
 */
public class ImmutableDocumentLazyLoadingInconsistencyTest extends TestHelper {

  /**
   * Custom RuntimeException to simulate permission denied scenarios.
   */
  public static class PermissionDeniedException extends RuntimeException {
    public PermissionDeniedException(String message) {
      super(message);
    }
  }

  /**
   * Test listener that simulates a security check that denies access.
   */
  public static class SecurityCheckListener implements AfterRecordReadListener {
    private final boolean shouldDenyAccess;
    private final String  propertyName;

    public SecurityCheckListener(boolean shouldDenyAccess, String propertyName) {
      this.shouldDenyAccess = shouldDenyAccess;
      this.propertyName = propertyName;
    }

    @Override
    public Record onAfterRead(Record record) {
      if (shouldDenyAccess) {
        throw new PermissionDeniedException("Access denied to property: " + propertyName);
      }
      return record;
    }
  }

  @Override
  public void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("SecurityTest");
      type.createProperty("publicProperty", Type.STRING);
      type.createProperty("secretProperty", Type.STRING);
    });
  }

  @Test
  void lazyLoadingConsistencyWithSecurityException() {
    database.transaction(() -> {
      // Create a document with both public and secret properties
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.set("secretProperty", "secret_value");
      doc.save();

      final RID documentRid = doc.getIdentity();

      // Register a security listener that denies access when reading records
      final SecurityCheckListener securityListener = new SecurityCheckListener(true, "secretProperty");
      database.getEvents().registerListener(securityListener);

      try {
        // Test with an ImmutableDocument created by reading from DB (lazy loading will occur on first access)
        database.commit();
        database.begin();

        // Get the document as ImmutableDocument (it will have buffer=null initially)
        final ImmutableDocument immutableDoc = (ImmutableDocument) database.lookupByRID(documentRid, false);

        // Test all methods that should trigger lazy loading and p ropagate the exception
        Stream.<Consumer<ImmutableDocument>>of(
            d -> d.has("secretProperty"),
            ImmutableDocument::modify,
            ImmutableDocument::toJSON,
            ImmutableDocument::toMap,
            ImmutableDocument::getPropertyNames,
            ImmutableDocument::propertiesAsMap,
            d -> d.propertiesAsMap("secretProperty"),
            d -> d.get("secretProperty")
        ).forEach(action -> {
          // Get a new instance for each test to ensure lazy loading is triggered
          final ImmutableDocument freshDoc = (ImmutableDocument) database.lookupByRID(documentRid, false);
          assertThatThrownBy(() -> action.accept(freshDoc))
              .isInstanceOf(PermissionDeniedException.class)
              .hasMessage("Access denied to property: secretProperty");
        });

      } finally {
        // Clean up the security listener
        database.getEvents().unregisterListener(securityListener);
      }
    });
  }

  @Test
  void lazyLoadingConsistencyWithoutSecurityException() {
    database.transaction(() -> {
      // Create a document
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.set("secretProperty", "secret_value");
      doc.save();

      final RID documentRid = doc.getIdentity();

      // Register a security listener that allows access
      final SecurityCheckListener securityListener = new SecurityCheckListener(false, "secretProperty");
      database.getEvents().registerListener(securityListener);

      try {
        database.commit();
        database.begin();

        // All methods should work normally when security check passes
        final ImmutableDocument immutableDoc1 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThat(immutableDoc1.has("secretProperty")).isTrue();

        final ImmutableDocument immutableDoc2 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThat(immutableDoc2.get("secretProperty")).isEqualTo("secret_value");

        final ImmutableDocument immutableDoc3 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThat(immutableDoc3.modify()).isNotNull();

        final ImmutableDocument immutableDoc4 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThat(immutableDoc4.toJSON().has("secretProperty")).isTrue();

        final ImmutableDocument immutableDoc5 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThat(immutableDoc5.toMap()).containsKey("secretProperty");

        final ImmutableDocument immutableDoc6 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThat(immutableDoc6.getPropertyNames()).contains("secretProperty");

        final ImmutableDocument immutableDoc7 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThat(immutableDoc7.propertiesAsMap()).containsEntry("secretProperty", "secret_value");

        final ImmutableDocument immutableDoc8 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThat(immutableDoc8.propertiesAsMap("secretProperty")).containsEntry("secretProperty", "secret_value");

      } finally {
        // Clean up the security listener
        database.getEvents().unregisterListener(securityListener);
      }
    });
  }

  @Test
  void consistentExceptionTypesInLazyLoading() {
    database.transaction(() -> {
      // Create a document
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.save();

      final RID documentRid = doc.getIdentity();

      // Test with different exception types
      final AfterRecordReadListener runtimeExceptionListener = new AfterRecordReadListener() {
        @Override
        public Record onAfterRead(Record record) {
          throw new RuntimeException("General runtime error");
        }
      };

      final AfterRecordReadListener illegalStateListener = new AfterRecordReadListener() {
        @Override
        public Record onAfterRead(Record record) {
          throw new IllegalStateException("Invalid state for access");
        }
      };

      // Test with RuntimeException
      database.getEvents().registerListener(runtimeExceptionListener);
      try {
        database.commit();
        database.begin();

        final ImmutableDocument immutableDoc1 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        // has() should propagate RuntimeException
        assertThatThrownBy(() -> immutableDoc1.has("publicProperty"))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("General runtime error");

        // get() now correctly propagates RuntimeException (FIXED)
        final ImmutableDocument immutableDoc2 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThatThrownBy(() -> immutableDoc2.get("publicProperty"))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("General runtime error");

      } finally {
        database.getEvents().unregisterListener(runtimeExceptionListener);
      }

      // Test with IllegalStateException
      database.getEvents().registerListener(illegalStateListener);
      try {
        database.commit();
        database.begin();

        final ImmutableDocument immutableDoc3 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        // has() should propagate IllegalStateException
        assertThatThrownBy(() -> immutableDoc3.has("publicProperty"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Invalid state for access");

        // get() now correctly propagates IllegalStateException (FIXED)
        final ImmutableDocument immutableDoc4 = (ImmutableDocument) database.lookupByRID(documentRid, false);
        assertThatThrownBy(() -> immutableDoc4.get("publicProperty"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Invalid state for access");

      } finally {
        database.getEvents().unregisterListener(illegalStateListener);
      }
    });
  }

  /**
   * Regression for issue #5723: {@code propertiesAsMap()} was the one accessor that did not lazy-load, so a record
   * handed out by a scan - never materialised - answered an EMPTY map instead of its properties. Silently: no
   * exception, no log, just a record that looks like it has nothing on it. {@code copyType()} read every record that
   * way and copied their emptiness.
   */
  @Test
  void propertiesAsMapLoadsARecordComingStraightFromAScan() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.set("secretProperty", "secret_value");
      doc.save();
    });

    database.transaction(() -> {
      final Iterator<Record> iterator = database.iterateType("SecurityTest", false);
      assertThat(iterator.hasNext()).isTrue();
      final Document scanned = (Document) iterator.next();

      assertThat(scanned.propertiesAsMap())//
          .containsEntry("publicProperty", "public_value")//
          .containsEntry("secretProperty", "secret_value");
    });

    database.transaction(() -> {
      final ImmutableDocument scanned = (ImmutableDocument) database.iterateType("SecurityTest", false).next();
      assertThat(scanned.propertiesAsMap("publicProperty")).containsExactly(entry("publicProperty", "public_value"));
    });
  }

  /**
   * Regression for issue #5755: when an after-read listener returns a DIFFERENT record,
   * {@code checkForLazyLoading()} replaces the buffer with a freshly serialised one. If that replacement is not
   * positioned at the properties, every accessor that reads straight after the lazy load - {@code toJSON()},
   * {@code getPropertyNames()}, {@code toMap()}, {@code has()}, {@code get()} - consumes the record-type byte as the
   * first byte of the header size and answers nonsense.
   * <p>
   * The listener here returns a DIRTY {@link MutableDocument}, which is what sends
   * {@code BinarySerializer.serializeDocument()} through {@code serializeProperties()} and its closing
   * {@code header.flip()} - the buffer comes back at 0. The in-tree encryption listener
   * ({@link com.arcadedb.event.RecordEncryptionTest}) takes exactly that branch too, but on a VERTEX, where
   * {@code ImmutableVertex.checkForLazyLoading()} re-parses the edge pointers from position 1 and repositions as a
   * side effect. Plain documents have no such second pass, which is why the gap survived unnoticed.
   */
  @Test
  void afterReadListenerReturningADifferentRecordLeavesTheBufferAtTheProperties() {
    final RID[] documentRid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.set("secretProperty", "encrypted_value");
      doc.save();
      documentRid[0] = doc.getIdentity();
    });

    // MIMICS THE ENCRYPTION HOOK: HAND BACK A DIFFERENT, DIRTY RECORD
    final AfterRecordReadListener rewriter = record -> ((Document) record).modify().set("secretProperty", "decrypted_value");

    database.getSchema().getType("SecurityTest").getEvents().registerListener(rewriter);
    try {
      database.transaction(() -> {
        final Document scanned = (Document) database.iterateType("SecurityTest", false).next();
        assertThat(scanned.toJSON().getString("secretProperty")).isEqualTo("decrypted_value");
      });

      database.transaction(() -> {
        final Document scanned = (Document) database.iterateType("SecurityTest", false).next();
        assertThat(scanned.getPropertyNames()).containsExactlyInAnyOrder("publicProperty", "secretProperty");
      });

      database.transaction(() -> {
        final Document scanned = (Document) database.iterateType("SecurityTest", false).next();
        assertThat(scanned.toMap(false)).containsEntry("publicProperty", "public_value")
            .containsEntry("secretProperty", "decrypted_value");
      });

      database.transaction(() -> {
        final Document scanned = (Document) database.iterateType("SecurityTest", false).next();
        assertThat(scanned.has("secretProperty")).isTrue();
      });

      database.transaction(() -> {
        final Document scanned = (Document) database.iterateType("SecurityTest", false).next();
        assertThat(scanned.get("secretProperty")).isEqualTo("decrypted_value");
      });

      database.transaction(() -> {
        final Document scanned = (Document) database.iterateType("SecurityTest", false).next();
        assertThat(scanned.propertiesAsMap()).containsEntry("secretProperty", "decrypted_value");
      });

      // AND THE SAME RECORD REACHED BY RID, NOT BY SCAN
      database.transaction(() -> {
        final Document loaded = (Document) database.lookupByRID(documentRid[0], false);
        assertThat(loaded.toJSON().getString("secretProperty")).isEqualTo("decrypted_value");
      });
    } finally {
      database.getSchema().getType("SecurityTest").getEvents().unregisterListener(rewriter);
    }
  }

  /**
   * Second half of issue #5755, on the same line: the buffer handed back by {@code BinarySerializer.serialize()} for a
   * DIRTY record is {@code DatabaseContext.getTemporaryBuffer1()} - a per-thread scratch buffer that every subsequent
   * serialization {@code clear()}s and overwrites. Keeping it as the record's own buffer means the record's content is
   * silently rewritten by the next unrelated save on the same thread. The record must own a private copy.
   */
  @Test
  void afterReadListenerReturningADifferentRecordDoesNotShareTheSerializerScratchBuffer() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.set("secretProperty", "encrypted_value");
      doc.save();
    });

    final AfterRecordReadListener rewriter = record -> ((Document) record).modify().set("secretProperty", "decrypted_value");

    database.getSchema().getType("SecurityTest").getEvents().registerListener(rewriter);
    try {
      database.transaction(() -> {
        final Document scanned = (Document) database.iterateType("SecurityTest", false).next();
        // MATERIALISE IT: FROM HERE ON THE RECORD READS FROM ITS OWN BUFFER
        assertThat(scanned.get("secretProperty")).isEqualTo("decrypted_value");

        // ANY OTHER SERIALIZATION ON THIS THREAD REUSES THE SAME SCRATCH BUFFER
        final MutableDocument other = database.newDocument("SecurityTest");
        other.set("publicProperty", "0123456789012345678901234567890123456789");
        other.set("secretProperty", "9876543210987654321098765432109876543210");
        other.save();

        assertThat(scanned.get("secretProperty")).isEqualTo("decrypted_value");
        assertThat(scanned.get("publicProperty")).isEqualTo("public_value");
        assertThat(scanned.propertiesAsMap()).containsEntry("secretProperty", "decrypted_value");
      });
    } finally {
      database.getSchema().getType("SecurityTest").getEvents().unregisterListener(rewriter);
    }
  }

  /**
   * The vertex shape of the same two defects. {@code ImmutableVertex} inherits
   * {@code ImmutableDocument.checkForLazyLoading()} and only re-reads its fixed edge-pointer prefix afterwards, so it
   * was shielded from the wrong buffer POSITION but not from aliasing the serializer's scratch buffer - and the
   * encryption recipe this hook exists for ({@link com.arcadedb.event.RecordEncryptionTest}) is written on vertices.
   */
  @Test
  void afterReadListenerRewritingAVertexIsAlsoIsolatedFromTheScratchBuffer() {
    database.transaction(() -> database.getSchema().createVertexType("SecurityVertex"));

    final RID[] vertexRid = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("SecurityVertex");
      v.set("publicProperty", "public_value");
      v.set("secretProperty", "encrypted_value");
      v.save();
      vertexRid[0] = v.getIdentity();
    });

    final AfterRecordReadListener rewriter = record -> record.asVertex().modify().set("secretProperty", "decrypted_value");

    database.getSchema().getType("SecurityVertex").getEvents().registerListener(rewriter);
    try {
      database.transaction(() -> {
        final Vertex v = (Vertex) database.lookupByRID(vertexRid[0], false);
        assertThat(v.getString("secretProperty")).isEqualTo("decrypted_value");
        assertThat(v.toJSON().getString("secretProperty")).isEqualTo("decrypted_value");

        // ANY OTHER SERIALIZATION ON THIS THREAD REUSES THE SAME SCRATCH BUFFER
        final MutableVertex other = database.newVertex("SecurityVertex");
        other.set("publicProperty", "0123456789012345678901234567890123456789");
        other.set("secretProperty", "9876543210987654321098765432109876543210");
        other.save();

        assertThat(v.getString("secretProperty")).isEqualTo("decrypted_value");
        assertThat(v.getString("publicProperty")).isEqualTo("public_value");
      });
    } finally {
      database.getSchema().getType("SecurityVertex").getEvents().unregisterListener(rewriter);
    }
  }

  /**
   * Third defect on the same contract, found while fixing #5755: {@link BaseRecord#reload()} handles the very same "the after-read listener returned a
   * different record" case as {@code checkForLazyLoading()}, but took the record's own {@code getBuffer()} instead of
   * serialising it. On a dirty {@link MutableDocument} that buffer still holds the PRE-modification content, so a
   * reload silently threw the listener's work away and handed back the raw stored value - ciphertext, for the
   * encryption recipe this hook exists for. It is also plainly {@code null} for a record the listener built from
   * scratch rather than by {@code modify()}, which was an NPE.
   */
  @Test
  void reloadKeepsWhatTheAfterReadListenerReturned() {
    final RID[] documentRid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.set("secretProperty", "encrypted_value");
      doc.save();
      documentRid[0] = doc.getIdentity();
    });

    final AfterRecordReadListener rewriter = record -> ((Document) record).modify().set("secretProperty", "decrypted_value");

    database.getSchema().getType("SecurityTest").getEvents().registerListener(rewriter);
    try {
      database.transaction(() -> {
        final ImmutableDocument doc = (ImmutableDocument) database.lookupByRID(documentRid[0], false);
        assertThat(doc.get("secretProperty")).isEqualTo("decrypted_value");

        doc.reload();

        assertThat(doc.get("secretProperty")).isEqualTo("decrypted_value");
        assertThat(doc.get("publicProperty")).isEqualTo("public_value");
        assertThat(doc.toJSON().getString("secretProperty")).isEqualTo("decrypted_value");
      });
    } finally {
      database.getSchema().getType("SecurityTest").getEvents().unregisterListener(rewriter);
    }
  }

  /**
   * The other half of defect 3: a listener that builds its replacement FROM SCRATCH rather than by {@code modify()}
   * hands back a record whose {@code getBuffer()} is plainly {@code null}, so the old {@code getBuffer().copy()} was a
   * straight NPE. Every other test here goes through {@code modify()} and so only pins the stale-content half.
   */
  @Test
  void reloadAcceptsARecordTheAfterReadListenerBuiltFromScratch() {
    final RID[] documentRid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.set("secretProperty", "encrypted_value");
      doc.save();
      documentRid[0] = doc.getIdentity();
    });

    // NEVER PERSISTED, SO IT CARRIES NO BUFFER AT ALL
    final AfterRecordReadListener rewriter = record -> database.newDocument("SecurityTest")
        .set("publicProperty", "public_value")
        .set("secretProperty", "rebuilt_value");

    database.getSchema().getType("SecurityTest").getEvents().registerListener(rewriter);
    try {
      database.transaction(() -> {
        final ImmutableDocument doc = (ImmutableDocument) database.lookupByRID(documentRid[0], false);
        assertThat(doc.get("secretProperty")).isEqualTo("rebuilt_value");

        doc.reload();

        assertThat(doc.get("secretProperty")).isEqualTo("rebuilt_value");
        assertThat(doc.get("publicProperty")).isEqualTo("public_value");
      });
    } finally {
      database.getSchema().getType("SecurityTest").getEvents().unregisterListener(rewriter);
    }
  }

  /**
   * {@code reload()} on the VERTEX shape, which the encryption recipe this hook exists for is written on. It takes a
   * different route than the document one - {@code ImmutableVertex.reload()} nulls the buffer before delegating - so
   * it is worth its own case.
   */
  @Test
  void reloadKeepsWhatTheAfterReadListenerReturnedOnAVertex() {
    database.transaction(() -> database.getSchema().createVertexType("SecurityReloadVertex"));

    final RID[] vertexRid = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("SecurityReloadVertex");
      v.set("publicProperty", "public_value");
      v.set("secretProperty", "encrypted_value");
      v.save();
      vertexRid[0] = v.getIdentity();
    });

    final AfterRecordReadListener rewriter = record -> record.asVertex().modify().set("secretProperty", "decrypted_value");

    database.getSchema().getType("SecurityReloadVertex").getEvents().registerListener(rewriter);
    try {
      database.transaction(() -> {
        final ImmutableVertex v = (ImmutableVertex) database.lookupByRID(vertexRid[0], false);
        assertThat(v.getString("secretProperty")).isEqualTo("decrypted_value");

        v.reload();

        assertThat(v.getString("secretProperty")).isEqualTo("decrypted_value");
        assertThat(v.getString("publicProperty")).isEqualTo("public_value");
        assertThat(v.toJSON().getString("secretProperty")).isEqualTo("decrypted_value");
      });
    } finally {
      database.getSchema().getType("SecurityReloadVertex").getEvents().unregisterListener(rewriter);
    }
  }

  /**
   * The other half of the #5723 change, stated deliberately: a record whose RID no longer resolves now REPORTS that,
   * where before the missing lazy load was indistinguishable from a record with no properties. This is the same
   * contract {@link #consistentExceptionTypesInLazyLoading()} pins down for the other accessors - an accessor of this
   * class does not answer for a record it could not read.
   */
  @Test
  void propertiesAsMapReportsARecordThatNoLongerResolves() {
    final RID[] documentRid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("SecurityTest");
      doc.set("publicProperty", "public_value");
      doc.save();
      documentRid[0] = doc.getIdentity();
    });

    database.transaction(() -> {
      // Not loaded yet: the buffer is only materialised on the first access, which is the whole point here.
      final ImmutableDocument stale = (ImmutableDocument) database.lookupByRID(documentRid[0], false);
      database.deleteRecord(database.lookupByRID(documentRid[0], true));

      assertThatThrownBy(stale::propertiesAsMap).isInstanceOf(RecordNotFoundException.class);
      assertThatThrownBy(() -> stale.propertiesAsMap("publicProperty")).isInstanceOf(RecordNotFoundException.class);
    });
  }
}
