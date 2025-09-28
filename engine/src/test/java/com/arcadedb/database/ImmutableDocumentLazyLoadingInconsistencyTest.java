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
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
      type.createProperty("publicProperty", com.arcadedb.schema.Type.STRING);
      type.createProperty("secretProperty", com.arcadedb.schema.Type.STRING);
    });
  }

  @Test
  public void testLazyLoadingConsistencyWithSecurityException() {
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
  public void testLazyLoadingConsistencyWithoutSecurityException() {
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

      } finally {
        // Clean up the security listener
        database.getEvents().unregisterListener(securityListener);
      }
    });
  }

  @Test
  public void testConsistentExceptionTypesInLazyLoading() {
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
}
