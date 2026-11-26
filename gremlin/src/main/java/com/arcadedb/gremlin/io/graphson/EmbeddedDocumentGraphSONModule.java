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
package com.arcadedb.gremlin.io.graphson;

import com.arcadedb.database.EmbeddedDocument;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * The GraphSON serializer of an ArcadeDB {@link EmbeddedDocument}, the value of an EMBEDDED property (issue #13043). With none,
 * Jackson walks the document as a bean and reaches the engine's own object graph through its {@code database} field, which
 * fails the whole Gremlin Server result ("Could not find a type identifier for the class AtomicLong") and, were it written, would
 * put the engine's internals on the wire.
 * <p>
 * The document is written as the map {@link EmbeddedDocument#toMap(boolean) toMap(true)} answers (its own properties and the
 * {@code @type} / {@code @cat} metadata, the way the HTTP endpoint returns it), through the mapper's own map serializer: a typed
 * GraphSON mapper writes the {@code g:Map} a stock client reads, with no type the client does not know. The values of the map
 * (a nested embedded document, a list or a map of them) are dispatched one by one, so this serializer meets them again.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class EmbeddedDocumentGraphSONModule extends SimpleModule {

  public EmbeddedDocumentGraphSONModule() {
    super("arcadedb-graphson-embedded-document");
    // THE INTERFACE, NOT THE IMPLEMENTATION: IT COVERS THE IMMUTABLE AND THE MUTABLE EMBEDDED DOCUMENT ALIKE
    addSerializer(EmbeddedDocument.class, new EmbeddedDocumentJacksonSerializer());
  }

  private static final class EmbeddedDocumentJacksonSerializer extends StdSerializer<EmbeddedDocument> {
    private EmbeddedDocumentJacksonSerializer() {
      super(EmbeddedDocument.class);
    }

    @Override
    public void serialize(final EmbeddedDocument value, final JsonGenerator generator, final SerializerProvider provider)
        throws IOException {
      // THE MAP IS TYPED (OR NOT) BY THE MAPPER ITSELF, THE WAY IT TYPES EVERY OTHER MAP, WHATEVER THE TYPE OF THE SERIALIZED VALUE
      provider.defaultSerializeValue(value.toMap(true), generator);
    }

    @Override
    public void serializeWithType(final EmbeddedDocument value, final JsonGenerator generator, final SerializerProvider provider,
        final TypeSerializer typeSerializer) throws IOException {
      // THE TYPE ID THE TYPE SERIALIZER WOULD WRITE IS THAT OF THE DOCUMENT CLASS, WHICH NO GRAPHSON CLIENT KNOWS: THE MAP CARRIES ITS OWN
      serialize(value, generator, provider);
    }
  }
}
