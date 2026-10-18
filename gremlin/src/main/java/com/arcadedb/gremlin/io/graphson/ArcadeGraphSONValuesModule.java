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

import com.arcadedb.database.RID;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * The GraphSON 3 types of the property values ArcadeDB stores and neither TinkerPop's core module nor its extended
 * {@code gx} module knows (issue #10202): {@code byte[]}, the primitive arrays and the Cypher duration. Without them the
 * typed GraphSON writer refused the whole export at the first such value, and a {@code float[]} came back as a list.
 * <p>
 * A link is written as the plain text of its RID: a GraphSON reader recreates the vertices with new identities and has no way
 * to re-point a property at them, so a typed link would come back pointing at whatever record the target holds at the source
 * RID.
 * <p>
 * Used by the GraphSON export and import only ({@link #mapper}), not registered with the Gremlin Server: a client that does not
 * know the {@code arcadedb} namespace could not read a result carrying one of these types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeGraphSONValuesModule extends TinkerPopJacksonModule {
  private static final Map<Class, String> TYPES = new LinkedHashMap<>();

  static {
    TYPES.put(byte[].class, "Binary");
    TYPES.put(short[].class, "ShortArray");
    TYPES.put(int[].class, "IntegerArray");
    TYPES.put(long[].class, "LongArray");
    TYPES.put(float[].class, "FloatArray");
    TYPES.put(double[].class, "DoubleArray");
    TYPES.put(CypherDuration.class, "Duration");
  }

  public ArcadeGraphSONValuesModule() {
    super("arcadedb-graphson-values");

    addSerializer(byte[].class, new TextSerializer<>(byte[].class, v -> Base64.getEncoder().encodeToString(v)));
    addDeserializer(byte[].class, new TextDeserializer<>(byte[].class, t -> Base64.getDecoder().decode(t)));
    addSerializer(CypherDuration.class, new TextSerializer<>(CypherDuration.class, CypherDuration::toString));
    addDeserializer(CypherDuration.class, new TextDeserializer<>(CypherDuration.class, CypherDuration::parse));

    addSerializer(short[].class, new NumbersSerializer<>(short[].class, a -> {
      final List<Number> list = new ArrayList<>(a.length);
      for (final short v : a)
        list.add(v);
      return list;
    }));
    addDeserializer(short[].class, new NumbersDeserializer<>(short[].class, l -> {
      final short[] a = new short[l.size()];
      for (int i = 0; i < a.length; i++)
        a[i] = (short) l.get(i).longValue();
      return a;
    }));
    addSerializer(int[].class, new NumbersSerializer<>(int[].class, a -> {
      final List<Number> list = new ArrayList<>(a.length);
      for (final int v : a)
        list.add(v);
      return list;
    }));
    addDeserializer(int[].class, new NumbersDeserializer<>(int[].class, l -> {
      final int[] a = new int[l.size()];
      for (int i = 0; i < a.length; i++)
        a[i] = (int) l.get(i).longValue();
      return a;
    }));
    addSerializer(long[].class, new NumbersSerializer<>(long[].class, a -> {
      final List<Number> list = new ArrayList<>(a.length);
      for (final long v : a)
        list.add(v);
      return list;
    }));
    addDeserializer(long[].class, new NumbersDeserializer<>(long[].class, l -> {
      final long[] a = new long[l.size()];
      for (int i = 0; i < a.length; i++)
        a[i] = l.get(i).longValue();
      return a;
    }));
    addSerializer(float[].class, new NumbersSerializer<>(float[].class, a -> {
      final List<Number> list = new ArrayList<>(a.length);
      for (final float v : a)
        list.add(v);
      return list;
    }));
    addDeserializer(float[].class, new NumbersDeserializer<>(float[].class, l -> {
      final float[] a = new float[l.size()];
      for (int i = 0; i < a.length; i++)
        a[i] = l.get(i).floatValue();
      return a;
    }));
    addSerializer(double[].class, new NumbersSerializer<>(double[].class, a -> {
      final List<Number> list = new ArrayList<>(a.length);
      for (final double v : a)
        list.add(v);
      return list;
    }));
    addDeserializer(double[].class, new NumbersDeserializer<>(double[].class, l -> {
      final double[] a = new double[l.size()];
      for (int i = 0; i < a.length; i++)
        a[i] = l.get(i).doubleValue();
      return a;
    }));

    addSerializer(RID.class, new LinkAsTextSerializer());
  }

  @Override
  public Map<Class, String> getTypeDefinitions() {
    return TYPES;
  }

  @Override
  public String getTypeNamespace() {
    return "arcadedb";
  }

  /** The mapper the GraphSON export and import share: TinkerPop's {@code gx} types and this module on top of the graph's own. */
  public static GraphSONMapper mapper(final ArcadeGraph graph) {
    return graph.io(IoCore.graphson()).mapper().addCustomModule(GraphSONXModuleV3.build())
        .addCustomModule(new ArcadeGraphSONValuesModule()).create();
  }

  /**
   * A single typed GraphSON 3 value ({@code {"@type": ..., "@value": ...}}) read through the {@code gx} types and this module, or
   * {@code fallback} when no deserializer reads it.
   */
  public static Object readTypedValue(final String typeName, final String typedJson, final Object fallback) {
    final Object decoded;
    try {
      decoded = TypedValueMapperHolder.MAPPER.readValue(typedJson, Object.class);
    } catch (final IOException | RuntimeException e) {
      // THE TYPE AND THE FAILURE, NOT THE VALUE NOR A MESSAGE THAT MAY QUOTE IT: IMPORTED DATA DOES NOT BELONG IN THE LOG
      LogManager.instance().log(ArcadeGraphSONValuesModule.class, Level.WARNING,
          "Cannot decode a GraphSON value of type '%s' (%s): it is imported as it is in the file", null, typeName,
          e.getClass().getSimpleName());
      return fallback;
    }
    return storable(decoded, fallback);
  }

  /**
   * {@code value} as ArcadeDB can store it: a ByteBuffer as its bytes, a Character as a string, a value of a type ArcadeDB
   * has no storage for (a MonthDay, a Year, a Period, ...) as {@code fallback}, its text in the file, and the elements of a
   * list or a map the same way, an unsupported one as its own text. Saving such a value would
   * otherwise drop the property without a word.
   */
  static Object storable(final Object value, final Object fallback) {
    if (value == null || value instanceof UUID)
      return value;
    // THE ELEMENTS TOO: SAVED AS THEY ARE, ONE ARCADEDB CANNOT STORE WOULD BECOME A NULL
    if (value instanceof Collection<?> collection) {
      final List<Object> result = new ArrayList<>(collection.size());
      for (final Object item : collection)
        result.add(storable(item, String.valueOf(item)));
      return result;
    }
    if (value instanceof Map<?, ?> map) {
      final Map<Object, Object> result = new LinkedHashMap<>();
      for (final Map.Entry<?, ?> entry : map.entrySet())
        result.put(storable(entry.getKey(), String.valueOf(entry.getKey())), storable(entry.getValue(), String.valueOf(entry.getValue())));
      return result;
    }
    if (value instanceof ByteBuffer buffer) {
      final byte[] bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
      return bytes;
    }
    if (value instanceof Character c)
      return c.toString();
    return Type.getTypeByValue(value) != null ? value : fallback;
  }

  private static final class TypedValueMapperHolder {
    private static final ObjectMapper MAPPER = GraphSONMapper.build().version(GraphSONVersion.V3_0)
        .addCustomModule(GraphSONXModuleV3.build()).addCustomModule(new ArcadeGraphSONValuesModule()).create().createMapper();
  }

  private static final class TextSerializer<T> extends StdScalarSerializer<T> {
    private final Function<T, String> toText;

    private TextSerializer(final Class<T> type, final Function<T, String> toText) {
      super(type);
      this.toText = toText;
    }

    @Override
    public void serialize(final T value, final JsonGenerator generator, final SerializerProvider provider) throws IOException {
      generator.writeString(toText.apply(value));
    }
  }

  private static final class TextDeserializer<T> extends StdDeserializer<T> {
    private final Function<String, T> fromText;

    private TextDeserializer(final Class<T> type, final Function<String, T> fromText) {
      super(type);
      this.fromText = fromText;
    }

    @Override
    public T deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
      return fromText.apply(parser.getValueAsString());
    }

    @Override
    public boolean isCachable() {
      return true;
    }
  }

  /** A primitive array as a JSON array of plain numbers; the non-finite floating point values as their Java text. */
  private static final class NumbersSerializer<T> extends StdScalarSerializer<T> {
    private final Function<T, List<Number>> toNumbers;

    private NumbersSerializer(final Class<T> type, final Function<T, List<Number>> toNumbers) {
      super(type);
      this.toNumbers = toNumbers;
    }

    @Override
    public void serialize(final T value, final JsonGenerator generator, final SerializerProvider provider) throws IOException {
      generator.writeStartArray();
      for (final Number n : toNumbers.apply(value)) {
        if ((n instanceof Double || n instanceof Float) && !Double.isFinite(n.doubleValue()))
          generator.writeString(n.toString());
        else if (n instanceof Double || n instanceof Float)
          generator.writeNumber(n.doubleValue());
        else
          generator.writeNumber(n.longValue());
      }
      generator.writeEndArray();
    }
  }

  private static final class NumbersDeserializer<T> extends StdDeserializer<T> {
    private final Function<List<Number>, T> fromNumbers;

    private NumbersDeserializer(final Class<T> type, final Function<List<Number>, T> fromNumbers) {
      super(type);
      this.fromNumbers = fromNumbers;
    }

    @Override
    public T deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
      final List<Number> numbers = new ArrayList<>();
      JsonToken token = parser.currentToken();
      if (token == JsonToken.START_ARRAY)
        token = parser.nextToken();
      while (token != null && token != JsonToken.END_ARRAY) {
        if (token == JsonToken.VALUE_STRING)
          numbers.add(Double.valueOf(parser.getText()));
        else if (token == JsonToken.VALUE_NUMBER_INT)
          numbers.add(parser.getLongValue());
        else
          numbers.add(parser.getDoubleValue());
        token = parser.nextToken();
      }
      return fromNumbers.apply(numbers);
    }

    @Override
    public boolean isCachable() {
      return true;
    }
  }

  /** A link as plain, untyped text: see the class comment for why it is not a typed value. */
  private static final class LinkAsTextSerializer extends StdSerializer<RID> {
    private LinkAsTextSerializer() {
      super(RID.class);
    }

    @Override
    public void serialize(final RID value, final JsonGenerator generator, final SerializerProvider provider) throws IOException {
      generator.writeString(value.toString());
    }

    @Override
    public void serializeWithType(final RID value, final JsonGenerator generator, final SerializerProvider provider,
        final TypeSerializer typeSerializer) throws IOException {
      serialize(value, generator, provider);
    }
  }
}
