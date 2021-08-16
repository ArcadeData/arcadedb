/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure.io.graphson;

import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.arcadedb.structure.io.ArcadeIoRegistry;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public final class RIDJacksonSerializer extends JsonSerializer<RID> {

  @Override
  public void serialize(final RID value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException {
    jgen.writeStartObject();
    jgen.writeFieldName(ArcadeIoRegistry.BUCKET_ID);
    jgen.writeNumber(value.getBucketId());
    jgen.writeFieldName(ArcadeIoRegistry.BUCKET_POSITION);
    jgen.writeNumber(value.getPosition());
    jgen.writeEndObject();
  }

  @Override
  public void serializeWithType(final RID value, final JsonGenerator jgen, final SerializerProvider serializers, final TypeSerializer typeSer)
      throws IOException {
    typeSer.writeTypePrefixForScalar(value, jgen);
    jgen.writeString(value.toString());
    typeSer.writeTypeSuffixForScalar(value, jgen);
  }

}
