/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.common.expression;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.expression.UnquotedFieldReference.De2;
import org.apache.drill.common.expression.UnquotedFieldReference.Se2;

import java.io.IOException;

@JsonSerialize(using = Se2.class)
@JsonDeserialize(using = De2.class)
public class UnquotedFieldReference extends SchemaPath{
  MajorType overrideType;

  public UnquotedFieldReference(SchemaPath sp) {
    super(sp);
    checkData();
  }

  private void checkData() {
    if (getRootSegment().getChild() != null) {
      throw new UnsupportedOperationException("UnquotedField references must be singular names.");
    }

  }


  private void checkSimpleString(CharSequence value) {
    if (value.toString().contains(".")) {
      throw new UnsupportedOperationException("UnquotedField references must be singular names.");
    }
  }

  public UnquotedFieldReference(CharSequence value) {
    this(value, ExpressionPosition.UNKNOWN);
    checkSimpleString(value);
  }

  public static UnquotedFieldReference getWithQuotedRef(CharSequence safeString) {
    return new UnquotedFieldReference(safeString, ExpressionPosition.UNKNOWN, false);
  }


  public UnquotedFieldReference(CharSequence value, ExpressionPosition pos) {
    this(value, pos, true);
  }

  public UnquotedFieldReference(CharSequence value, ExpressionPosition pos, boolean check) {
    super(new NameSegment(value), pos);
    if (check) {
      checkData();
      checkSimpleString(value);
    }

  }

  public UnquotedFieldReference(String value, ExpressionPosition pos, MajorType dataType) {
    this(value, pos);
    this.overrideType = dataType;
  }

  @Override
  public MajorType getMajorType() {
    if (overrideType == null) {
      return super.getMajorType();
    } else {
      return overrideType;
    }
  }

  public static class De2 extends StdDeserializer<UnquotedFieldReference> {

    public De2() {
      super(UnquotedFieldReference.class);
    }

    @Override
    public UnquotedFieldReference deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      String ref = this._parseString(jp, ctxt);
      //ref = ref.replace("`", "");
      return new UnquotedFieldReference(ref, ExpressionPosition.UNKNOWN);
    }

  }

  public static class Se2 extends StdSerializer<UnquotedFieldReference> {

    public Se2() {
      super(UnquotedFieldReference.class);
    }

    @Override
    public void serialize(UnquotedFieldReference value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      jgen.writeString(value.getRootSegment().getNameSegment().getPath());
    }

  }

}
