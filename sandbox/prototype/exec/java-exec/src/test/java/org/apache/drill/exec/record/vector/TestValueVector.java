package org.apache.drill.exec.record.vector;

import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.io.UTF8;
import org.junit.Test;

import java.nio.charset.Charset;

public class TestValueVector {

  DirectBufferAllocator allocator = new DirectBufferAllocator();

  @Test
  public void testFixedType() {
    // Build a required uint field definition
    SchemaDefProtos.MajorType.Builder typeBuilder = SchemaDefProtos.MajorType.newBuilder();
    SchemaDefProtos.FieldDef.Builder defBuilder = SchemaDefProtos.FieldDef.newBuilder();
    typeBuilder
        .setMinorType(SchemaDefProtos.MinorType.UINT4)
        .setMode(SchemaDefProtos.DataMode.REQUIRED)
        .setWidth(4);
    defBuilder
        .setFieldId(1)
        .setParentId(0)
        .setMajorType(typeBuilder.build());
        MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    ValueVector.UInt4 v = new ValueVector.UInt4(field, allocator);
    v.allocateNew(1024);

    // Put and set a few values
    v.set(0, 100);
    v.set(1, 101);
    v.set(100, 102);
    v.set(1022, 103);
    v.set(1023, 104);
    assertEquals(100, v.get(0));
    assertEquals(101, v.get(1));
    assertEquals(102, v.get(100));
    assertEquals(103, v.get(1022));
    assertEquals(104, v.get(1023));
    
    // Ensure unallocated space returns 0
    assertEquals(0, v.get(3));
  }

  @Test
  public void testNullableVarLen2() {
    // Build an optional varchar field definition
    SchemaDefProtos.MajorType.Builder typeBuilder = SchemaDefProtos.MajorType.newBuilder();
    SchemaDefProtos.FieldDef.Builder defBuilder = SchemaDefProtos.FieldDef.newBuilder();
    typeBuilder
        .setMinorType(SchemaDefProtos.MinorType.VARCHAR2)
        .setMode(SchemaDefProtos.DataMode.OPTIONAL)
        .setWidth(2);
    defBuilder
        .setFieldId(1)
        .setParentId(0)
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    ValueVector.NullableVarChar2 v = new ValueVector.NullableVarChar2(field, allocator);
    v.allocateNew(1024);

    // Create and set 3 sample strings
    String str1 = new String("AAAAA1");
    String str2 = new String("BBBBBBBBB2");
    String str3 = new String("CCCC3");
    v.set(0, str1.getBytes(Charset.defaultCharset()));
    v.set(1, str2.getBytes(Charset.defaultCharset()));
    v.set(2, str3.getBytes(Charset.defaultCharset()));

    // Check the sample strings
    ByteBuf chk1 = allocator.buffer(6);
    ByteBuf chk2 = allocator.buffer(10);
    ByteBuf chk3 = allocator.buffer(5);
    v.get(0).getBytes(0, chk1);
    v.get(1).getBytes(0, chk2);
    v.get(2).getBytes(0, chk3);

    assertEquals(str1, chk1.toString(0, 6, Charset.defaultCharset()));
    assertEquals(str2, chk2.toString(0, 10, Charset.defaultCharset()));
    assertEquals(str3, chk3.toString(0, 5, Charset.defaultCharset()));

    // Ensure null value throws
    try {
      v.get(3);
      assertFalse(false);
    } catch(NullValueException e) { }

  }


  @Test
  public void testNullableFixedType() {
    // Build an optional uint field definition
    SchemaDefProtos.MajorType.Builder typeBuilder = SchemaDefProtos.MajorType.newBuilder();
    SchemaDefProtos.FieldDef.Builder defBuilder = SchemaDefProtos.FieldDef.newBuilder();
    typeBuilder
        .setMinorType(SchemaDefProtos.MinorType.UINT4)
        .setMode(SchemaDefProtos.DataMode.OPTIONAL)
        .setWidth(4);
    defBuilder
        .setFieldId(1)
        .setParentId(0)
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    ValueVector.NullableUInt4 v = new ValueVector.NullableUInt4(field, allocator);
    v.allocateNew(1024);

    // Put and set a few values
    v.set(0, 100);
    v.set(1, 101);
    v.set(100, 102);
    v.set(1022, 103);
    v.set(1023, 104);
    assertEquals(100, v.get(0));
    assertEquals(101, v.get(1));
    assertEquals(102, v.get(100));
    assertEquals(103, v.get(1022));
    assertEquals(104, v.get(1023));

    // Ensure null values throw
    try {
      v.get(3);
      assertFalse(false);
    } catch(NullValueException e) { }

  }

}
