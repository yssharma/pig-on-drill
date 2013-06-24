/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.record.vector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;

/**
 * ValueVectorTypes defines a set of template-generated classes which implement type-specific
 * value vectors.  The template approach was chosen due to the lack of multiple inheritence.  It
 * is also important that all related logic be as efficient as possible.
 */
public class ValueVectorTypes {

  /**
   * ValueVectorBase implements common logic for all value vectors.  Note that only the derived
   * classes should be used to avoid virtual function call lookups which cannot be optimized out.
   */
  public static class ValueVectorBase implements Closeable {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorBase.class);

    protected final BufferAllocator allocator;
    protected ByteBuf data = DeadBuf.DEAD_BUFFER;
    protected int maxValueCount = 0;
    protected final MaterializedField field;
    private int recordCount;

    public ValueVectorBase(MaterializedField field, BufferAllocator allocator) {
      this.allocator = allocator;
      this.field = field;
    }

    protected int getAllocationSize(int maxValueCount) { return maxValueCount; }
    protected void childResetAllocation(int valueCount, ByteBuf buf) { }
    protected void childClear() { }

    /**
     * Update the current buffer allocation utilize the provided allocation.
     * @param maxValueCount
     * @param buf
     */
    protected final void resetAllocation(int maxValueCount, ByteBuf buf) {
      clear();
      buf.retain();
      this.maxValueCount = maxValueCount;
      this.data = buf;
      childResetAllocation(maxValueCount, buf);
    }

    protected final void clear() {
      if (this.data != DeadBuf.DEAD_BUFFER) {
        this.data.release();
        this.data = DeadBuf.DEAD_BUFFER;
        this.maxValueCount = 0;
      }
      childClear();
    }

    /**
     * Update the value vector to the provided record information.
     * @param metadata
     * @param data
     */
    public void setTo(FieldMetadata metadata, ByteBuf data) {
      clear();
      resetAllocation(metadata.getValueCount(), data);
    }

    /**
     * Zero copy move of data from this vector to the target vector. Any future access to this vector without being
     * populated by a new vector will cause problems.
     *
     * @param vector
     */
    public void transferTo(ValueVectorBase vector) {
      vector.data = this.data;
      cloneMetadata(vector);
      childResetAllocation(maxValueCount, data);
      clear();
    }

    // TODO: add derived implementations
    public void cloneMetadata(ValueVectorBase other) {
      other.maxValueCount = this.maxValueCount;
    }

    // TODO: add derived implementations
    /**
     * Copies the data from this vector into its pair.
     *
     * @param vector
     */
    public void cloneInto(ValueVectorBase vector) {
      vector.allocateNew(maxValueCount);
      data.writeBytes(vector.data);
      cloneMetadata(vector);
      childResetAllocation(maxValueCount, vector.data);
    }

    /**
     * Allocate a new memory space for this vector.
     *
     * @param valueCount
     *          The number of possible values which should be contained in this vector.
     */
    public void allocateNew(int valueCount) {
      int allocationSize = getAllocationSize(valueCount);
      ByteBuf newBuf = allocator.buffer(allocationSize);
      resetAllocation(valueCount, newBuf);
    }

    /**
     * Return the underlying buffers associated with this vector. Note that this doesn't impact the
     * reference counts for this buffer so it only should be used for in-context access. Also note 
     * that this buffer changes regularly thus external classes shouldn't hold a reference to
     * it (unless they change it).
     *
     * @return The underlying ByteBuf.
     */
    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{data};
    }

    /**
     * Returns the maximum number of values contained within this vector.
     * @return Vector size
     */
    public int capacity() {
      return maxValueCount;
    }

    /**
     * Release supporting resources.
     */
    @Override
    public void close() {
      clear();
    }

    /**
     * Get information about how this field is materialized.
     *
     * @return
     */
    public MaterializedField getField(){
      return field;
    }

    /**
     * Define the number of records that are in this value vector.
     * @param recordCount Number of records active in this vector.
     */
    public void setRecordCount(int recordCount) {
      this.recordCount = recordCount;
    }

    public int getRecordCount() {
      return recordCount;
    }

    /**
     * Get the metadata for this field.
     * @return
     */
    public FieldMetadata getMetadata() {
      int len = 0;
      for(ByteBuf b : getBuffers()){
        len += b.writerIndex();
      }
      return FieldMetadata.newBuilder()
               .setDef(getField().getDef())
               .setValueCount(getRecordCount())
               .setBufferLength(len)
               .build();
    }

  }

  public static class Bit extends ValueVectorBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Bit.class);

    public Bit(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
    }

    /**
     * Set the element at the given index to the given value (1 == true, 0 == false).
     */
    public void set(int index, boolean value) {
      byte currentValue = data.getByte((int)Math.floor(index/8));
      if (value)
        currentValue |= (byte) Math.pow(2, (index % 8));
      else
        currentValue ^= (byte) Math.pow(2, (index % 8));
      data.setByte((int) Math.floor(index/8), currentValue);
    }

    public boolean get(int index) {
      return (data.getByte((int) Math.floor(index/8)) & (int) Math.pow(2, (index % 8))) == 1;
    }

    public Object getObject(int index) {
      return new Boolean(get(index));
    }

    protected int getAllocationSize(int valueCount) {
      return (int) Math.ceil(valueCount / 8);
    }

  }

<#list types as type>
  <#list type.minor as minor>
    <#if type.major == "Fixed">

  public static class ${minor.class} extends ValueVectorBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}.class);

    public ${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
    }

    protected int getAllocationSize(int valueCount) {
      return (int) Math.ceil(valueCount * ${type.width});
    }

      <#if (type.width > 8)>
    /**
     * Set the element at the given index to the given value.  Note that widths smaller than
     * 32 bits are handled by the ByteBuf interface.
     */
    public void set(int index, <#if (type.width > 4)>${type.javaType}<#else>int</#if> value) {
      data.setBytes(index * ${type.width}, value);
    }

    public ${type.javaType} get(int index) {
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index * ${type.width}, dst, 0, ${type.width});
      return dst;
    }

    public Object getObject(int index) {
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index, dst, 0, ${type.width});
      return dst;
    }
      <#else> <#-- type.width <= 8 -->
    /**
     * Set the element at the given index to the given value.  Note that widths smaller than
     * 32-bits are handled by the ByteBuf interface.
     */
    public void set(int index, <#if (type.width > 4)>${type.javaType}<#else>int</#if> value) {
      data.set${type.javaType?cap_first}(index * ${type.width}, value);
    }

    public ${type.javaType} get(int index) {
      return data.get${type.javaType?cap_first}(index * ${type.width});
    }

    public Object getObject(int index) {
      return data.get${type.javaType?cap_first}(index);
    }

      </#if> <#-- type.width -->
  }

    <#elseif type.major == "VarLen">

  /**
   * ${minor.class} implements a vector of variable width values.  Elements in the vector
   * are accessed by position from the logical start of the vector.  A fixed width lengthVector
   * is used to convert an element's position to it's offset from the start of the (0-based) ByteBuf.
   *   The width of each element is ${type.width} byte(s)
   *   The equivilent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class ${minor.class} extends ValueVectorBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}.class);

    protected final UInt${type.width} lengthVector;
    protected int expectedValueLength;

    public ${minor.class}(MaterializedField field, BufferAllocator allocator, int expectedValueLength) {
      super(field, allocator);
      this.lengthVector = getNewLengthVector(allocator);
      this.expectedValueLength = expectedValueLength;
    }

    protected UInt${type.width} getNewLengthVector(BufferAllocator allocator) {
      return new UInt${type.width}(null, allocator);
    }

    protected void childCloneMetadata(${minor.class} other) {
      lengthVector.cloneMetadata(other.lengthVector);
      other.expectedValueLength = expectedValueLength;
    }

    public ByteBuf get(int index) {
      int offset = lengthVector.get(index);
      int length = lengthVector.get(index+1) - offset;
      ByteBuf dst = allocator.buffer(length);
      data.getBytes(index, dst, 0, length);
      return dst;
    }

    public void set(int index, byte[] bytes) {
      checkArgument(index >= 0);
      if (index == 0) {
        lengthVector.set(0, bytes.length);
        data.setBytes(0, bytes);
      }
      else {
        int previousOffset = lengthVector.get(index - 1);
        lengthVector.set(index, previousOffset + bytes.length);
        data.setBytes(previousOffset, bytes);
      }
    }

    @Override
    protected int getAllocationSize(int valueCount) {
      return lengthVector.getAllocationSize(valueCount) + expectedValueLength * valueCount;
    }

    @Override
    protected void childResetAllocation(int valueCount, ByteBuf buf) {
      int firstSize = lengthVector.getAllocationSize(valueCount);
      lengthVector.resetAllocation(valueCount, buf.slice(0, firstSize));
      data = buf.slice(firstSize, expectedValueLength * valueCount);
    }

    @Override
    protected void childClear() {
      lengthVector.clear();
      if (data != DeadBuf.DEAD_BUFFER) {
        data.release();
        data = DeadBuf.DEAD_BUFFER;
      }
    }

    @Override
    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{lengthVector.data, data};
    }

    @Override
    public void setRecordCount(int recordCount) {
      super.setRecordCount(recordCount);
      lengthVector.setRecordCount(recordCount);
    }

    public void setTotalBytes(int totalBytes){
      data.writerIndex(totalBytes);
    }

    public Object getObject(int index) {
      checkArgument(index >= 0);
      int startIdx = 0;
      if (index > 0) {
        startIdx = (int) lengthVector.getObject(index - 1);
      }
      int size = (int) lengthVector.getObject(index) - startIdx;
      checkState(size >= 0);
      byte[] dst = new byte[size];
      data.getBytes(startIdx, dst, 0, size);
      return dst;
    }
  }

    </#if> <#-- type.major -->

  /**
   * Nullable${minor.class} implements a vector of values which could be null.  Elements in the vector
   * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
   * from the base class (if not null).
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class Nullable${minor.class} extends ${minor.class} {

    protected Bit bits;

    <#if type.major == "Fixed">
    public Nullable${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      bits = new Bit(null, allocator);
    }
    <#else>
    public Nullable${minor.class}(MaterializedField field, BufferAllocator allocator, int expectedValueLength) {
      super(field, allocator, expectedValueLength);
      bits = new Bit(null, allocator);
    }
    </#if>

    /**
     * Set the element at the given index to the given value.  Note that widths smaller than
     * 32-bits are handled by the ByteBuf interface.
     */
    public void set(int index, <#if (type.width > 4)>${type.javaType}<#elseif type.major == "VarLen">byte[]<#else>int</#if> value) {
      setNotNull(index);
      super.set(index, value);
    }

    public <#if type.major == "Fixed">${type.javaType}<#else>ByteBuf</#if> get(int index) {
      return isNull(index) ? null : super.get(index);
    }

    protected void childCloneMetadata(Nullable${minor.class} other) {
      bits.cloneMetadata(other.bits);
      super.cloneInto(other);
    }

    public void setNull(int index) {
      bits.set(index, false);
    }

    public void setNotNull(int index) {
      bits.set(index, true);
    }

    public boolean isNull(int index) {
      return !bits.get(index);
    }

    @Override
    protected int getAllocationSize(int valueCount) {
      return bits.getAllocationSize(valueCount) + super.getAllocationSize(valueCount);
    }

    @Override
    public MaterializedField getField() {
      return field;
    }

    @Override
    protected void childResetAllocation(int valueCount, ByteBuf buf) {
      int firstSize = bits.getAllocationSize(valueCount);
      bits.resetAllocation(valueCount, buf.slice(0, firstSize));
          // bits.setAllFalse();
      super.resetAllocation(valueCount, buf.slice(firstSize, getAllocationSize(valueCount)));
    }

    @Override
    protected void childClear() {
      bits.clear();
      super.clear();
    }

    @Override
    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{bits.data, super.data};
    }

    @Override
    public void setRecordCount(int recordCount) {
      super.setRecordCount(recordCount);
      bits.setRecordCount(recordCount);
      super.setRecordCount(recordCount);
    }

    @Override
    public Object getObject(int index) {
      return isNull(index) ? null : super.getObject(index);
    }
  }

  public static class Repeated${minor.class} {
    protected UInt<#if (type.width > 4)>4<#else>${type.width}</#if> countVector; // number of repeated elements in the record
    private ${minor.class} dataVector;
    // protected UInt${type.width} offsetVector; // TODO: do we need this?

    <#if type.major == "Fixed">
    public Repeated${minor.class}(MaterializedField field, BufferAllocator allocator) {
      dataVector = new ${minor.class}(field, allocator);
    <#else>
    public Repeated${minor.class}(MaterializedField field, BufferAllocator allocator, int expectedValueLength) {
      dataVector = new ${minor.class}(field, allocator, expectedValueLength);
    </#if>
      countVector = new UInt<#if (type.width > 4)>4<#else>${type.width}</#if>(null, allocator); // UInt1, UInt2 or Uint4
    }

    /**
     * Set the element at the given index to the given values.  Note that widths smaller than
     * 32-bits are handled by the ByteBuf interface.
     */
    public void set(int index, <#if (type.width > 4)> ${type.javaType}[]
                               <#elseif type.major == "VarLen"> byte[][]
                               <#else> int[]
                               </#if> values) {
      countVector.set(index, values.length);
      for (<#if (type.width > 4)> ${type.javaType}
           <#elseif type.major == "VarLen"> byte[]
           <#else> int
           </#if> i: values) {
        // TODO: memcpy block of values?
        dataVector.set(index, i);
      }
    }

    /**
     * Get the elements at the given index.
     */
    public ByteBuf get(int index) {
      return dataVector.data.slice(index, countVector.get(index));
    }

    protected void childCloneMetadata(Repeated${minor.class} other) {
      countVector.cloneMetadata(other.countVector);
      dataVector.cloneInto(other.dataVector);
    }

    protected int getAllocationSize(int valueCount) {
      return countVector.getAllocationSize(valueCount) + dataVector.getAllocationSize(valueCount);
    }

    public MaterializedField getField() {
      return dataVector.field;
    }

    protected void childResetAllocation(int valueCount, ByteBuf buf) {
      int firstSize = countVector.getAllocationSize(valueCount);
      countVector.resetAllocation(valueCount, buf.slice(0, firstSize));
      dataVector.resetAllocation(valueCount, buf.slice(firstSize, getAllocationSize(valueCount)));
    }

    protected void childClear() {
      countVector.clear();
      dataVector.clear();
    }

    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{countVector.data, dataVector.data};
    }

    public void setRecordCount(int recordCount) {
      dataVector.setRecordCount(recordCount);
      countVector.setRecordCount(recordCount);
    }

    public ByteBuf getObjects(int index) {
      return get(index);
    }      
  }
  </#list>
</#list>
}

