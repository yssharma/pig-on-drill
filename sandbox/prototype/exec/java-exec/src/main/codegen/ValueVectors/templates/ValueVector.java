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

// TODO:
//    - Create ReadableValueVector to complement mutable version
//    - Implement repeated map

/**
 * ValueVectorTypes defines a set of template-generated classes which implement type-specific
 * value vectors.  The template approach was chosen due to the lack of multiple inheritence.  It
 * is also important that all related logic be as efficient as possible.
 */
public class ValueVector {

  /**
   * ValueVectorBase implements common logic for all value vectors.  Note that only the derived
   * classes should be used to avoid virtual function call lookups which cannot be optimized out.
   */
  public static class ValueVectorBase implements Closeable {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorBase.class);

    protected final BufferAllocator allocator;
    public ByteBuf data = DeadBuf.DEAD_BUFFER;
    protected MaterializedField field;
    protected int recordCount;
    protected int totalBytes;

    public ValueVectorBase(MaterializedField field, BufferAllocator allocator) {
      this.allocator = allocator;
      this.field = field;
    }

    /**
     * Get the explicitly specified size of the allocated buffer, if available.  Otherwise
     * calculate the size based on width and record count.
     */
    public int getAllocatedSize() {
      return (totalBytes > 0) ? totalBytes : getSizeFromCount(recordCount);
    }

    /**
     * Virtaul method to get the size requirement (in bytes) for the given number of values.  Only
     * accurate for fixed width value vectors.
     */
    protected int getSizeFromCount(int valueCount) {
      return 0;
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of values which can be contained within this vector.
     */
    public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
      clear();
      sourceBuffer.retain();
      this.recordCount = valueCount;
      this.totalBytes = totalBytes > 0 ? totalBytes : getSizeFromCount(valueCount);
      this.data = sourceBuffer;
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of values which can be contained within this vector.
     */
    public void allocateNew(int valueCount) {
      totalBytes = getSizeFromCount(valueCount);
      allocateNew(totalBytes, allocator.buffer(totalBytes), valueCount);
    }

    protected void clear() {
      if (data != DeadBuf.DEAD_BUFFER) {
        data.release();
        data = DeadBuf.DEAD_BUFFER;
        recordCount = 0;
        totalBytes = 0;
      }
    }

    /**
     * Get an object representation of the element at the given index
     */
    public Object getObject(int index) {
      return null;
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
      return getRecordCount();
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
    public MaterializedField getField() {
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
      byte currentByte = data.getByte((int)Math.floor(index/8));
      if (value)
        currentByte |= (byte) Math.pow(2, (index % 8));
      else
        currentByte ^= (byte) Math.pow(2, (index % 8));
      data.setByte((int) Math.floor(index/8), currentByte);
    }

    public void set(int index, int value) {
      set(index, value != 0);
    }

    public boolean get(int index) {
      logger.warn("BIT GET: index: {}, byte: {}, mask: {}, masked byte: {}",
                  index,
                  data.getByte((int)Math.floor(index/8)),
                  (int)Math.pow(2, (index % 8)),
                  data.getByte((int)Math.floor(index/8)) & (int)Math.pow(2, (index % 8)));

      return (data.getByte((int)Math.floor(index/8)) & (int)Math.pow(2, (index % 8))) != 0;
    }

    public Object getObject(int index) {
      return new Boolean(get(index));
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.
     */
    protected int getSizeFromCount(int valueCount) {
      return (int) Math.floor(valueCount / 8);
    }

    public int getAllocatedSize() {
      return (int) Math.ceil(recordCount / 8);
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of values which can be contained within this vector.
     */
    public void allocateNew(int valueCount) {
      totalBytes = (int)Math.ceil(valueCount / 8);
      allocateNew(totalBytes, allocator.buffer(totalBytes), valueCount);
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

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of values which can be contained within this vector.
     */
    public void allocateNew(int valueCount) {
      totalBytes = valueCount * ${type.width};
      allocateNew(totalBytes, allocator.buffer(totalBytes), valueCount);
    }

    public int getAllocatedSize() {
      return (int) Math.ceil(totalBytes);
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.  Only accurate
     * for fixed width value vectors.
     */
    protected int getSizeFromCount(int valueCount) {
      return valueCount * ${type.width};
    }

      <#if (type.width > 8)>
    /**
     * Set the element at the given index to the given value.  Note that widths smaller than
     * 32 bits are handled by the ByteBuf interface.
     */
    public void set(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
      data.setBytes(index * ${type.width}, value);
    }

    public ${minor.javaType!type.javaType} get(int index) {
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
    public void set(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
      data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, value);
    }

    public ${minor.javaType!type.javaType} get(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    public Object getObject(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index);
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

    public ${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      this.lengthVector = new UInt${type.width}(null, allocator);
    }

    public ByteBuf get(int index) {
      int offset = lengthVector.get(index);
      int length = lengthVector.get(index+1) - offset;
      ByteBuf dst = allocator.buffer(length);
      data.getBytes(offset, dst, 0, length);
      return dst;
    }

    public void set(int index, byte[] bytes) {
      checkArgument(index >= 0);
      if (index == 0) {
        lengthVector.set(0, 0);
        lengthVector.set(1, bytes.length);
        data.setBytes(0, bytes);
      }
      else {
        int currentOffset = lengthVector.get(index);
        lengthVector.set(index + 1, currentOffset + bytes.length); // set the end offset of the buffer
        data.setBytes(currentOffset, bytes);
      }
    }

    @Override
    public int getAllocatedSize() {
      return lengthVector.getAllocatedSize() + totalBytes;
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.  Only accurate
     * for fixed width value vectors.
     */
    protected int getSizeFromCount(int valueCount) {
      return valueCount * ${type.width};
    }

    @Override
    protected void clear() {
      super.clear();
      lengthVector.clear();
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of values which can be contained within this vector.
     */
    public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
      super.allocateNew(totalBytes, sourceBuffer, valueCount);
      lengthVector.allocateNew(valueCount);
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
      this.totalBytes = totalBytes;
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

    public Nullable${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      bits = new Bit(null, allocator);
    }

    /**
     * Set the element at the given index to the given value.  Note that widths smaller than
     * 32-bits are handled by the ByteBuf interface.
     */
    public void set(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#elseif type.major == "VarLen">byte[]<#else>int</#if> value) {
      setNotNull(index);
      super.set(index, value);
    }

    /**
     * Get the element at the specified position.
     * @return  value of the element, if not null
     * @throws  NullValueException if the value is null
     */
    public <#if type.major == "VarLen">ByteBuf<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      if (isNull(index))
        throw new NullValueException(index);
      return super.get(index);
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

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of values which can be contained within this vector.
     */
    public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
      super.allocateNew(totalBytes, sourceBuffer, valueCount);
      bits.allocateNew(valueCount);
    }

    @Override
    public int getAllocatedSize() {
      return bits.getAllocatedSize() + super.getAllocatedSize();
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.  Only accurate
     * for fixed width value vectors.
     */
    protected int getSizeFromCount(int valueCount) {
      return valueCount * ${type.width} + (valueCount / 8);
    }

    @Override
    public MaterializedField getField() {
      return field;
    }

    @Override
    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{bits.data, super.data};
    }

    @Override
    public void setRecordCount(int recordCount) {
      super.setRecordCount(recordCount);
      bits.setRecordCount(recordCount);
    }

    @Override
    public Object getObject(int index) {
      return isNull(index) ? null : super.getObject(index);
    }
  }

  public static class Repeated${minor.class} extends ${minor.class} {

    private UInt4 countVector;    // number of repeated elements in each record
    private UInt4 offsetVector;   // offsets to start of each record

    public Repeated${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      countVector = new UInt4(null, allocator);
      offsetVector = new UInt4(null, allocator);
    }

    public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
      super.allocateNew(totalBytes, sourceBuffer, valueCount);
      countVector.allocateNew(valueCount);
      offsetVector.allocateNew(valueCount);
    }

    /**
     * Add an element to the given record index.
     */
    public void add(int index, <#if (type.width > 4)> ${minor.javaType!type.javaType}
                               <#elseif type.major == "VarLen"> byte[]
                               <#elseif type.major == "Bit"> boolean
                               <#else> int
                               </#if> value) {
      countVector.set(index, countVector.get(index) + 1);
      offsetVector.set(index, offsetVector.get(index - 1) + countVector.get(index-1));
      super.set(offsetVector.get(index), value);
    }

    public <#if type.major == "VarLen">ByteBuf<#else>${minor.javaType!type.javaType}</#if> get(int index, int positionIndex) {
      assert positionIndex < countVector.get(index);
      return super.get(offsetVector.get(index) + positionIndex);
    }

    public MaterializedField getField() {
      return field;
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.  Only accurate
     * for fixed width value vectors.
     */
    protected int getSizeFromCount(int valueCount) {
      return valueCount * ${type.width} + (valueCount * <#if (type.width > 4)>4<#else>${type.width}</#if>);
    }

    /**
     * Get the explicitly specified size of the allocated buffer, if available.  Otherwise
     * calculate the size based on width and record count.
     */
    public int getAllocatedSize() {
      return super.getAllocatedSize() +
             countVector.getAllocatedSize() +
             offsetVector.getAllocatedSize();
    }

    /**
     * Get the elements at the given index.
     */
    public int getCount(int index) {
      return countVector.get(index);
    }

    public void setRecordCount(int recordCount) {
      super.setRecordCount(recordCount);
      offsetVector.setRecordCount(recordCount);
      countVector.setRecordCount(recordCount);
    }

    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{countVector.data, offsetVector.data, data};
    }

    public Object getObject(int index) {
      return data.slice(index, getSizeFromCount(countVector.get(index)));
    }



  }
  </#list>
</#list>
}

