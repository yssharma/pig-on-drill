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

// TODO: add read-only class
//       make bit vector a stand-alone class
//       add map
//       add repeated map
//       add nullable
//       add repeatable
//       catch netty ByteBuf exceptions

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
    // BB TODO: can derived classes inline these?
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
      if(this.data != DeadBuf.DEAD_BUFFER){
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
     * Return the underlying buffers associated with this vector. Note that this doesn't impact the reference counts for this buffer so it only should be
     * used for in context access. Also note that this buffer changes regularly thus external classes shouldn't hold a
     * reference to it (unless they change it).
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
     * @param recordCount Number of records active in this vector.  Used for purposes such as getting a writable range of the data.
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
      return FieldMetadata.newBuilder().setDef(getField().getDef()).setValueCount(getRecordCount()).setBufferLength(len).build();
    }

  }

<#list types as type>
<#list type.minor as minor>
<#if type.major == "Fixed">

  /**
   * ${minor.class} implements a vector of fixed width values.  Elements in the vector
   * are accessed by offset from the logical start of the (0-based) ByteBuf.
   *   The width of each element is ${type.width} byte(s)
   *   The equivilent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class ${minor.class} extends ValueVectorBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}.class);

    protected final int widthInBits = ${type.width};
    protected int longWords = 0;

    public ${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
    }

    /**
     * Set the element at the given index to the given value.  Note that widths smaller than
     * 32-bits are handled by the ByteBuf interface.
     */
    public void set(int index, <#if (type.width > 4)>${type.javaType?cap_first}<#else>int</#if> value) {
      index *= widthInBits;
<#if (type.width > 8)>
      data.setBytes(index, value);
<#else>
      data.set${type.javaType?cap_first}(index, value);
</#if>
    }
    
    public ${type.javaType} get(int index) {
      index *= widthInBits;
<#if (type.width > 8)>
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index, dst, 0, ${type.width});
      return dst;
<#else>
      return data.get${type.javaType?cap_first}(index);
</#if>
    }

    public final int getWidthInBits() {
        return widthInBits;
    }

    public void setRecordCount(int recordCount) {
      //TODO/FIXME: truncation issue?
      this.data.writerIndex(recordCount*(widthInBits/8));
      super.setRecordCount(recordCount);
    }

    protected int getAllocationSize(int valueCount) {
      return (int) Math.ceil(valueCount*widthInBits*1.0/8);
    }
    
    protected void childResetAllocation(int valueCount, ByteBuf buf) {
      this.longWords = valueCount/8;
    }

    protected void childCloneMetadata(${minor.class} other) {
      other.longWords = this.longWords;
    }

    protected void childClear() {
      longWords = 0;
    }

    public Object getObject(int index) {
<#if (type.width > 8)>
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index, dst, 0, ${type.width});
      return dst;
<#else>
      return data.get${type.javaType?cap_first}(index);
</#if>
    }
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

    public ByteBuf get(int index) {
      int offset = lengthVector.get(index);
      int length = lengthVector.get(index+1) - offset;
      ByteBuf dst = allocator.buffer(length);
      data.getBytes(index, dst, 0, length);
      return dst;
    }

    public void set(int index, byte[] bytes) {
      checkArgument(index >= 0);
      // assert index < Math.exp(2, {$type.width} * 8);
      if (index == 0) {
        lengthVector.set(0, bytes.length);
        data.setBytes(0, bytes);
      } else {
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

    protected void childCloneMetadata(${minor.class} other) {
      lengthVector.cloneMetadata(other.lengthVector);
      other.expectedValueLength = expectedValueLength;
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

</#if>

  /**
   * Nullable${minor.class} implements a vector of values which could be null.  Elements in the vector
   * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
   * from the base class (if not null).
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public class Nullable${minor.class} extends ${minor.class} {

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
    public void set(int index, <#if (type.width > 4)>${type.javaType?cap_first}<#elseif type.major == "VarLen">byte[]<#else>int</#if> value) {
      setNotNull(index);
      super.set(index, value);
    }
    
<#if type.major == "Fixed">
    public ${type.javaType} get(int index) {
<#else>
    public ByteBuf get(int index) {
</#if>
      return isNull(index) ? null : super.get(index);
    }

    public void setNull(int index) {
        bits.set(index, 1);
    }

    public void setNotNull(int index) {
        bits.set(index, 0);
    }

    public boolean isNull(int index) {
      return bits.get(index) == 0;
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
      super.resetAllocation(valueCount, buf.slice(firstSize, super.getAllocationSize(valueCount)));
      bits.resetAllocation(valueCount, buf.slice(0, firstSize));
      // bits.setAllFalse();
    }

    protected void childCloneMetadata(Nullable${minor.class} other) {
      bits.cloneMetadata(other.bits);
      super.cloneInto(other);
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

</#list>
</#list>
}