// TODO: add read-only class

public class ValueVectorTypes {
<#list types as type>
<#list type.minor as minor>
  public class ${minor.type} extends Closable {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.type}.class);

    protected final int widthInBits = ${type.width};
    protected int longWords = 0;
    protected final BufferAllocator allocator;
    protected ByteBuf data = DeadBuf.DEAD_BUFFER;
    protected int maxValueCount = 0;
    protected final MaterializedField field;
    private int recordCount;

    public ${minor.type}(MaterializedField field, BufferAllocator allocator) {
      this.allocator = allocator;
      this.field = field;
    }

    public final void allocateNew(int valueCount){
      int allocationSize = getAllocationSize(valueCount);
      ByteBuf newBuf = allocator.buffer(allocationSize);
      resetAllocation(valueCount, newBuf);
    }

    public final void set(int index, ${type.javaType} value){
      index *= widthInBits;
      data.set${type.javaType?cap_first}(index, value);
    }
    
    public final ${type.javaType} get(int index){
      index *= widthInBits;
      return data.getInt(index);
    }

    public int getWidthInBits() {
        return widthInBits;
    }

    public void setRecordCount(int recordCount) {
      this.data.writerIndex(recordCount*(widthInBits/8));
      super.setRecordCount(recordCount);
    }

    /**
     * Update the current buffer allocation utilize the provided allocation.
     * @param maxValueCount
     * @param buf
     */
    protected final void resetAllocation(int maxValueCount, ByteBuf buf){
      clear();
      buf.retain();
      this.maxValueCount = maxValueCount;
      this.data = buf;
      childResetAllocation(maxValueCount, buf);
    }

    protected int getAllocationSize(int valueCount) {
      return (int) Math.ceil(valueCount*widthInBits*1.0/8);
    }
    
    protected void childResetAllocation(int valueCount, ByteBuf buf) {
      this.longWords = valueCount/8;
    }

    protected void childCloneMetadata(T other) {
      other.longWords = this.longWords;
    }

    protected void childClear() {
      longWords = 0;
    }

    protected final void clear(){
      if(this.data != DeadBuf.DEAD_BUFFER){
        this.data.release();
        this.data = DeadBuf.DEAD_BUFFER;
        this.maxValueCount = 0;
      }
      childClear();
    }

    public final void cloneMetadata(T other){
      other.maxValueCount = this.maxValueCount;
    }

    /**
     * Copies the data from this vector into its pair.
     * 
     * @param vector
     */
    public final void cloneInto(T vector) {
      vector.allocateNew(maxValueCount);
      data.writeBytes(vector.data);
      cloneMetadata(vector);
      childResetAllocation(maxValueCount, vector.data);
    }

    // ValueVector Interface:

    /**
     * Allocate a new memory space for this vector.
     * 
     * @param valueCount
     *          The number of possible values which should be contained in this vector.
     */
    public abstract void allocateNew(int valueCount);

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
    public final void transferTo(T vector) {
      vector.data = this.data;
      cloneMetadata(vector);
      childResetAllocation(maxValueCount, data);
      clear();
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


</#list>
</#list>
}