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
package org.apache.drill.exec.store;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.drill.exec.schema.*;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ParquetRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);
  private static final int DEFAULT_LENGTH = 256 * 1024; // 256kb
  private static final String SEPERATOR = System.getProperty("file.separator");


  private final IntObjectOpenHashMap<VectorHolder> valueVectorMap;
  private final IntObjectOpenHashMap<ColumnDescriptor> descriptorMap;

  private ParquetFileReader parquetReader;

  private SchemaIdGenerator generator;
  private RecordSchema currentSchema;
  // this class represents a row group, it is named poorly in the parquet library
  // to signal that a RowGroup has finished being read, this will be assigned null
  // when next() is called the next row group can then be read from the parquet file for processing
  private PageReadStore currentRowGroup;

  private int byteWidthAllFixedFields;
  private boolean allFieldsFixedLength;
  private boolean previousPageFinished;
  private int recordsPerBatch;
  // records the number of records that have been read out of all of the current pages
  // for fixed width fields that can be used to find the read position in next()
  private int recordsReadFromPage;

  // class to keep track of the read position of variable length columns
  private class PageReadStatus{
    int readPos;
  }

  // stores the read statuses of the variable length columns
  private IntObjectOpenHashMap<PageReadStatus> readStatuses;

  // would only need this to compare schemas of different row groups
  //List<Footer> footers;
  //Iterator<Footer> footerIter;
  ParquetMetadata footer;
  BytesInput currBytes;

  private OutputMutator outputMutator;
  private BufferAllocator allocator;
  private int currentRowGroupIndex;
  private int batchSize;
  private MessageType schema;


  public ParquetRecordReader(FragmentContext fragmentContext,
                             ParquetFileReader reader, ParquetMetadata footer) {
    this(fragmentContext, DEFAULT_LENGTH, reader, footer);
  }


  public ParquetRecordReader(FragmentContext fragmentContext, int batchSize,
                             ParquetFileReader reader, ParquetMetadata footer) {
    this.allocator = fragmentContext.getAllocator();
    this.batchSize = batchSize;
    this.footer = footer;
    valueVectorMap = new IntObjectOpenHashMap<>();
    descriptorMap = new IntObjectOpenHashMap<>();
    readStatuses = new IntObjectOpenHashMap<>();

    parquetReader = reader;
  }

  /**
   * @param type a fixed length type from the parquet library enum
   * @return the length in bytes of the type
   */
  public int getTypeLengthInBytes(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT64:   return 8;
      case INT32:   return 4;
      case BOOLEAN: return 1;
      case FLOAT:   return 4;
      case DOUBLE:  return 8;
      case INT96:   return 16;
      default: // binary, fixed length byte array
        return -1;
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    valueVectorMap.clear();
    descriptorMap.clear();
    outputMutator = output;
    schema = footer.getFileMetaData().getSchema();
    generator = new SchemaIdGenerator();
    currentSchema = new ObjectSchema();
    currentRowGroupIndex = -1;
    currentRowGroup = null;

    try {
      List<ColumnDescriptor> columns = schema.getColumns();
      allFieldsFixedLength = true;
      for (int i = 0; i < columns.size(); ++i) {
        ColumnDescriptor column = columns.get(i);


        // sum the lengths of all of the fixed length fields
        if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
          // There is not support for the fixed binary type yet in parquet, leaving a task here as a reminder
          // TODO - implement this when the feature is added upstream
//          if (column.getType() != PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY){
//              byteWidthAllFixedFields += column.getType().getWidth()
//          }
          byteWidthAllFixedFields += getTypeLengthInBytes(column.getType());
        }
        else{
          allFieldsFixedLength = false;
        }
        Field field = new NamedField(
            0,
            generator,
            "",
            toFieldName(column.getPath()),
            toMajorType(column.getType(), getDataMode(column))
        );
        currentSchema.addField(field);
        getOrCreateVectorHolder(field, 0);
        descriptorMap.put(field.getFieldId(), column);
      }
      if (allFieldsFixedLength){
        recordsPerBatch = DEFAULT_LENGTH / byteWidthAllFixedFields;
      }
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private static String toFieldName(String[] paths) {
    return join(SEPERATOR, paths);
  }

  private SchemaDefProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (schema.getColumnDescription(column.getPath()).getMaxDefinitionLevel() == 0) {
      return SchemaDefProtos.DataMode.REQUIRED;
    } else {
      return SchemaDefProtos.DataMode.OPTIONAL;
    }
  }

  private void resetBatch() {
    for (ObjectCursor<VectorHolder> holder : valueVectorMap.values()) {
      holder.value.reset();
    }
  }

  private VectorHolder getOrCreateVectorHolder(Field field, int parentFieldId) throws SchemaChangeException {
    if (!valueVectorMap.containsKey(field.getFieldId())) {
      SchemaDefProtos.MajorType type = field.getFieldType();
      int fieldId = field.getFieldId();
      MaterializedField f = MaterializedField.create(new SchemaPath(field.getFieldName()), fieldId, parentFieldId, type);
      ValueVector.Base v = TypeHelper.getNewVector(f, allocator);
      v.allocateNew(batchSize);
      VectorHolder holder = new VectorHolder(batchSize, v);
      valueVectorMap.put(fieldId, holder);
      outputMutator.addField(fieldId, v);
      return holder;
    }
    return valueVectorMap.lget();
  }

  @Override
  public int next() {
    resetBatch();
    Page p = null;
    int newRecordCount = 0;

    int recordsToRead = 0;
    try {

      if (allFieldsFixedLength){
        recordsToRead = recordsPerBatch;
      }
      else{
        //loop through variable length data to find the maximum records that will fit in this batch
        // this will be a bit annoying if we want to loop though row groups, then pages and then individual variable
        // length values...
        // jacques believes that variable length fields will be encoded as |length|value|length|value|...
        // cannot find more information on this right now, will keep looking
      }

      while (newRecordCount < recordsToRead && p != null) {
        if (currentRowGroup == null){
          currentRowGroup = parquetReader.readNextRowGroup();
          currentRowGroupIndex++;
        }

        if (currentRowGroup == null) {
          return 0;
        }

        for (ColumnChunkMetaData column : footer.getBlocks().get(currentRowGroupIndex).getColumns()) {

          Field field = checkNotNull(
              currentSchema.getField(toFieldName(column.getPath()), 0), "Field not found: %s", column.getPath()
          );

          ColumnDescriptor descriptor = descriptorMap.get(field.getFieldId());

          PageReader pageReader = currentRowGroup.getPageReader(descriptor);
          p = pageReader.readPage();
          VectorHolder holder = valueVectorMap.get(field.getFieldId());


          // add check here for fixed length column, by checking the value vector, or field schema,
          // not sure what will be easier but right now all I can think to do is a chain of instanceof checks...
          if (true){
            boolean finishedLastPage = previousPageFinished;
            int readStart = 0, readEnd = 0, typeLength = 0;
            while (newRecordCount < recordsToRead && p != null) {
              readStart = 0;
              readEnd = Integer.MAX_VALUE;
              typeLength = 0;
              currBytes = p.getBytes();
              typeLength = getTypeLengthInBytes(descriptor.getType());
              if (! finishedLastPage){
                readStart = typeLength * recordsReadFromPage;
                finishedLastPage = true;
              }
              // read to the end of the page, or the end of the last value that will fit in the batch
              readEnd = Math.min(p.getValueCount() * typeLength, (recordsToRead - newRecordCount) * typeLength) ;

              holder.getValueVector().data.writeBytes(currBytes.toByteArray(), readStart, readEnd);
              newRecordCount += (readEnd - readStart) / typeLength;
              p = pageReader.readPage();
            }
            // the last page of this row group was read
            if (p == null){
              previousPageFinished = true;
            }
            // the end of the page was not reached with the last read, set up for the next read
            else if (readEnd < p.getValueCount() * typeLength){
              previousPageFinished = false;
              recordsReadFromPage = (readEnd - readStart) / typeLength;
            }
            else{
              previousPageFinished = true;
            }
          }
          else{ // TODO - variable length columns

          }
        }
      }

      return newRecordCount;
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

  static SchemaDefProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, SchemaDefProtos.DataMode mode) {
    return toMajorType(primitiveTypeName, 0, mode);
  }

  static SchemaDefProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length, SchemaDefProtos.DataMode mode) {
    switch (primitiveTypeName) {
      case BINARY:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.VARBINARY4).setMode(mode).build();
      case INT64:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.BIGINT).setMode(mode).build();
      case INT32:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.INT).setMode(mode).build();
      case BOOLEAN:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.BOOLEAN).setMode(mode).build();
      case FLOAT:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.FLOAT4).setMode(mode).build();
      case DOUBLE:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.FLOAT8).setMode(mode).build();
      // Both of these are not supported by the parquet library yet (7/3/13), but they are declared here for when they are implemented
      case INT96:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.FIXEDBINARY).setWidth(12).setMode(mode).build();
      case FIXED_LEN_BYTE_ARRAY:
        checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.FIXEDBINARY).setWidth(length).setMode(mode).build();
      default:
        throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
    }
  }

  static String join(String delimiter, String... str) {
    StringBuilder builder = new StringBuilder();
    int i = 0;
    for (String s : str) {
      builder.append(s);
      if (i < str.length) {
        builder.append(delimiter);
      }
      i++;
    }
    return builder.toString();
  }

  @Override
  public void cleanup() {
  }
}
