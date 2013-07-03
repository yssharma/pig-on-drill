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

    // would only need this to compare schemas of different row groups
    //List<Footer> footers;
    //Iterator<Footer> footerIter;
    ParquetMetadata footer;
    BytesInput currBytes;

    private OutputMutator outputMutator;
    private BufferAllocator allocator;
    private int currentRowGroup;
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

        parquetReader = reader;
    }

    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
        valueVectorMap.clear();
        descriptorMap.clear();
        outputMutator = output;
        schema = footer.getFileMetaData().getSchema();
        generator = new SchemaIdGenerator();
        currentSchema = new ObjectSchema();
        currentRowGroup = -1;

        try {
            List<ColumnDescriptor> columns = schema.getColumns();
            for (int i = 0; i < columns.size(); ++i) {
                ColumnDescriptor column = columns.get(i);
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
            ValueVector.ValueVectorBase v = TypeHelper.getNewVector(f, allocator);
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
        try {
            PageReadStore currentPage = parquetReader.readNextRowGroup();
            if (currentPage == null) {
                return 0;
            }

            currentRowGroup++;

            for(ColumnChunkMetaData column : footer.getBlocks().get(currentRowGroup).getColumns()) {
                Field field = checkNotNull(
                        currentSchema.getField(toFieldName(column.getPath()), 0), "Field not found: %s", column.getPath()
                );

                ColumnDescriptor descriptor = descriptorMap.get(field.getFieldId());

                PageReader pageReader = currentPage.getPageReader(descriptor);
                p = pageReader.readPage();
                VectorHolder holder = valueVectorMap.get(field.getFieldId());
                while (p != null) {
                    currBytes = p.getBytes();
                    holder.getValueVector().data.writeBytes(currBytes.toByteArray());
                    p = pageReader.readPage();
                }
            }

            //TODO: Fix the row count
            return (int) currentPage.getRowCount();
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
