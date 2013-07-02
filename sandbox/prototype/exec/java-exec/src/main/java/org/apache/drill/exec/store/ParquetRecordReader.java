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
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import io.netty.buffer.ByteBuf;
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
import org.apache.drill.exec.schema.json.jackson.JacksonHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class ParquetRecordReader implements RecordReader {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);
    private static final int DEFAULT_LENGTH = 256 * 1024; // 256kb


    // from JSONRecordReader
    private final String inputPath;

    private final IntObjectOpenHashMap<VectorHolder> valueVectorMap;

    private ParquetFileReader parquetReader;
    private PageReadStore currentPage;


    private SchemaIdGenerator generator;
    // would only need this to compare schemas of different row groups
    private DiffSchema diffSchema;
    private RecordSchema currentSchema;
    //List<Footer> footers;
    //Iterator<Footer> footerIter;
    ParquetMetadata footer;
    BytesInput currBytes;

    private List<Field> removedFields;
    private OutputMutator outputMutator;
    private BufferAllocator allocator;
    private int batchSize;


    public ParquetRecordReader(FragmentContext fragmentContext, String inputPath,
                               ParquetFileReader reader, ParquetMetadata footer) {
        this(fragmentContext, inputPath, DEFAULT_LENGTH, reader, footer);
    }


    public ParquetRecordReader(FragmentContext fragmentContext, String inputPath, int batchSize,
                               ParquetFileReader reader, ParquetMetadata footer) {
        this.inputPath = inputPath;
        this.allocator = fragmentContext.getAllocator();
        this.batchSize = batchSize;
        this.footer = footer;
        valueVectorMap = new IntObjectOpenHashMap<>();

        parquetReader = reader;
    }

    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
        outputMutator = output;
        currentSchema = new ObjectSchema();
        diffSchema = new DiffSchema();
        removedFields = Lists.newArrayList();

        InputSupplier<InputStreamReader> input;
        if (inputPath.startsWith("resource:")) {
            input = Resources.newReaderSupplier(Resources.getResource(inputPath.substring("resource:".length())), Charsets.UTF_8);
        } else {
            input = Files.newReaderSupplier(new File(inputPath), Charsets.UTF_8);
        }



        generator = new SchemaIdGenerator();
    }

    private void resetBatch() {
        for (ObjectCursor<VectorHolder> holder : valueVectorMap.values()) {
            holder.value.reset();
        }

        currentSchema.resetMarkedFields();
        diffSchema.reset();
        removedFields.clear();
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
            currentPage = parquetReader.readNextRowGroup();
            MessageType schema = footer.getFileMetaData().getSchema();
            ColumnChunkMetaData column = footer.getBlocks().get(0).getColumns().get(0);
            ValueVector.ValueVectorBase vector;
            switch (column.getType()) {
                case BINARY:
                    break;
                case INT64:
                    break;
                case INT32:
                    break;
                case BOOLEAN:
                    break;
                case FLOAT:
                    break;
                case DOUBLE:
                    break;
                case INT96:
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    break;
            }
            MaterializedField f = MaterializedField.create(new SchemaPath(join(System.getProperty(
                    "file.separator"), column.getPath())), 2, 1, JacksonHelper.INT_TYPE);
            //ValueVector.NullableInt vec = (ValueVector.NullableInt) TypeHelper.getNewVector(f, allocator);
            ValueVector.NullableUInt8 vec = new ValueVector.NullableUInt8(f, allocator);
            vec.allocateNew(30);

            p = currentPage.getPageReader(schema.getColumnDescription(column.getPath())).readPage();

            currBytes = p.getBytes();
            vec.data.writeBytes(currBytes.toByteArray());

            String s = "";
            for (int i = 0; i < 8; i++){
                vec.setNotNull(i);
                s += " " + vec.get(i);
            }

            p = currentPage.getPageReader(schema.getColumnDescription(column.getPath())).readPage();

            currBytes = p.getBytes();
            vec.data.writeBytes(currBytes.toByteArray());

            s += ", ";
            for (int i = 0; i < 8; i++){
                vec.setNotNull(i);
                s += " " + vec.get(i);
            }

//            p = currentPage.getPageReader(schema.getColumnDescription(column.getPath())).readPage();
//
//            currBytes = p.getBytes();
//            vec.data.writeBytes(currBytes.toByteArray());
//
//            s += ", ";
//            for (int i = 0; i < 8; i++){
//                vec.setNotNull(i);
//                s += " " + vec.get(i);
//            }

            throw new RuntimeException(s);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return p.getValueCount();
    }

    private void recordNewField(Field field) {
        diffSchema.recordNewField(field);
    }

    static String join(String delimiter, String... str) {
        StringBuilder builder = new StringBuilder();
        int i = 0;
        for (String s : str){
            builder.append(s);
            if ( i < str.length){
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
