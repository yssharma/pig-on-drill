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
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.schema.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.column.ColumnDescriptor;
import parquet.column.page.PageReadStore;
import parquet.hadoop.CodecFactory;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetRecordReader implements RecordReader {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);
    private static final int DEFAULT_LENGTH = 256 * 1024; // 256kb


    // from JASONRecordReader
    private final String inputPath;

    private final IntObjectOpenHashMap<VectorHolder> valueVectorMap;

    private ParquetFileReader parquetReader;
    private PageReadStore currentPage;
    List<Footer> footers;


    private SchemaIdGenerator generator;
    // would only need this to compare schemas of different row groups
    private DiffSchema diffSchema;
    private RecordSchema currentSchema;

    private List<Field> removedFields;
    private OutputMutator outputMutator;
    private BufferAllocator allocator;
    private int batchSize;

    public ParquetRecordReader(FragmentContext fragmentContext, String inputPath, int batchSize,
            List<BlockMetaData> blocks, List<ColumnDescriptor> columns, Configuration configuration) throws IOException {
        this.inputPath = inputPath;
        this.allocator = fragmentContext.getAllocator();
        this.batchSize = batchSize;
        valueVectorMap = new IntObjectOpenHashMap<>();

        Path filePath = new Path(inputPath);
        FileSystem fileSystem = filePath.getFileSystem(configuration);
        footers = ParquetFileReader.readFooters(configuration, fileSystem.getFileStatus(filePath));

        parquetReader = new ParquetFileReader(configuration, filePath, blocks, columns);
        for (Footer footer : footers){


        }
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

    @Override
    public int next() {
        try {
            currentPage = parquetReader.readNextRowGroup();

            currentPage.getPageReader().readPage().getBytes();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public void cleanup() {

    }
}
