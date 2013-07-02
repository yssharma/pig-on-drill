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

import com.beust.jcommander.internal.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.drill.common.exceptions.ExecutionSetupException;

import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.PrintFooter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static parquet.column.Encoding.BIT_PACKED;
import static parquet.column.Encoding.PLAIN;

public class ParquetRecordReaderTest {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageEngineRegistry.class);

    private String getResource(String resourceName) {
        return "resource:" + resourceName;
    }

    class MockOutputMutator implements OutputMutator {
        List<Integer> removedFields = Lists.newArrayList();
        List<ValueVector.ValueVectorBase> addFields = Lists.newArrayList();

        @Override
        public void removeField(int fieldId) throws SchemaChangeException {
            removedFields.add(fieldId);
        }

        @Override
        public void addField(int fieldId, ValueVector.ValueVectorBase vector) throws SchemaChangeException {
            addFields.add(vector);
        }

        @Override
        public void setNewSchema() throws SchemaChangeException {
        }

        List<Integer> getRemovedFields() {
            return removedFields;
        }

        List<ValueVector.ValueVectorBase> getAddFields() {
            return addFields;
        }
    }

    @Test
    public void testBasicWriteRead() throws Exception {

        File testFile = new File("exec/java-exec/src/test/resources/testParquetFile").getAbsoluteFile();
        System.out.println(testFile.toPath().toString());
        testFile.delete();

        Path path = new Path(testFile.toURI());
        Configuration configuration = new Configuration();

        MessageType schema = MessageTypeParser.parseMessageType("message m { required int32 b; }");
        String[] path1 = {"b"};
        ColumnDescriptor c1 = schema.getColumnDescription(path1);
//        String[] path2 = {"c", "d"};
//        ColumnDescriptor c2 = schema.getColumnDescription(path2);

        byte[] bytes1 = { 1, 1, 1, 1, 2, 2, 2, 2 };
        byte[] bytes2 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
//        byte[] bytes3 = { 2, 3, 4, 5};
//        byte[] bytes4 = { 3, 4, 5, 6};
        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
        w.start();
        w.startBlock(1);
        w.startColumn(c1, 5, codec);
        long c1Starts = w.getPos();
        w.writeDataPage(2, 4, BytesInput.from(bytes1), BIT_PACKED, BIT_PACKED, PLAIN);
        w.writeDataPage(3, 4, BytesInput.from(bytes2), BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        long c1Ends = w.getPos();
//        w.startColumn(c2, 6, codec);
//        long c2Starts = w.getPos();
//        w.writeDataPage(2, 4, BytesInput.from(bytes2), BIT_PACKED, PLAIN, PLAIN);
//        w.writeDataPage(3, 4, BytesInput.from(bytes2), BIT_PACKED, BIT_PACKED, PLAIN);
//        w.writeDataPage(1, 4, BytesInput.from(bytes2), BIT_PACKED, BIT_PACKED, PLAIN);
//        w.endColumn();
//        long c2Ends = w.getPos();
//        w.endBlock();
//        w.startBlock(4);
//        w.startColumn(c1, 7, codec);
//        w.writeDataPage(7, 4, BytesInput.from(bytes3), BIT_PACKED, BIT_PACKED, PLAIN);
//        w.endColumn();
//        w.startColumn(c2, 8, codec);
//        w.writeDataPage(8, 4, BytesInput.from(bytes4), BIT_PACKED, BIT_PACKED, PLAIN);
//        w.endColumn();
        w.endBlock();
        w.end(new HashMap<String, String>());

        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
        //assertEquals("footer: "+readFooter, 2, readFooter.getBlocks().size());
        assertEquals(c1Ends - c1Starts, readFooter.getBlocks().get(0).getColumns().get(0).getTotalSize());
        //assertEquals(c2Ends - c2Starts, readFooter.getBlocks().get(0).getColumns().get(1).getTotalSize());


        { // read first block of col #1
            ParquetFileReader r = new ParquetFileReader(configuration, path, Arrays.asList(readFooter.getBlocks().get(0)), Arrays.asList(schema.getColumnDescription(path1)));
            PageReadStore pages = r.readNextRowGroup();
            assertEquals(1, pages.getRowCount());
            validateContains(schema, pages, path1, 2, BytesInput.from(bytes1));
            validateContains(schema, pages, path1, 3, BytesInput.from(bytes2));
            assertNull(r.readNextRowGroup());
        }

//        { // read all blocks of col #1 and #2
//
//            ParquetFileReader r = new ParquetFileReader(configuration, path, readFooter.getBlocks(), Arrays.asList(schema.getColumnDescription(path1), schema.getColumnDescription(path2)));
//
//            PageReadStore pages = r.readNextRowGroup();
//            assertEquals(3, pages.getRowCount());
//            validateContains(schema, pages, path1, 2, BytesInput.from(bytes1));
//            validateContains(schema, pages, path1, 3, BytesInput.from(bytes1));
//            validateContains(schema, pages, path2, 2, BytesInput.from(bytes2));
//            validateContains(schema, pages, path2, 3, BytesInput.from(bytes2));
//            validateContains(schema, pages, path2, 1, BytesInput.from(bytes2));
//
//            pages = r.readNextRowGroup();
//            assertEquals(4, pages.getRowCount());
//
//            validateContains(schema, pages, path1, 7, BytesInput.from(bytes3));
//            validateContains(schema, pages, path2, 8, BytesInput.from(bytes4));
//
//            assertNull(r.readNextRowGroup());
//        }
        PrintFooter.main(new String[]{path.toString()});
    }

    @Test
    public void parquetTest(@Injectable final FragmentContext context) throws IOException, ExecutionSetupException {
        new Expectations() {
            {
                context.getAllocator();
                returns(new DirectBufferAllocator());
            }
        };

        MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 b; }");
        String[] path1 = {"b"};
        ColumnDescriptor c1 = schema.getColumnDescription(path1);

        File testFile = FileUtils.getResourceAsFile("/testParquetFile").getAbsoluteFile();
        System.err.println(testFile.toPath().toString());
        //testFile.delete();

        Path path = new Path(testFile.toURI());
        Configuration configuration = new Configuration();
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);

        ParquetFileReader parReader = new ParquetFileReader(configuration, path, Arrays.asList(
                readFooter.getBlocks().get(0)), Arrays.asList(schema.getColumnDescription(path1)));
        ParquetRecordReader pr = new ParquetRecordReader(context, testFile.getAbsolutePath(), parReader, readFooter);

        MockOutputMutator mutator = new MockOutputMutator();
        List<ValueVector.ValueVectorBase> addFields = mutator.getAddFields();
        pr.setup(mutator);
        assertEquals(2, pr.next());
        assertEquals(3, addFields.size());

        assertEquals(0, pr.next());
    }

    private void validateFooters(final List<Footer> metadata) {
        logger.debug(metadata.toString());
        assertEquals(3, metadata.size());
        for (Footer footer : metadata) {
            final File file = new File(footer.getFile().toUri());
            assertTrue(file.getName(), file.getName().startsWith("part"));
            assertTrue(file.getPath(), file.exists());
            final ParquetMetadata parquetMetadata = footer.getParquetMetadata();
            assertEquals(2, parquetMetadata.getBlocks().size());
            final Map<String, String> keyValueMetaData = parquetMetadata.getFileMetaData().getKeyValueMetaData();
            assertEquals("bar", keyValueMetaData.get("foo"));
            assertEquals(footer.getFile().getName(), keyValueMetaData.get(footer.getFile().getName()));
        }
    }

    private void validateContains(MessageType schema, PageReadStore pages, String[] path, int values, BytesInput bytes) throws IOException {
        PageReader pageReader = pages.getPageReader(schema.getColumnDescription(path));
        Page page = pageReader.readPage();
        assertEquals(values, page.getValueCount());
        assertArrayEquals(bytes.toByteArray(), page.getBytes().toByteArray());
    }

}
