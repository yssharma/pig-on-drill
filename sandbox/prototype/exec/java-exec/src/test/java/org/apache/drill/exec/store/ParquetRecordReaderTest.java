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
import parquet.column.ColumnDescriptor;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParquetRecordReaderTest {

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
    public void testSameSchemaInSameBatch(@Injectable final FragmentContext context) throws IOException, ExecutionSetupException {
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

}
