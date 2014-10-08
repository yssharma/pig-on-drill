/**
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
 */
package org.apache.drill.exec.pigparser;


import com.google.common.io.Resources;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.pigparser.planconverter.PigPlanTranslator;
import org.apache.drill.exec.pigparser.util.PigParserUtil;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.util.VectorUtil;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestPigLatinOperators extends BaseTestQuery{

    @Test
    public void testPigLoadStore() throws Exception {
        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-load-store.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);

        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 5);
    }


    @Test
    public void testPigLoadCustomDelimiter() throws Exception {
        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-load-store-custom-delim.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);

        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 30);
    }

    @Test
    public void testPigLimit() throws Exception {
        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-limit.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);

        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 2);
    }

    @Test
    public void testPigDistinct() throws Exception {
        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-distinct.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);
        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 8);
    }

    @Test
    public void testPigInnerJoin() throws Exception {
        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-inner-join.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);
        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 50); // 50: 5 matches of Nations for each Region
    }

    @Test
    public void testPigLeftOuterJoin() throws Exception {
        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-left-join.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);
        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 53); // 53: 5 matches of Nations for each Region + 3 unmatched Regions
    }

    @Test
    public void testPigRightOuterJoin() throws Exception {
        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-right-join.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);
        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 55); // 55: 5 matches of Nations for each Region + 5 unmatched Nations
    }

    @Test
    public void testPigFilterExpression() throws Exception {
        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-filter.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);
        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 2);
    }

    @Test
    public void testDagPruning() throws Exception {

        // Pig Parser leaver unterminated branches,
        // Whereas Drill expects a Sink for every branch.
        // Nodes/Branches need to be pruned for a valid Drill LP.

        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-dag-pruning.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);
        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 5);
    }

    @Test
    public void testCompositePigPlan() throws Exception {

        // Pig Parser leaver unterminated branches,
        // Whereas Drill expects a Sink for every branch.
        // Nodes/Branches need to be pruned for a valid Drill LP.

        String PIG_SCRIPT_PATH = "pigparsing/pigscripts/pig-composite-plan.txt";

        File pigScript = new File(Resources.getResource(PIG_SCRIPT_PATH).getFile());
        LogicalPlan generatedLogicalPlan = new PigPlanTranslator().toDrillLogicalPlan(pigScript, PigParserUtil.PigExecType.LOCALFILE);
        List<QueryResultBatch> results =  testLogicalWithResults(generatedLogicalPlan.toJsonString(DrillConfig.create()));
        int count = 0;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for (QueryResultBatch result : results){
            count += result.getHeader().getRowCount();
            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() > 0) {
                VectorUtil.showVectorAccessibleContent(loader);
            }
            loader.clear();
            result.release();
        }
        assertTrue(count == 6);
    }
}
