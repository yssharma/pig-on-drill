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

package org.apache.drill.exec.pigparser.planconverter;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.pigparser.parser.PigLatinParser;
import org.apache.drill.exec.exception.PigParsingException;
import org.apache.drill.exec.pigparser.util.PigParserUtil;


import java.io.File;

public class PigPlanTranslator {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PigPlanTranslator.class);

    public LogicalPlan toDrillLogicalPlan(String scriptText, PigParserUtil.PigExecType execType) throws PigParsingException {
        try {
            org.apache.pig.newplan.logical.relational.LogicalPlan pigPlan = new PigLatinParser().getLogicalPlan(scriptText, PigLatinParser.ParamType.QUERY_STRING);
            return new PigPlanVisitor().toDrillLP(pigPlan, execType);
        } catch (PigParsingException ex) {
            throw new PigParsingException("Error in parsing Pig Script. "+ex.getCause());
        }
    }

    public LogicalPlan toDrillLogicalPlan(File file, PigParserUtil.PigExecType execType) throws PigParsingException {
        try {
            org.apache.pig.newplan.logical.relational.LogicalPlan pigPlan = new PigLatinParser().getLogicalPlan(file.getAbsolutePath(), PigLatinParser.ParamType.FILE_PATH_STRING);
            return new PigPlanVisitor().toDrillLP(pigPlan, execType);
        } catch (PigParsingException ex) {
            throw new PigParsingException("Error in parsing Pig Script from file. "+ex.getCause());
        }
    }


}