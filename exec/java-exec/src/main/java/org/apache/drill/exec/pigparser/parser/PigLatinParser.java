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
package org.apache.drill.exec.pigparser.parser;

import org.apache.drill.exec.exception.PigParsingException;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigscript.parser.ParseException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.nio.charset.Charset;
import java.util.Properties;


public class PigLatinParser {

    /**
     * ExtendedPigServer : Reuse the Pig LogicalPlan from PigServer.
     */
    private class ExtendedPigServer extends PigServer {

        ExtendedPigServer(PigContext context) throws ExecException {
            super(context);
        }

        private LogicalPlan getLogicalPlan(Reader reader) throws PigParsingException {
            try {
                GruntParser parser = new GruntParser(reader, this);
                parser.setInteractive(false);
                setBatchOn();
                parser.parseOnly();
                return getCurrentDAG().getLogicalPlan();
            }
            catch(IOException | ParseException ex){
                throw new PigParsingException(ex);
            }
        }
    }


    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PigLatinParser.class);

    public static enum ParamType {
        QUERY_STRING, FILE_PATH_STRING
    }

    PigContext context = new PigContext(ExecType.LOCAL, new Properties());

    /**
     * Overloaded method for PigLatin input.
     * @param query Can be PigLatin query as String or a FilePath as String.
     *              Type is determined by the next parameter ParamType(type)
     * @param type QUERY_STRING - The whole PigLatin script as String
     *             FILE_PATH_STRING - File path of PigLatin script as string
     * @return
     * @throws PigParsingException
     */
    public LogicalPlan getLogicalPlan(String query, ParamType type) throws PigParsingException {
        BufferedReader reader = null;
        ExtendedPigServer server = null;
        try {
            switch (type) {
                case QUERY_STRING:
                    query = query + "\n"; // Keep GruntParser happy in case user misses extra newline at end.
                    InputStream is = new ByteArrayInputStream(query.getBytes());
                    reader = new BufferedReader(new InputStreamReader(is));
                    server = new ExtendedPigServer(context);
                    return server.getLogicalPlan(reader);

                case FILE_PATH_STRING:
                    Charset charset = Charset.forName("UTF-8");
                    InputStream filestream = new FileInputStream(query);
                    InputStream delimiterstream = new ByteArrayInputStream("\n".getBytes(charset));
                    // Keep GruntParser happy in case user misses extra newline at end.
                    SequenceInputStream stream = new SequenceInputStream(filestream, delimiterstream);
                    reader = new BufferedReader(new InputStreamReader(stream, charset));

                    server = new ExtendedPigServer(context);
                    return server.getLogicalPlan(reader);
                default:
                    throw new PigParsingException("Unsupported input mode for Pig Parsing. Unable to create Pig Logical Plan.");
            }
        }catch(ExecException | FileNotFoundException ex){
            throw new PigParsingException("Error in Pig Latin Script. Unable to create Pig Logical Plan.");
        }
    }


}