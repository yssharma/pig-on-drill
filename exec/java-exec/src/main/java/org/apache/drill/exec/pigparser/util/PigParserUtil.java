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

package org.apache.drill.exec.pigparser.util;

import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.pig.newplan.logical.relational.LOLoad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PigParserUtil {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PigParserUtil.class);
    private static Map<String, String> delimMap = new HashMap<>();

    public enum PigExecType{
        LOCALFILE, HDFS
    }

    static{
        delimMap = new HashMap<>();
        delimMap.put(",", "csv");
        delimMap.put("\t", "tsv");
        delimMap.put("|", "psv");
    }



    public static PlanProperties getPlanProperties(Class caller) {
        PlanProperties planProperties = PlanProperties.builder()
                .generator(caller.getName(), "drill-pig-parser")
                .version(1)
                .type(PlanProperties.PlanType.APACHE_DRILL_LOGICAL)
                .build();
        return planProperties;
    }

    // TODO: Only handles text format from filesystem for now
    public static FileSystemConfig getStorageConfig(Map<String, FormatPluginConfig> pluginformats, PigExecType exectype) {
        FileSystemConfig config = new FileSystemConfig();

        switch(exectype){
            case LOCALFILE:config.connection = "file:///"; break;
            case HDFS: config.connection = "hdfs:///"; break; //TODO: ADD HDFS workspaces
            default: config.connection = null;
        }

        config.formats = pluginformats;
        return config;
    }


    // TODO: Only handles text format from filesystem for now
    public static FormatPluginConfig createFormatPluginConfig(String delim) {
        TextFormatPlugin.TextFormatConfig textconfig = new TextFormatPlugin.TextFormatConfig();
        List<String> extensions = new ArrayList<>();
        textconfig.delimiter = delim;
        extensions.add(delimMap.get(delim));
        textconfig.extensions = extensions;

        return textconfig;
    }


    public static String getDelimiterName(String delim){
        if(! delimMap.containsKey(delim)) {
            return "delim" + delim;
        }
        else{
            return delimMap.get(delim);
        }
    }

    public static String getDelimiterName(LOLoad op){
        String[] args = op.getFileSpec().getFuncSpec().getCtorArgs();
        if(args == null){
            return delimMap.get("\t"); // Default delim for PigStorage
        }
        else{
            return getDelimiterName(args[0]);
        }
    }

    public static String getDelimiter(LOLoad op){
        String[] args = op.getFileSpec().getFuncSpec().getCtorArgs();
        return ((args == null) ? "\t" : args[0]);
    }
}
