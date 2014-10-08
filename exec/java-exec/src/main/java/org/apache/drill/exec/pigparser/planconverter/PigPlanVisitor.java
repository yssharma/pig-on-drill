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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.UnquotedFieldReference;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.LogicalPlanBuilder;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.GroupingAggregate;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.Limit;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.exec.exception.PigParsingException;
import org.apache.drill.exec.pigparser.util.PigParserUtil;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.eigenbase.rel.JoinRelType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


class PigPlanVisitor {

    // Flag for reading input file from specified location. Default should be HDFS for Pig.
    PigParserUtil.PigExecType execType = PigParserUtil.PigExecType.LOCALFILE;

    // Pig Delimiter.
    private final String PIG_SCHEMA_DELIMETER = "::";

    // Keeps track of all inputs used in Pig LOAD commands.
    // Adds the corresponding drill's storage plugins to logical plan.
    Map<String, FormatPluginConfig> pluginformats = null;


    // Incoming Pig Plan to parse.
    org.apache.pig.newplan.logical.relational.LogicalPlan pigPlan = null;


    // LogicalPlanBuilder for construction of the Drill LogicalPlan
    LogicalPlanBuilder builder = null;


    // Keep all the mapping from Pig Operator to Drill Operators.
    // Note: A single Pig operator might require multiple Drill Op's
    Map<Operator, List<LogicalOperator>> pigToDrillOperatorsMapping = new HashMap<>();


    // Keeping track of last projected operators by Drill
    List<Project> projectedOps = null;

    // Sinks of the plan. Useful in pruning un-terminated branches in Pig Plan.
    List<Operator> storeSinks = null;

    LogicalPlan toDrillLP(org.apache.pig.newplan.logical.relational.LogicalPlan pigPlan, PigParserUtil.PigExecType execType) throws PigParsingException {
        this.pigPlan = pigPlan;
        if(null != execType){
            this.execType = execType;
        }

        try {
            Iterator<Operator> ops = pigPlan.getOperators();
            Operator op = null;
            pluginformats = new HashMap<>();
            builder = LogicalPlan.builder();
            builder.planProperties(PigParserUtil.getPlanProperties(this.getClass()));

            List<Operator> sinks = pigPlan.getSinks();
            storeSinks = new ArrayList<>();

            // Get all STORE operations as sink
            for(Operator sink : sinks){
                if(sink.getName().equals("LOStore")){
                    storeSinks.add(sink);
                }
            }

            while (ops.hasNext()) {
                op = ops.next();

                // Check if operator is not connected to sink
                boolean connectedToSink = false;
                for(Operator sink : storeSinks){
                    if(this.pigPlan.pathExists(op, sink)){
                        connectedToSink = true;
                        break;
                    }
                }

                // Skip operator not connected to Sink
                if(!op.getName().equals("LOStore") && !connectedToSink){
                    continue;
                }

                // Update PluginFormats to add new input type.
                if (op instanceof LOLoad) {
                    String delim = PigParserUtil.getDelimiter((LOLoad) op);
                    pluginformats.put(PigParserUtil.getDelimiterName(delim), PigParserUtil.createFormatPluginConfig(delim));
                }

                addOperator(op);
            }

            FileSystemConfig storage = PigParserUtil.getStorageConfig(pluginformats, execType);
            builder.addStorageEngine("cp", storage);
            return builder.build();
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }


    private void addOperator(Operator op) throws IOException, PigParsingException {
        try{
            if(op instanceof LOLoad){ addOperator((LOLoad) op);}
            else if(op instanceof LOStore){ addOperator((LOStore)op);}
            else if(op instanceof LOLimit){ addOperator((LOLimit)op);}
            else if(op instanceof LOForEach){ addOperator((LOForEach)op);}
            else if(op instanceof LOInnerLoad){ addOperator((LOInnerLoad)op);}
            else if(op instanceof LOJoin){ addOperator((LOJoin)op);}
            else if(op instanceof LOUnion){ addOperator((LOUnion)op);}
            else if(op instanceof LODistinct){ addOperator((LODistinct)op);}
            else if(op instanceof LOFilter){ addOperator((LOFilter)op);}
            else {
                throw new PigParsingException("Pig Operator " + op.getName() + " not supported yet.");
            }
        } catch (IllegalArgumentException ex){
            throw new PigParsingException("Pig Operator " + op.getName() + " not supported yet.");
        }
    }

    private void addOperator(LOLoad op) throws IOException, PigParsingException {
        // Remove file:/// prefix if injected by Pig
        String filepath = op.getSchemaFile();
        if(filepath.startsWith("file:///")){
            filepath = filepath.substring(7, filepath.length());
        }

        String jsonSelections = "{ " +
                " \"format\" : { \"type\" : \"named\",  \"name\" : \""+ PigParserUtil.getDelimiterName(op) +"\" }," +
                " \"files\" : [ \"" +
                (execType == PigParserUtil.PigExecType.LOCALFILE?"file:":"") +
                filepath + "\"  ] " +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readValue(jsonSelections, JsonNode.class);
        JSONOptions opts = new JSONOptions(jsonNode, null);
        Scan scan = new Scan("cp", opts);
        updateOperatorMappings(op, scan);
        builder.addLogicalOperator(scan);

        // Pig LOAD operator also holds schema details.
        // Use them for Drill projections.
        List<NamedExpression> namedExps = new ArrayList<>();
        NamedExpression ne = null;
        UnquotedFieldReference key = null;

        for (int i = 0; i < op.getSchema().getFields().size(); i++) {
            LogicalSchema.LogicalFieldSchema feild = op.getSchema().getFields().get(i);
            key = new UnquotedFieldReference("`columns`[" + i + "]");
            namedExps.add(new NamedExpression(key, new FieldReference(feild.alias)));
        }

        Project project = new Project(namedExps.toArray(new NamedExpression[]{}));
        project.setInput(scan);
        updateOperatorMappings(op, project);
        builder.addLogicalOperator(project);
    }


    // TODO: Currently only Dumps on SCREEN
    private void addOperator(LOStore op) throws IOException, PigParsingException {
        Store store = new Store("--SCREEN--", null);
        store.setInput(getLastDrillOperator(op.getPlan().getPredecessors(op).get(0)));
        updateOperatorMappings(op, store);
        builder.addLogicalOperator(store);
    }


    private void addOperator(LOLimit op) throws IOException, PigParsingException {
        Limit limit = new Limit(0, (int) op.getLimit());
        limit.setInput(getLastDrillOperator(op.getInput(pigPlan)));
        updateOperatorMappings(op, limit);
        builder.addLogicalOperator(limit);
    }

    private void addOperator(LOJoin op) throws IOException, PigParsingException{

        // Get Pig Join type converted to corresponding Drill Join Type.
        // Note: Drill Full Join is not currently supported.
        String joinType = null;
        boolean[] flags = op.getInnerFlags();
        if(flags[0] && flags[1]){
            joinType = JoinRelType.INNER.name();
        }
        else {
            if(flags[0]==true && flags[1]==false){
                joinType = JoinRelType.LEFT.name();
            }
            else if(flags[0]==false && flags[1]==true){
                joinType = JoinRelType.RIGHT.name();
            }
            else{
                joinType = JoinRelType.FULL.name();
            }
        }

        // Check if the two join candidates do not have common alias name.
        // If yes the alias has to be projected with unique ref's
        // else Drill gets confused which ones to pick. Do not confuse Drill.
        boolean isAliased = false;
        String leftSchema=null, rightSchema=null;
        List<NamedExpression> namedExps = null;
        Project project = null;

        Operator leftPigOp = op.getPlan().getPredecessors(op).get(0);
        Operator rightPigOp = op.getPlan().getPredecessors(op).get(1);
        Set<String> leftAliasSet = new HashSet<>();
        leftAliasSet.addAll(getLastSchemaCols(leftPigOp));
        Set<String> rightAliasSet = new HashSet<>();
        rightAliasSet.addAll(getLastSchemaCols(rightPigOp));

        if(!Sets.intersection(leftAliasSet, rightAliasSet).isEmpty()){
            if(!(leftPigOp instanceof LogicalRelationalOperator && leftPigOp instanceof  LogicalRelationalOperator) ){
                throw new PigParsingException("Last Pig operator was not an instance of LogicalRelationalOperator");
            }

            leftSchema = ((LogicalRelationalOperator) leftPigOp).getAlias();
            rightSchema = ((LogicalRelationalOperator) rightPigOp).getAlias();

            // Add projections for all left schema
            namedExps = new ArrayList<>();
            for(String s : leftAliasSet){
                namedExps.add(new NamedExpression(new FieldReference(s), new FieldReference(leftSchema + PIG_SCHEMA_DELIMETER + s)));
            }
            project = new Project(namedExps.toArray(new NamedExpression[]{}));
            project.setInput(getLastDrillOperator(leftPigOp));
            updateOperatorMappings(leftPigOp, project);
            builder.addLogicalOperator(project);

            // Add projections for all right schema
            namedExps = new ArrayList<>();
            for(String s : rightAliasSet){
                namedExps.add(new NamedExpression(new FieldReference(s), new FieldReference(rightSchema + PIG_SCHEMA_DELIMETER + s)));
            }
            project = new Project(namedExps.toArray(new NamedExpression[]{}));
            project.setInput(getLastDrillOperator(rightPigOp));
            updateOperatorMappings(rightPigOp, project);
            builder.addLogicalOperator(project);

            isAliased = true;
        }

        LogicalExpressionPlan[] exprs =  op.getExpressionPlanValues().toArray(new LogicalExpressionPlan[]{});
        ProjectExpression exLeft = (ProjectExpression)exprs[0].getOperators().next();
        String leftPigAlias = exLeft.getColAlias();
        if(isAliased){ leftPigAlias = leftSchema+ PIG_SCHEMA_DELIMETER +leftPigAlias; }

        ProjectExpression exRight = (ProjectExpression)exprs[1].getOperators().next();
        String rightPigAlias = exRight.getColAlias();
        if(isAliased){ rightPigAlias = rightSchema+ PIG_SCHEMA_DELIMETER +rightPigAlias; }

        String leftDrillAlias = getDrillAliasForPigOp(leftPigOp, leftPigAlias);
        String rightDrillAlias = getDrillAliasForPigOp(rightPigOp, rightPigAlias);

        if(leftDrillAlias==null || rightDrillAlias==null){
            throw new PigParsingException("Failure in resolution for Pig alias "+leftPigAlias) ;
        }

        FieldReference left = new FieldReference(leftDrillAlias);
        FieldReference right = new FieldReference(rightDrillAlias);

        JoinCondition condition = new JoinCondition("==", left, right);
        JoinCondition[] conditions = new JoinCondition[1];
        conditions[0] = condition;

        // TODO: Currently only supports join by named col names.
        // Does not support index based joins.
        LogicalOperator leftschema = getLastDrillOperator(op.getPlan().getPredecessors(op).get(0));
        LogicalOperator rightschema = getLastDrillOperator(op.getPlan().getPredecessors(op).get(1));

        Join join = new Join(leftschema, rightschema, conditions, joinType);

        updateOperatorMappings(op, join);
        builder.addLogicalOperator(join);

        // Add projections for all input schema
        namedExps = new ArrayList<>();
        for(String s : getLastDrillProjectedSchema(op)){
            namedExps.add(new NamedExpression(new FieldReference(s), new FieldReference(s)));
        }
        project = new Project(namedExps.toArray(new NamedExpression[]{}));
        project.setInput(join);
        updateOperatorMappings(op, project);
        builder.addLogicalOperator(project);
    }

    private void addOperator(LOUnion op) throws IOException, PigParsingException{
        List<Operator> pigInputOperators = op.getInputs();
        LogicalOperator[] drillInputs = new LogicalOperator[pigInputOperators.size()];

        for(int index = 0; index < pigInputOperators.size(); index++){
            drillInputs[index] = getLastDrillOperator(pigInputOperators.get(index));
        }

        Union union = new Union(drillInputs, false);
        updateOperatorMappings(op, union);
        builder.addLogicalOperator(union);

        List<LogicalSchema.LogicalFieldSchema> fields = op.getSchema().getFields();
        List<NamedExpression> namedExps = new ArrayList<>();
        for(LogicalSchema.LogicalFieldSchema f : fields){
            namedExps.add(new NamedExpression(new FieldReference(f.alias), new FieldReference(f.alias)));
        }

        Project project = new Project(namedExps.toArray(new NamedExpression[]{}));
        project.setInput(union);
        updateOperatorMappings(op, project);
        builder.addLogicalOperator(project);
    }

    private void addOperator(LODistinct op) throws IOException, PigParsingException{
        List<NamedExpression> namedExps = new ArrayList<>();
        for(String s : getLastDrillProjectedSchema(op)){
            namedExps.add(new NamedExpression(new FieldReference(s), new FieldReference(s)));
        }
        GroupingAggregate distinct = new GroupingAggregate(namedExps.toArray(new NamedExpression[]{}), new NamedExpression[]{});
        distinct.setInput(getLastDrillOperator(op.getInput(pigPlan)));
        updateOperatorMappings(op, distinct);
        builder.addLogicalOperator(distinct);
    }

    private void addOperator(LOFilter op) throws IOException, PigParsingException{
        LogicalExpressionPlan pigExpressionPlan = op.getFilterPlan();
        LogicalExpression expr = new PigExpressionVisitor().getDrillExpression(pigExpressionPlan);

        Filter filter = new Filter(expr);
        filter.setInput(getLastDrillOperator(op.getInput(pigPlan)));
        updateOperatorMappings(op, filter);
        builder.addLogicalOperator(filter);
    }


    // Returns the Schema aliases for the Pig operator.
    // If no schema details are found it returns the
    // schema details of immediate Predecessors.
    private List<String> getLastSchemaCols(Operator pigOp) {
        List<Operator> ops = pigOp.getPlan().getPredecessors(pigOp);
        List<String> aliases = new ArrayList<>();
        List<LogicalSchema.LogicalFieldSchema> fields = null;
        try {
            fields = ((LogicalRelationalOperator) pigOp).getSchema().getFields();
            for(LogicalSchema.LogicalFieldSchema f : fields){
                aliases.add(f.alias);
            }
            // Return if schema found for current operator
            if(aliases.size() > 0){
                return aliases;
            }
            else{
            // Get schema of Predecessor of this operator.
            for (Operator opp : ops) {
                if (opp instanceof LogicalRelationalOperator) {
                    fields = ((LogicalRelationalOperator) opp).getSchema().getFields();
                    for (LogicalSchema.LogicalFieldSchema f : fields) {
                        aliases.add(f.alias);
                    }


                } else {
                    return new ArrayList<>();
                }
            }
        }
        } catch (FrontendException e) {
            e.printStackTrace();
        }
        return aliases;
    }

    // Utility function to update the Mapping of PigOperators to Drill Logical Operators.
    // A single Pig operator could be mapped to multiple Drill Operators.
    private void updateOperatorMappings(Operator pigOp, LogicalOperator drillOp){
        if(null == pigToDrillOperatorsMapping.get(pigOp)){
            List<LogicalOperator> l = new ArrayList();
            l.add(drillOp);
            pigToDrillOperatorsMapping.put(pigOp, l);
        }
        else{
            pigToDrillOperatorsMapping.get(pigOp).add(drillOp);
        }
    }

    // Gets the last Drill Logical Operator in mapping of PigOp => List<DrillOp>
    private LogicalOperator getLastDrillOperator(Operator pigOp){
        return (pigToDrillOperatorsMapping.get(pigOp)==null) ? (null) : (pigToDrillOperatorsMapping.get(pigOp).get(pigToDrillOperatorsMapping.get(pigOp).size() -1));
    }

    /**
     * Used internally by getLastDrillProjectedSchema to collect list of last
     * project nodes in drill logical plan.
     * Its used to fetch the last projected aliases/refs from NamedExprs.
     */
    private void getLastDrillProjections(Operator pigOp){
        if(getLastDrillOperator(pigOp) instanceof Project) {
            projectedOps.add((Project)getLastDrillOperator(pigOp));
            return;
        }
        else{
            List<Operator> opList = pigOp.getPlan().getPredecessors(pigOp);
            for (Operator op : opList) {
                getLastDrillProjections(op);
            }
        }
    }

    private List<String> getLastDrillProjectedSchema(Operator pigOp){
        projectedOps = new ArrayList<>();
        getLastDrillProjections(pigOp);
        List<String> aliases = new ArrayList<>();

        for(Project proj : projectedOps){
            for(NamedExpression ne : proj.getSelections().clone()){
                aliases.add(ne.getRef().getAsUnescapedPath());
            }
        }
        return aliases;
    }

    // Needs improvement. Need BFS Traversal.
    @Deprecated
    private String getDrillAliasForPigOp(Operator pigOp, String pigAlias, List<String> list){
        boolean found = false;

        if(list.contains(pigAlias)){
            return pigAlias;
        }
        else if (pigOp instanceof LogicalRelationalOperator) {
            try {
                for(LogicalSchema.LogicalFieldSchema f : (((LogicalRelationalOperator) pigOp).getSchema().getFields())){
                    String alias = ((LogicalRelationalOperator) pigOp).getAlias();
                    if(f.alias.equals(pigAlias) && (list.contains(pigAlias) || list.contains(f.alias))){
                        found = true;
                        return f.alias;
                    }
                }

                if(!found){
                    List<Operator> opList = pigOp.getPlan().getPredecessors(pigOp);
                    if(null == opList){
                        return null;
                    }
                    for (Operator op : opList) {
                        return getDrillAliasForPigOp(op, pigAlias, list);
                    }
                }

            } catch (FrontendException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    private String getDrillAliasForPigOp(Operator pigOp, String pigAlias){
        List<String> list = getLastDrillProjectedSchema(pigOp);
        return getDrillAliasForPigOp(pigOp, pigAlias, list);
    }

}
