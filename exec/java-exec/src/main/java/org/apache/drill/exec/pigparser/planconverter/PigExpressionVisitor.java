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

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.NullExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.PigParsingException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UnaryExpression;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class PigExpressionVisitor {

    // Map for Mapping Pig's operators and UDF's to Drill's internal functions
    static Map<String, String> drillLookupMap;
    {
        drillLookupMap = new HashMap<>();
        // Binary functions
        drillLookupMap.put("Equal", "equal");
        drillLookupMap.put("NotEqual", "not_equal");
        drillLookupMap.put("Add", "add");
        drillLookupMap.put("Subtract", "subtract");
        drillLookupMap.put("Multiply", "multiply");
        drillLookupMap.put("Divide", "divide");
        drillLookupMap.put("Mod", "mod");
        drillLookupMap.put("And", "booleanAnd");
        drillLookupMap.put("Or", "booleanOr");
        drillLookupMap.put("GreaterThan", "greater_than");
        drillLookupMap.put("GreaterThanEqual", "greater_than_or_equal_to");
        drillLookupMap.put("LessThan", "less_than");
        drillLookupMap.put("LessThanEqual", "less_than_or_equal_to");

        // Unary functions
        drillLookupMap.put("IsNull", "is_null");
        drillLookupMap.put("Negetive", "negetive");
        drillLookupMap.put("Not", "not");

    }

    // Drill datatype mapping from Pig DataTypes
    static Map<Byte, TypeProtos.MajorType> datatypeMap;
    {
        datatypeMap = new HashMap<>();
        datatypeMap.put(DataType.UNKNOWN, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.NULL).build());
        datatypeMap.put(DataType.NULL, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.NULL).build());
        datatypeMap.put(DataType.BOOLEAN, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.BIT).build());
        datatypeMap.put(DataType.INTEGER, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.INT).build());
        datatypeMap.put(DataType.LONG, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.DECIMAL18).build());
        datatypeMap.put(DataType.FLOAT, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.FLOAT4).build());
        datatypeMap.put(DataType.DOUBLE, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.FLOAT8).build());
        datatypeMap.put(DataType.DATETIME, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.DATE).build());
        datatypeMap.put(DataType.CHARARRAY, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.VARCHAR).build());
        datatypeMap.put(DataType.BIGINTEGER, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.BIGINT).build());
        datatypeMap.put(DataType.BIGDECIMAL, TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.REQUIRED).setMinorType(TypeProtos.MinorType.DECIMAL28DENSE).build());
    }

    // Stack to hold Pig's Expression operators
    Stack<LogicalExpression> exprStack = new Stack<>();

    public LogicalExpression getDrillExpression(LogicalExpressionPlan pigExpression) throws PigParsingException, FrontendException {
        LogicalExpression drillExpression = null;
        Iterator<Operator> ops = pigExpression.getOperators(); // A STACK. Get OP pop last 2 operands and do Stuff and push back.

        while(ops.hasNext()){
            org.apache.pig.newplan.logical.expression.LogicalExpression op = (org.apache.pig.newplan.logical.expression.LogicalExpression)ops.next();

            if (op instanceof ProjectExpression){visit((ProjectExpression)op);}
            else if (op instanceof ConstantExpression){visit((ConstantExpression)op);}
            else if (op instanceof BinaryExpression){visit((BinaryExpression)op);}
            else if (op instanceof EqualExpression){visit((BinaryExpression)op);}
            else if (op instanceof NotEqualExpression){visit((BinaryExpression)op);}
            else if (op instanceof AddExpression){visit((BinaryExpression)op);}
            else if (op instanceof SubtractExpression){visit((BinaryExpression)op);}
            else if (op instanceof MultiplyExpression){visit((BinaryExpression)op);}
            else if (op instanceof DivideExpression){visit((BinaryExpression)op);}
            else if (op instanceof ModExpression){visit((BinaryExpression)op);}
            else if (op instanceof AndExpression){visit((BinaryExpression)op);}
            else if (op instanceof OrExpression){visit((BinaryExpression)op);}
            else if (op instanceof GreaterThanEqualExpression){visit((BinaryExpression)op);}
            else if (op instanceof GreaterThanExpression){visit((BinaryExpression)op);}
            else if (op instanceof LessThanEqualExpression){visit((BinaryExpression)op);}
            else if (op instanceof LessThanExpression){visit((BinaryExpression)op);}
            else if (op instanceof IsNullExpression){visit((UnaryExpression)op);}
            else if (op instanceof NegativeExpression){visit((UnaryExpression)op);}
            else if (op instanceof NotExpression){visit((UnaryExpression)op);}
            else if (op instanceof CastExpression){visit((CastExpression) op);}
            //else if(op instanceof BinCondExpression){}
            //else if (op instanceof UserFuncExpression){}
            //else if (op instanceof RegexExpression){}
            else { throw new PigParsingException("Operator not supported : "+op.getName());}

        }

        if(exprStack.size() != 1){throw new PigParsingException("Error in expression conversion.");}
        return exprStack.pop();
    }


    private void visit(ProjectExpression expr) throws FrontendException {
        exprStack.push(new org.apache.drill.common.expression.CastExpression(new FieldReference(expr.getColAlias()), datatypeMap.get(expr.getType()), ExpressionPosition.UNKNOWN));
    }

    private void visit(ConstantExpression expr) throws PigParsingException {
        if(expr.getValue() == null ){exprStack.push(new NullExpression());}
        else if (expr.getValue() instanceof Integer){exprStack.push(new ValueExpressions.IntExpression((Integer)expr.getValue(), ExpressionPosition.UNKNOWN));}
        else if (expr.getValue() instanceof Long){exprStack.push(new ValueExpressions.LongExpression((Long)expr.getValue(), ExpressionPosition.UNKNOWN));}
        else if (expr.getValue() instanceof Float){exprStack.push(new ValueExpressions.FloatExpression((Float)expr.getValue(), ExpressionPosition.UNKNOWN));}
        else if (expr.getValue() instanceof Double){exprStack.push(new ValueExpressions.DoubleExpression((Double)expr.getValue(), ExpressionPosition.UNKNOWN));}
        else if (expr.getValue() instanceof String){exprStack.push(new ValueExpressions.QuotedString((String)expr.getValue(), ExpressionPosition.UNKNOWN));}
        else if (expr.getValue() instanceof Boolean){exprStack.push(new ValueExpressions.BooleanExpression(expr.getValue().toString(),ExpressionPosition.UNKNOWN));}
        else if (expr.getValue() instanceof BigDecimal){exprStack.push(new ValueExpressions.Decimal18Expression((BigDecimal)expr.getValue(), ExpressionPosition.UNKNOWN));}
        else {throw new PigParsingException("Unsupported expression "+expr.getValue());}
    }


    private void visit(BinaryExpression expr) throws PigParsingException {
        visitBinaryExpression(expr);
    }

    private void visit(UnaryExpression expr) throws PigParsingException {
        visitUnaryExpression(expr);
    }

    private void visit(CastExpression expr) throws FrontendException {
        LogicalExpression in = exprStack.pop();
        exprStack.push(new org.apache.drill.common.expression.CastExpression(in, datatypeMap.get(expr.getFieldSchema().type), ExpressionPosition.UNKNOWN));
    }

    private void visitBinaryExpression(BinaryExpression expr) throws PigParsingException {
        String drillfuncname = drillLookupMap.get(expr.getName());
        List<LogicalExpression> params = new ArrayList<>();
        params.add(exprStack.pop());
        params.add(exprStack.pop());
        FunctionCall call = new FunctionCall(drillfuncname, params, ExpressionPosition.UNKNOWN);
        exprStack.push(call);
    }

    private void visitUnaryExpression(UnaryExpression expr) throws PigParsingException {
        String drillfuncname = drillLookupMap.get(expr.getName());
        List<LogicalExpression> params = new ArrayList<>();
        params.add(exprStack.pop());
        FunctionCall call = new FunctionCall(drillfuncname, params, ExpressionPosition.UNKNOWN);
        exprStack.push(call);
    }

}
