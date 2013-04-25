package org.apache.drill.exec.ref.rops;

import com.google.common.collect.Lists;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.RunningAggregate;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.TestUtils;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes;
import org.apache.drill.exec.ref.eval.fn.FunctionArguments;
import org.apache.drill.exec.ref.eval.fn.FunctionEvaluatorRegistry;
import org.apache.drill.exec.ref.eval.fn.agg.CountAggregator;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class RunningAggregateROPTest {
    @Test
    public void shouldComputeRunningSum() throws IOException {
        String input = "{\"id\":1, \"amount\": 5}\n{\"id\":2, \"amount\":10}";
        String outputPath = "test.output";
        SchemaPath outputSchemaPath = new SchemaPath(outputPath);
        FieldReference ref = new FieldReference(outputPath);
        FunctionDefinition funcDef =  FunctionDefinition.aggregator("sum",  new ArgumentValidators.AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput());
        LogicalExpression expr = funcDef.newCall(Lists.<LogicalExpression>newArrayList(new SchemaPath("test.amount")));
        NamedExpression[] namedExpr = new NamedExpression[]{new NamedExpression(expr, ref)};
        RunningAggregateROP rop = new RunningAggregateROP(new RunningAggregate(namedExpr, null));
        RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
        rop.setInput(incoming);
        rop.setupEvals(new BasicEvaluatorFactory(null));
        RecordIterator output = rop.getOutput();
        RecordPointer pointer = output.getRecordPointer();
        assertEquals(RecordIterator.NextOutcome.INCREMENTED_SCHEMA_CHANGED, output.next());
        assertEquals(5, pointer.getField(outputSchemaPath).getAsNumeric().getAsLong());
        assertEquals(RecordIterator.NextOutcome.INCREMENTED_SCHEMA_CHANGED, output.next());
        assertEquals(15, pointer.getField(outputSchemaPath).getAsNumeric().getAsLong());
        assertEquals(RecordIterator.NextOutcome.NONE_LEFT, output.next());
    }

    @Test
    public void shouldComputeRunningSumWithin() throws IOException {
        String input = "{\"id\":1, \"amount\": 5, \"boundary\": null}\n" +
                       "{\"id\":2, \"amount\":10, \"boundary\": 1}\n" +
                       "{\"id\":3, \"amount\":15, \"boundary\": 1}\n" +
                       "{\"id\":4, \"amount\":20, \"boundary\": 2}\n" +
                       "{\"id\":5, \"amount\":25, \"boundary\": 2}\n";
        String outputPath = "test.output";
        SchemaPath outputSchemaPath = new SchemaPath(outputPath);
        FieldReference ref = new FieldReference(outputPath);
        FunctionDefinition funcDef =  FunctionDefinition.aggregator("sum",  new ArgumentValidators.AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput());
        LogicalExpression expr = funcDef.newCall(Lists.<LogicalExpression>newArrayList(new SchemaPath("test.amount")));
        NamedExpression[] namedExpr = new NamedExpression[]{new NamedExpression(expr, ref)};
        RunningAggregateROP rop = new RunningAggregateROP(new RunningAggregate(namedExpr, new FieldReference("test.boundary")));
        RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
        rop.setInput(incoming);
        rop.setupEvals(new BasicEvaluatorFactory(null));
        RecordIterator output = rop.getOutput();
        RecordPointer pointer = output.getRecordPointer();
        assertEquals(RecordIterator.NextOutcome.INCREMENTED_SCHEMA_CHANGED, output.next());
        assertEquals(5, pointer.getField(outputSchemaPath).getAsNumeric().getAsLong());
        assertEquals(RecordIterator.NextOutcome.INCREMENTED_SCHEMA_CHANGED, output.next());
        assertEquals(10, pointer.getField(outputSchemaPath).getAsNumeric().getAsLong());
        assertEquals(RecordIterator.NextOutcome.INCREMENTED_SCHEMA_CHANGED, output.next());
        assertEquals(25, pointer.getField(outputSchemaPath).getAsNumeric().getAsLong());
        assertEquals(RecordIterator.NextOutcome.INCREMENTED_SCHEMA_CHANGED, output.next());
        assertEquals(20, pointer.getField(outputSchemaPath).getAsNumeric().getAsLong());
        assertEquals(RecordIterator.NextOutcome.INCREMENTED_SCHEMA_CHANGED, output.next());
        assertEquals(45, pointer.getField(outputSchemaPath).getAsNumeric().getAsLong());
        assertEquals(RecordIterator.NextOutcome.NONE_LEFT, output.next());
    }
}
