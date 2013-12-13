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
package org.apache.drill.exec.physical.impl.join;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashJoin;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.IntVector;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.*;

/**
 * Hash join batch that stores one side into a in-memory hash and test conditions on the left
 */
public class HashJoinBatch extends AbstractRecordBatch<HashJoin> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashJoinBatch.class);

  private final HashJoin join;
  private final RecordBatch iteratedBatch;
  private final RecordBatch cachedBatch;
  private final HashJoinState currentState;

  public HashJoinBatch(HashJoin join, FragmentContext context, RecordBatch iteratedBatch, RecordBatch cachedBatch) {
    super(join, context);
    this.iteratedBatch = iteratedBatch;
    this.cachedBatch = cachedBatch;
    this.join = join;
    this.currentState = HashJoinState.BUILD;
  }

  @Override
  protected void killIncoming() {

  }

  @Override
  public int getRecordCount() {
    return 0;
  }

  @Override
  public IterOutcome next() {
    if (currentState == HashJoinState.BUILD) {
      logger.debug("Building hash join in memory table.");
      IterOutcome outcome = cachedBatch.next();
      switch (outcome) {
        case OK:
        case OK_NEW_SCHEMA:

          break;
        default:
          break;
      }

    }

    return IterOutcome.NONE;
  }

  private void generateNewWorker() throws ClassTransformationException {

    // materialize value vector readers from join expression
    for (JoinCondition condition : join.getConditions()) {
      final CodeGenerator<JoinWorker> cg = new CodeGenerator<>(HashJoinTemplate.TEMPLATE_DEFINITION, context.getFunctionRegistry());
      final ErrorCollector errorCollector = new ErrorCollectorImpl();
      final LogicalExpression leftFieldExpr = condition.getLeft();
      final LogicalExpression rightFieldExpr = condition.getRight();

      final LogicalExpression materializedCachedExpr = ExpressionTreeMaterializer.materialize(condition.getRight(), cachedBatch, errorCollector);
      if (errorCollector.hasErrors())
        throw new ClassTransformationException(String.format(
            "Failure while trying to materialize incoming left field.  Errors:\n %s.", errorCollector.toErrorString()));
      //materializedCachedExpr.

      DB db = DBMaker.newDirectMemoryDB().make();
      HTreeMap<Integer, IntArrayList> map = db.createHashMap (context.getHandle().getQueryId().toString()).make();
      cachedBatch.getValueAccessorById(0, IntVector.class).getValueVector();
      map.get()

    }

  }

  private enum HashJoinState {
    BUILD,
    PROBE
  }
}
