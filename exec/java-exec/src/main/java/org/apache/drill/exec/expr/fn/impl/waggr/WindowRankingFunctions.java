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

package org.apache.drill.exec.expr.fn.impl.waggr;

import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.DrillWindowPointAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class WindowRankingFunctions {

  /*
  @FunctionTemplate(name = "rank_win", scope = FunctionTemplate.FunctionScope.WINDOW_POINT_AGGREGATE)
  public static class Rank implements DrillFunc {

  }

  @FunctionTemplate(name = "dense_rank_win", scope = FunctionTemplate.FunctionScope.WINDOW_POINT_AGGREGATE)
  public static class DenseRank implements DrillFunc {

  }

  @FunctionTemplate(name = "ntile_win", scope = FunctionTemplate.FunctionScope.WINDOW_POINT_AGGREGATE)
  public static class Ntile implements DrillFunc {

  }
  */

  @FunctionTemplate(name = "row_number_win", scope = FunctionTemplate.FunctionScope.WINDOW_POINT_AGGREGATE)
  public static class RowNumberWin implements DrillWindowPointAggFunc {
    @Workspace BigIntHolder count;
    @Output BigIntHolder row;

    @Override
    public void setup(RecordBatch incoming) {
      count = new BigIntHolder();
      count.value = 0;
    }

    @Override
    public void add() {
      count.value++;
    }

    @Override
    public void remove() {
      count.value--;
    }

    @Override
    public void output() {
      row.value = count.value;
    }

    @Override
    public void reset() {
      count.value = 0;
    }
  }
}
