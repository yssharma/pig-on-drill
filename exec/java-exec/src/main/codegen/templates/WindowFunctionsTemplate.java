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
<@pp.dropOutputFile />



<#list windowTypes.windowfuncs as windowfunc>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/${windowfunc.className}Functions.java" />

<#include "/@includes/license.ftl" />


/*
 * This class is automatically generated from WindowTypes.tdd using FreeMarker.
 */

package org.apache.drill.exec.expr.fn.impl.gaggr;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.expr.DrillWindowPointAggFunc;

@SuppressWarnings("unused")

public class ${windowfunc.className}Functions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${windowfunc.className}Functions.class);

<#list windowfunc.types as type>
<#if type.major == "Numeric">

@FunctionTemplate(name = "${windowfunc.funcName}", scope = FunctionTemplate.FunctionScope.WINDOW_POINT_AGGREGATE)
public static class ${type.inputType}${windowfunc.className} implements DrillWindowPointAggFunc {

  @Param ${type.inputType}Holder in;
  @Workspace ${type.inputType}Holder lastValue;
  @Workspace ${windowfunc.runningType}Holder rank;
  @Workspace ${windowfunc.runningType}Holder numberOfMatches;
  @Output ${windowfunc.outputType}Holder out;

  @Override
  public void setup(RecordBatch b) {
  rank = new ${windowfunc.runningType}Holder();
  rank.value = 0;
  numberOfMatches = new ${windowfunc.runningType}Holder();
  numberOfMatches.value = 0;
  lastValue = new ${type.inputType}Holder();
  }

  @Override
  public void add() {
    if(in.value == lastValue.value){
      numberOfMatches.value += 1;
    }
    else{
      rank.value += 1;
      numberOfMatches.value = 0;
    }
    lastValue.value = in.value;
  }

  @Override
  public void remove() {
    // TODO
  }

  @Override
  public void output() {
  <#if windowfunc.funcName == "rank_win">
      out.value = rank.value + numberOfMatches.value;
  <#elseif windowfunc.funcName == "dense_rank_win">
  out.value = rank.value;
  </#if>

  }

  @Override
  public void reset() {
  rank.value = 0;
  numberOfMatches.value = 0;
  }

 }

</#if>
</#list>
}
</#list>
