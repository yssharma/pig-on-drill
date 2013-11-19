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
package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractExchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.physical.base.Sender;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

import static org.apache.drill.exec.proto.CoordinationProtos.*;

@JsonTypeName("single-broadcast-exchange")
public class SingleBroadcastExchange extends AbstractExchange {

  private DrillbitEndpoint senderLocation;
  private List<DrillbitEndpoint> receiverLocations;

  @JsonCreator
  public SingleBroadcastExchange(@JsonProperty("child") PhysicalOperator child) {
    super(child);
  }

  @Override
  protected void setupSenders(List<DrillbitEndpoint> senderLocations) throws PhysicalOperatorSetupException {
    if (senderLocations.size() != 1)
      throw new PhysicalOperatorSetupException("SingleBroadcastExchange only supports a single sender endpoint");
    senderLocation = Iterators.getOnlyElement(senderLocations.iterator());
  }

  @Override
  protected void setupReceivers(List<CoordinationProtos.DrillbitEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    this.receiverLocations = receiverLocations;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SingleBroadcastExchange(child);
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) throws PhysicalOperatorSetupException {
    return new BroadcastSender(minorFragmentId, child, receiverLocations);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    return new RandomReceiver(senderMajorFragmentId, Lists.newArrayList(senderLocation));
  }

  @Override
  public int getMaxSendWidth() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean supportsSelectionVector() {
    return true;
  }
}
