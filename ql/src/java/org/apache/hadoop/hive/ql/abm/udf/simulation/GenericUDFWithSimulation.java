/*
 * Copyright (C) 2015 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.abm.udf.simulation;

import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

@UDFType(stateful = true)
public abstract class GenericUDFWithSimulation extends GenericUDF {

  protected int numSimulation;
  protected SimulationSamples samples = null;
  protected int columnIndex;

  public void setNumSimulation(int numSimulation) {
    this.numSimulation = numSimulation;
  }

  public void setSamples(SimulationSamples samples) {
    this.samples = samples;
  }

  public void setColumnIndex(int columnIndex) {
    this.columnIndex = columnIndex;
  }

}