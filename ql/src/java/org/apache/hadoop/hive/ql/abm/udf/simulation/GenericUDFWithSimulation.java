package org.apache.hadoop.hive.ql.abm.udf.simulation;

import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

@UDFType(stateful = true)
public abstract class GenericUDFWithSimulation extends GenericUDF {

  protected int numSimulation;
  protected SimulationSamples samples = null;
  protected int idx;

  public void setNumSimulation(int numSimulation) {
    this.numSimulation = numSimulation;
  }

  public void setSamples(SimulationSamples samples) {
    this.samples = samples;
  }

  public void setIdx(int idx) {
    this.idx = idx;
  }

}