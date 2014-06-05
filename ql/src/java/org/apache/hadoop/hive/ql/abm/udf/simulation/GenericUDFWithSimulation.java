package org.apache.hadoop.hive.ql.abm.udf.simulation;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

public abstract class GenericUDFWithSimulation extends GenericUDF {

  protected int numSimulation;
  protected double[] samples; // TODO: should use DoubleArrayList, hack for now

  public void setSamples(double[] samples) {
    this.samples = samples;
  }

  public void setNumSimulation(int numSimulation) {
    this.numSimulation = numSimulation;
  }

}