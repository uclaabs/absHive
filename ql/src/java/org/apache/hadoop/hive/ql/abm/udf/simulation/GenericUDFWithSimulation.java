package org.apache.hadoop.hive.ql.abm.udf.simulation;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

public abstract class GenericUDFWithSimulation extends GenericUDF {

  protected int numSimulation;
  protected DoubleArrayList samples;

  public void setSamples(DoubleArrayList samples) {
    this.samples = samples;
  }

  public void setNumSimulation(int numSimulation) {
    this.numSimulation = numSimulation;
  }

}