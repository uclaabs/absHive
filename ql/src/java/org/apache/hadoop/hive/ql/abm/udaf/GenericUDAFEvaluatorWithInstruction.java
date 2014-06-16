package org.apache.hadoop.hive.ql.abm.udaf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public abstract class GenericUDAFEvaluatorWithInstruction extends GenericUDAFEvaluator {

  protected Instruction ins;
  protected int N;

  public void setFlags(int N) {
    this.N = N;
  }

  public void setInstruction(Instruction ins) {
    this.ins = ins;
  }
}
