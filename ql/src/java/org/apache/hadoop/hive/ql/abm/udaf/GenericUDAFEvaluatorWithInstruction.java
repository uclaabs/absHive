package org.apache.hadoop.hive.ql.abm.udaf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public abstract class GenericUDAFEvaluatorWithInstruction extends GenericUDAFEvaluator {

  protected Instruction ins;

  public void setInstruction(Instruction ins) {
    this.ins = ins;
  }

}
