package org.apache.hadoop.hive.ql.abm.udaf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public abstract class GenericUDAFInstructionGetEvaluator extends GenericUDAFEvaluator {
  protected Instruction instruction;

  public Instruction getInstruction() {
    return instruction;
  }
}