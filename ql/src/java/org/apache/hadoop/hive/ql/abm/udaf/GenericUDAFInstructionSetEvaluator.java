package org.apache.hadoop.hive.ql.abm.udaf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public abstract class GenericUDAFInstructionSetEvaluator extends GenericUDAFEvaluator {
    protected Instruction instruction;
    /**
     * id indicates in which group derived from the group by operator
     */
    protected Integer id;

    public GenericUDAFInstructionSetEvaluator () {}

    public void setInstruction(Instruction ins) {
      this.instruction = ins;
    }

    public void setId(Integer id) {
      this.id = id;
    }
}