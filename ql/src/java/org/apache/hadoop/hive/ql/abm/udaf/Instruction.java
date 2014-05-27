package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.ArrayList;
import java.util.List;


public class Instruction {
  
  private int groupInstruction = Integer.MIN_VALUE;
  private List<Merge> mergeInstruction = new ArrayList<Merge>();
  
  public void setGroupInstruction(int Instruction)
  {
    this.groupInstruction = Instruction;
  }
  
  public int getGroupInstruction()
  {
    return this.groupInstruction;
  }
  
  public void setMergeInstruction(Merge Instruction)
  {
    this.mergeInstruction.add(Instruction);
  }
  
  public List<Merge> getMergeInstruction()
  {
    return this.mergeInstruction;
  }
  
  public void reset()
  {
    this.groupInstruction = Integer.MIN_VALUE;
    this.mergeInstruction.clear();
  }


}
