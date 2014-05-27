package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class Instruction {

  private final IntArrayList groupInstruction = new IntArrayList();
  private final List<Merge> mergeInstruction = new ArrayList<Merge>();

  public void addGroupInstruction(int instruction) {
    groupInstruction.add(instruction);
  }

  public IntArrayList getGroupInstruction() {
    return groupInstruction;
  }

  public void addMergeInstruction(Merge instruction) {
    mergeInstruction.add(instruction);
  }

  public List<Merge> getMergeInstruction() {
    return mergeInstruction;
  }

  public void reset() {
    groupInstruction.clear();;
    mergeInstruction.clear();
  }

}
