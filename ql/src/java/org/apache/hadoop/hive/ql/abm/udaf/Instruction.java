package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class Instruction {

  private int tmp = -1;
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

  public void resetGroupInstruction() {
    groupInstruction.clear();
  }

  public void resetMergeInstruction() {
    mergeInstruction.clear();
  }

  public void fakeIterate() {
    tmp ++;
    if(tmp == 3) {
      tmp = -1;
    }
    groupInstruction.add(tmp);
  }

}
