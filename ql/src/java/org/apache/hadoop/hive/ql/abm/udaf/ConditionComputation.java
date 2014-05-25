package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;

public class ConditionComputation extends UDAFComputation {

  private CondGroup condGroup = null;
  private List<List<ConditionRange>> rangeMatrix = null;
  private double[] newCondRanges = null;
  private boolean flag = true;
  private int dimension = 0;


  public void setCondGroup(CondGroup cond, int dimension) {
    this.condGroup = cond;
    this.dimension = dimension;
    newCondRanges = new double[dimension * 2];
  }

  public void setFields(IntArrayList keyArray, List<List<ConditionRange>> rangeMatrix) {
    this.rangeMatrix = rangeMatrix;
    this.condGroup.addGroup(keyArray);

    if (this.rangeMatrix.size() > 0) {
      if (this.rangeMatrix.get(0).size() > 0) {
        this.flag = this.rangeMatrix.get(0).get(0).getFlag();
      }
    }
  }

  @Override
  public void iterate(int index) {
  }

  @Override
  public void partialTerminate(int level, int start, int end) {
    if (this.flag) {
      newCondRanges[level * 2] = this.rangeMatrix.get(level).get(start).getValue(this.flag);
      newCondRanges[level * 2 + 1] = (end == this.rangeMatrix.get(level).size()) ? Double.POSITIVE_INFINITY
          : this.rangeMatrix.get(level).get(end).getValue(this.flag);
    } else {
      newCondRanges[level * 2] = (end == this.rangeMatrix.get(level).size()) ? Double.NEGATIVE_INFINITY
          : this.rangeMatrix.get(level).get(end).getValue(this.flag);
      newCondRanges[level * 2 + 1] = this.rangeMatrix.get(level).get(start).getValue(this.flag);
    }
  }

  @Override
  public void terminate() {
    for (int i = 0; i < this.dimension; i++) {
      this.condGroup.getRangeMatrix().get(i)
          .add(new ConditionRange(newCondRanges[2 * i], newCondRanges[2 * i + 1]));
      ;
    }
  }



}
