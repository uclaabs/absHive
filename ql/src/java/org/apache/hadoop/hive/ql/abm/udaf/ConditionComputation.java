package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.CondGroups;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;

public class ConditionComputation extends UDAFComputation {

  private final CondGroups condGroups = new CondGroups();
  private CondGroup finalCondGroup = null;
  private List<List<ConditionRange>> rangeMatrix = null;
  private double[] newCondRanges = null;
  private List<Boolean> flags = null;
  private int dim = 0;

  public void setCondGroup(int dimension) {
    dim = dimension;
    newCondRanges = new double[dimension * 2];
  }

  public void setFields(IntArrayList keyArray, List<List<ConditionRange>> rangeMatrix) {
    this.rangeMatrix = rangeMatrix;
    this.condGroups.addGroup(keyArray);
  }

  @Override
  public void iterate(int index) {
  }

  @Override
  public void partialTerminate(int level, int start, int end) {
    boolean flag = flags.get(level);
    if (flag) {
      newCondRanges[level * 2] = this.rangeMatrix.get(level).get(start).getValue(flag);
      newCondRanges[level * 2 + 1] = (end == this.rangeMatrix.get(level).size()) ? Double.POSITIVE_INFINITY
          : this.rangeMatrix.get(level).get(end).getValue(flag);
    } else {
      newCondRanges[level * 2] = (end == this.rangeMatrix.get(level).size()) ? Double.NEGATIVE_INFINITY
          : this.rangeMatrix.get(level).get(end).getValue(flag);
      newCondRanges[level * 2 + 1] = this.rangeMatrix.get(level).get(start).getValue(flag);
    }
  }

  @Override
  public void terminate() {
    for (int i = 0; i < dim; i++) {
      condGroups.getRangeMatrix().get(i)
          .add(new ConditionRange(newCondRanges[2 * i], newCondRanges[2 * i + 1]));
    }
  }

  @Override
  public void unfold() {
    // unfold the conditions
    List<Integer> unfoldKeys = new ArrayList<Integer>();
    for (List<Integer> currentKeys : condGroups.getKeys()) {
      unfoldKeys.addAll(currentKeys);
    }

    List<List<ConditionRange>> unfoldRangeMatrix = new ArrayList<List<ConditionRange>>();
    for (int i = 0; i < dim * condGroups.getGroupNumber(); i++) {
      unfoldRangeMatrix.add(new ArrayList<ConditionRange>());
    }

    ConditionRange[] rangeArray = new ConditionRange[dim * condGroups.getGroupNumber()];
    unfoldRangeMatrix(0, rangeArray, unfoldRangeMatrix);

    // TODO: reuse
    this.finalCondGroup = new CondGroup(unfoldKeys, unfoldRangeMatrix);
  }

  private void unfoldRangeMatrix(int level, ConditionRange[] rangeArray,
      List<List<ConditionRange>> rangeMatrix) {
    boolean leaf = (level == condGroups.getGroupNumber() - 1);

    List<List<ConditionRange>> currentRangeMatrix = condGroups.getRangeMatrix(level);
    int rowNumber = currentRangeMatrix.get(0).size();

    for (int i = 0; i < rowNumber; i++) {
      for (int j = 0; j < dim; j++) {
        rangeArray[level * dim + j] = currentRangeMatrix.get(j).get(i);
      }

      if (leaf) {
        for (int dim = 0; dim < rangeArray.length; dim++) {
          rangeMatrix.get(dim).add(rangeArray[dim]);
        }
      } else {
        unfoldRangeMatrix(level + 1, rangeArray, rangeMatrix);
      }
    }
  }

  public void setFlags(List<Boolean> flags) {
    this.flags = flags;
  }

  @Override
  public Object serializeResult() {
    return this.finalCondGroup.toArray();
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub

  }

}
