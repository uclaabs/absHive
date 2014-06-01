package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;

public class ConditionComputation extends UDAFComputation {

  private final CondGroup condGroups = new CondGroup();
  private final CondList condList = new CondList();
  private List<RangeList> rangeMatrix = null;
  private double[] newCondRanges = null;
  private List<Boolean> flags = null;
  private int dim = 0;

  public void setCondGroup(int dimension) {
    dim = dimension;
    newCondRanges = new double[dimension * 2];
  }

  public void setFields(KeyWrapper keyArray, List<RangeList> rangeMatrix) {
    this.rangeMatrix = rangeMatrix;
    this.condGroups.addGroup(dim, keyArray);
  }

  public void clear() {
    condGroups.clear();
    condList.clear();
    rangeMatrix = null;
    flags.clear();
  }

  @Override
  public void iterate(int index) {
  }

  @Override
  public void partialTerminate(int level, int start, int end) {
    boolean flag = flags.get(level);
    if (flag) {
      newCondRanges[level * 2] = rangeMatrix.get(level).getValue(flag, start);
      newCondRanges[level * 2 + 1] = (end == rangeMatrix.get(level).numCases()) ? Double.POSITIVE_INFINITY
          : this.rangeMatrix.get(level).getValue(flag, end);
    } else {
      newCondRanges[level * 2] = (end == rangeMatrix.get(level).numCases()) ? Double.NEGATIVE_INFINITY
          : this.rangeMatrix.get(level).getValue(flag, end);
      newCondRanges[level * 2 + 1] = rangeMatrix.get(level).getValue(flag, start);
    }
  }

  @Override
  public void terminate() {
    for (int i = 0; i < dim; i++) {
      condGroups.getRangeMatrix().get(i).add(newCondRanges[2 * i]);
      condGroups.getRangeMatrix().get(i).add(newCondRanges[2 * i + 1]);
    }
  }

  @Override
  public void unfold() {
    // unfold the conditions
    for (KeyWrapper currentKeys : condGroups.getKeys()) {
      condList.addKeys(currentKeys);
    }

    for (int i = 0; i < dim * condGroups.getGroupNumber(); i++) {
     condList.addRanges(new RangeList());
    }

    double[] rangeArray = new double[dim * condGroups.getGroupNumber() * 2];
    unfoldRangeMatrix(0, rangeArray);
  }

  private void unfoldRangeMatrix(int level, double[] rangeArray) {
    boolean leaf = (level == condGroups.getGroupNumber() - 1);

    List<RangeList> currentRangeMatrix = condGroups.getRangeMatrix(level);
    int rowNumber = currentRangeMatrix.get(0).numCases();

    for (int i = 0; i < rowNumber; i++) {
      for (int j = 0; j < dim; j++) {
        int index = 2 * (level * dim + j);
        rangeArray[index] = currentRangeMatrix.get(j).getLower(i);
        rangeArray[index + 1] = currentRangeMatrix.get(j).getUpper(i);
      }

      if (leaf) {
        condList.addRange(rangeArray);
      } else {
        unfoldRangeMatrix(level + 1, rangeArray);
      }
    }
  }

  public void setFlags(List<Boolean> flags) {
    this.flags = flags;
  }

  @Override
  public Object serializeResult() {
    return condList.toArray();
  }

  @Override
  public void reset() {
  }

}
