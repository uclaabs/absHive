package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionIO;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.io.BytesWritable;

public class ConditionComputation extends UDAFComputation {

  private final int dim;
  private final double[] newCondRanges;

  private List<RangeList> rangeMatrix = null;
  private final CondGroup condGroups = new CondGroup();

  private final CondList condList = new CondList();

  public ConditionComputation(int dimension) {
    dim = dimension;
    newCondRanges = new double[dim];
  }

  public void setFields(KeyWrapper keyArray, List<RangeList> rangeMatrix) {
    this.rangeMatrix = rangeMatrix;
    this.condGroups.addGroup(dim, keyArray);
  }

  public void clear() {
    condGroups.clear();
    condList.clear();
    rangeMatrix = null;
  }

  @Override
  public void iterate(int index) {
  }

  @Override
  public void partialTerminate(int level, int index) {
    RangeList rangeList = rangeMatrix.get(level);
    newCondRanges[level] = rangeList.get(index);
  }

  @Override
  public void terminate() {
    List<RangeList> matrix = condGroups.getRangeMatrix();
    for (int i = 0; i < dim; i++) {
      matrix.get(i).add(newCondRanges[i]);
    }
  }

  @Override
  public void unfold() {
    if (this.dim == 0 || this.condGroups.getGroupNumber() == 0) {
      return;
    }

    // unfold the conditions
    for (KeyWrapper currentKeys : condGroups.getKeys()) {
      condList.addKeys(currentKeys);
    }

    for (int i = 0; i < dim * condGroups.getGroupNumber(); i++) {
      condList.addRanges(new RangeList());
    }

    double[] rangeArray = new double[dim * condGroups.getGroupNumber()];
    unfoldRangeMatrix(0, rangeArray);
  }

  private void unfoldRangeMatrix(int level, double[] rangeArray) {
    boolean leaf = (level == condGroups.getGroupNumber() - 1);

    List<RangeList> currentRangeMatrix = condGroups.getRangeMatrix(level);
    int rowNumber = currentRangeMatrix.get(0).size();

    for (int i = 0; i < rowNumber; i ++) {
      for (int j = 0; j < dim; j ++) {
        rangeArray[level * dim + j] = currentRangeMatrix.get(j).get(i);
      }

      if (leaf) {
        condList.addRange(rangeArray);
      } else {
        unfoldRangeMatrix(level + 1, rangeArray);
      }
    }
  }

  @Override
  public Object serializeResult() {
    // return condList.toArray();
    return new BytesWritable(ConditionIO.serialize(condList.getKeyList(), condList.getRangeMatrix()));
  }

  @Override
  public void reset() {
  }

}
