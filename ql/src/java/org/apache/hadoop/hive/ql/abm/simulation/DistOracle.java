package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;


public interface DistOracle {

  public int getRowSize();

  public int getColSize();

}

abstract class InnerDistOracle implements DistOracle {

  public int fill(IntArrayList groupIds, IntArrayList condIds, int pos, boolean[] fake,
      double[] mean, double[][] cov, int offset) {
    for (int i = 0; i < groupIds.size(); ++i, ++pos) {
      boolean f = fillMeanAndCov(groupIds.getInt(i), condIds.getInt(pos), mean, cov, offset);
      fake[pos] = f;
      if (!f) {
        for (int j = i + 1, offset2 = offset + getColSize(); j < groupIds.size(); ++j, offset2 += getColSize()) {
          fillCov(mean, cov, offset, offset2);
        }
      }
      offset += getRowSize();
    }
    return offset; // new offset
  }

  public abstract boolean fillMeanAndCov(int groupId, int condId, double[] mean, double[][] cov,
      int offset);

  public abstract void fillCov(double[] mean, double[][] cov, int offset1, int offset2);

}

abstract class InterDistOracle implements DistOracle {


  public int fillSym(IntArrayList groupIds1, IntArrayList groupIds2, IntArrayList condIds,
      int pos1, int pos2,
      boolean[] fake,
      double[] mean, double[][] cov, int offset1, int offset2) {
    int rowOffset = offset1;
    for (int i = 0; i < groupIds1.size(); ++i, ++pos1, rowOffset += getRowSize()) {
      if (!fake[pos1]) {
        int lg = groupIds1.getInt(i);
        int colOffset = offset2;
        for (int j = 0, tmp = pos2; j < groupIds2.size(); ++j, ++tmp, colOffset += getColSize()) {
          if (!fake[tmp]) {
            fillCovSym(lg, groupIds1.getInt(j), condIds.getInt(pos1), condIds.getInt(pos2), mean,
                mean, cov, rowOffset, colOffset);
          }
        }
      }
    }

    return offset2 + getColSize() * groupIds1.size();
  }

  public int fillAsym(IntArrayList groupIds1, IntArrayList groupIds2, IntArrayList condIds,
      int pos1, int pos2,
      boolean[] fake,
      double[] mean1, double[] mean2, double[][] cov, int offset1, int offset2) {
    int rowOffset = offset1;
    for (int i = 0; i < groupIds1.size(); ++i, ++pos1, rowOffset += getRowSize()) {
      if (!fake[pos1]) {
        int lg = groupIds1.getInt(i);
        int colOffset = offset2;
        for (int j = 0, tmp = pos2; j < groupIds2.size(); ++j, ++tmp, colOffset += getColSize()) {
          if (!fake[tmp]) {
            fillCovAsym(lg, groupIds1.getInt(j), condIds.getInt(pos1), condIds.getInt(pos2), mean1,
                mean2, cov, rowOffset, colOffset);
          }
        }
      }
    }

    return offset2 + getColSize() * groupIds1.size();
  }

  public abstract void fillCovSym(int groupId1, int groupId2, int condId1, int condId2,
      double[] mean1,
      double[] mean2, double[][] cov, int offset1, int offset2);

  public abstract void fillCovAsym(int groupId1, int groupId2, int condId1, int condId2,
      double[] mean1,
      double[] mean2, double[][] cov, int offset1, int offset2);

}
