package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.hadoop.hive.ql.abm.datatypes.SrvReader;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;

public abstract class InnerDistOracle {

  protected final Int2ReferenceOpenHashMap<SrvTuple> srv;
  protected final SrvReader reader;

  private final IntArrayList groupIds;
  protected final int elemDim;
  private final OffsetInfo offInfo;

  public InnerDistOracle(Int2ReferenceOpenHashMap<SrvTuple> srv, boolean continuous,
      IntArrayList groupIds, int elemDim, OffsetInfo offInfo) {
    this.srv = srv;
    reader = SrvReader.createReader(elemDim, continuous);
    this.groupIds = groupIds;
    this.elemDim = elemDim;
    this.offInfo = offInfo;
  }

  public void fill(IntArrayList condIds, boolean[] fake, double[] mean, double[][] cov) {
    for (int i = 0, cur = offInfo.pos, off1 = offInfo.offset; i < groupIds.size(); ++i, ++cur, off1 += elemDim) {
      boolean f = fillMeanAndCov(groupIds.getInt(i), condIds.getInt(cur), mean, cov, off1);
      fake[cur] = f;
      if (!f) {
        for (int j = i + 1, off2 = off1 + elemDim; j < groupIds.size(); ++j, off2 += elemDim) {
          fillCov(mean, cov, off1, off2);
        }
      }
    }
  }

  protected abstract boolean fillMeanAndCov(int groupId, int condId, double[] mean, double[][] cov,
      int offset);

  protected abstract void fillCov(double[] mean, double[][] cov, int offset1, int offset2);

}