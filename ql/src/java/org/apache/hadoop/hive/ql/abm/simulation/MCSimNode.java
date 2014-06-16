package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InnerCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InterCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvReader;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class MCSimNode {

  private final DistOracle[][] withinLevel;
  private final DistOracle[][] betweenLevel;

  public MCSimNode(List<Integer> gbyIds, List<List<UdafType>> udafTypes,
      List<Integer> gbyIdsInPreds, List<Integer> colsInPreds, List<PredicateType> predTypes,
      List<Integer> parentGbyIds,
      Int2ReferenceOpenHashMap<double[]>[] srvs, InnerCovMap[] inners, InterCovMap[][] inters,
      boolean independent) {
    int lastContinuousGby = inners.length - 1;

    // Initialize distribution oracles
    int numGbys1 = gbyIds.size();
    withinLevel = new DistOracle[numGbys1][numGbys1];
    for (int i = 0; i < numGbys1; ++i) {
      DistOracle[] level = new DistOracle[numGbys1];
      withinLevel[i] = level;

      int gby1 = gbyIds.get(i);
      List<UdafType> udaf1 = udafTypes.get(i);
      boolean continuous1 = (gby1 <= lastContinuousGby);
      SrvReader reader = SrvReader.createReader(udaf1.size(), continuous1);

      if (independent || !continuous1) {
        level[i] = new IndependentInnerDistOracle(srvs[gby1], reader, udaf1);
      } else {
        level[i] = new CorrelatedInnerDistOracle(srvs[gby1], reader, inners[gby1], udaf1);
      }

      for (int j = i + 1; j < numGbys1; ++j) {
        int gby2 = gbyIds.get(j);
        List<UdafType> udaf2 = udafTypes.get(j);
        boolean continuous2 = (gby2 <= lastContinuousGby);
        if (independent || !continuous1 || !continuous2) {
          level[i] = new IndependentInterDistOracle(udaf1, udaf2);
        } else {
          level[i] = new CorrelatedInterDistOracle(inters[i][j], udaf1, udaf2);
        }
      }
    }

    int numGbys2 = parentGbyIds.size();
    betweenLevel = new DistOracle[numGbys1][numGbys2];
    for (int i = 0; i < numGbys1; ++i) {
      DistOracle[] level = new DistOracle[numGbys2];
      betweenLevel[i] = level;

      int gby1 = gbyIds.get(i);
      List<UdafType> udaf1 = udafTypes.get(i);
      boolean continuous1 = (gby1 <= lastContinuousGby);

      for (int j = 0; j < numGbys2; ++j) {
        int gby2 = gbyIds.get(j);
        List<UdafType> udaf2 = udafTypes.get(j);
        boolean continuous2 = (gby2 <= lastContinuousGby);
        if (independent || !continuous1 || !continuous2) {
          level[i] = new IndependentInterDistOracle(udaf1, udaf2);
        } else {
          level[i] = new CorrelatedInterDistOracle(inters[i][j], udaf1, udaf2);
        }
      }
    }

    // Initialize condition reader
    // TODO
  }

  public void init() {
    //
  }

}
