package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InnerCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InterCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvReader;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class MCSimNode {

  private final IntArrayList[] target;
  private final int[] numAggrs;
  private int dimension = 0;

  private final InnerDistOracle[] withinLevel1;
  private final InterDistOracle[][] withinLevel2;
  private final List<InterDistOracle[][]> betweenLevel;

  private final KeyReader reader;

  private MCSimNode parent = null;

  public MCSimNode(List<Integer> gbyIds, List<List<UdafType>> udafTypes,
      List<Integer> gbyIdsInPreds, List<Integer> colsInPreds, List<PredicateType> predTypes,
      List<List<Integer>> parentGbyIds,
      TupleMap[] srvs, InnerCovMap[] inners, InterCovMap[][] inters,
      boolean independent) {
    int numGbys1 = gbyIds.size();

    target = new IntArrayList[numGbys1];
    numAggrs = new int[numGbys1];
    for (int i = 0; i < numGbys1; ++i) {
      numAggrs[i] = udafTypes.get(gbyIds.get(i)).size();
    }

    // Initialize distribution oracles
    int lastContinuousGby = inners.length - 1;
    withinLevel1 = new InnerDistOracle[numGbys1];
    withinLevel2 = new InterDistOracle[numGbys1][numGbys1];
    for (int i = 0; i < numGbys1; ++i) {
      InterDistOracle[] level = new InterDistOracle[numGbys1];
      withinLevel2[i] = level;

      int gby1 = gbyIds.get(i);
      List<UdafType> udaf1 = udafTypes.get(gby1);
      boolean continuous1 = (gby1 <= lastContinuousGby);
      SrvReader reader = SrvReader.createReader(udaf1.size(), continuous1);

      if (independent || !continuous1) {
        withinLevel1[i] = new IndependentInnerDistOracle(srvs[gby1], reader, udaf1);
      } else {
        withinLevel1[i] = new CorrelatedInnerDistOracle(srvs[gby1], reader, inners[gby1], udaf1);
      }

      for (int j = i + 1; j < numGbys1; ++j) {
        int gby2 = gbyIds.get(j);
        List<UdafType> udaf2 = udafTypes.get(gby2);
        boolean continuous2 = (gby2 <= lastContinuousGby);
        if (independent || !continuous1 || !continuous2) {
          level[i] = new IndependentInterDistOracle(udaf1, udaf2);
        } else {
          level[i] = new CorrelatedInterDistOracle(inters[i][j], udaf1, udaf2);
        }
      }
    }

    betweenLevel = new ArrayList<InterDistOracle[][]>(parentGbyIds.size());
    for (List<Integer> parents : parentGbyIds) {
      int numGbys2 = parents.size();
      InterDistOracle[][] cur = new InterDistOracle[numGbys1][numGbys2];
      betweenLevel.add(cur);
      for (int i = 0; i < numGbys1; ++i) {
        InterDistOracle[] level = new InterDistOracle[numGbys2];
        cur[i] = level;

        int gby1 = gbyIds.get(i);
        List<UdafType> udaf1 = udafTypes.get(gby1);
        boolean continuous1 = (gby1 <= lastContinuousGby);

        for (int j = 0; j < numGbys2; ++j) {
          int gby2 = parents.get(j);
          List<UdafType> udaf2 = udafTypes.get(gby2);
          boolean continuous2 = (gby2 <= lastContinuousGby);
          if (independent || !continuous1 || !continuous2) {
            level[i] = new IndependentInterDistOracle(udaf1, udaf2);
          } else {
            level[i] = new CorrelatedInterDistOracle(inters[i][j], udaf1, udaf2);
          }
        }
      }
    }

    // Initialize condition reader
    reader = new KeyReader(gbyIds, numAggrs, gbyIdsInPreds, colsInPreds, predTypes, srvs);
  }

  public void setParent(MCSimNode parent) {
    this.parent = parent;
  }

  public List<SimulationResult> simulate(int level) {
    if (parent == null) {
      // TODO
      return null;
    }

    dimension = reader.init(target, parent.target);

    int pLevel = level + 1;
    for (SimulationResult result : parent.simulate(pLevel)) {
      LinkedHashMap<IntArrayList, SimulationResult> map = new LinkedHashMap<IntArrayList, SimulationResult>();
      for (double[][] smpls : result.samples) {
        double[] sample = smpls[pLevel];
        IntArrayList condIds = reader.parse(sample);
        SimulationResult sr = map.get(condIds);
        if (sr == null) {
          sr = result.clone();
          map.put(condIds.clone(), sr);
        }
        sr.samples.add(smpls);
      }

      for (Map.Entry<IntArrayList, SimulationResult> entry : map.entrySet()) {
        IntArrayList condIds = entry.getKey();
        SimulationResult sr = entry.getValue();
        // TODO: compute
        boolean[] fake = new boolean[dimension];
        double[] mu = new double[dimension];
        double[][] A = new double[dimension][dimension];
        double[][] B = new double[dimension][sr.dimension];

        int pos = 0;
        int offset = 0;
        for (int i = 0; i < target.length; ++i) {
          int rOff = offset;
          offset = withinLevel1[i].fill(target[i], condIds, pos, fake, mu, A, offset);
          for (int j = i + 1, cOff = rOff, pos2 = pos + 1; j < target.length; ++j, ++pos2) {
            cOff = withinLevel2[i][j].fillSym(target[i], target[j], condIds, pos, pos2, fake, mu,
                A, rOff, cOff);
          }
          pos += target[i].size();
        }

        int lv = 0;
        for (InterDistOracle[][] os : betweenLevel) {
          for (int i = 0; i < target.length; ++i) {
            for (int j = 0; j < os.length; ++j) {
              os[i][j].fillAsym(target[i], sr.groupIds.get(lv), condIds, 0, 0, fake, mu, sr.means.get(lv), A, 0, 0); // TODO
            }
          }
        }

        Array2DRowRealMatrix a = new Array2DRowRealMatrix(A);
        Array2DRowRealMatrix b = new Array2DRowRealMatrix(B);
        Array2DRowRealMatrix c = new Array2DRowRealMatrix(b.transpose().getData());
        Array2DRowRealMatrix sigma = a.subtract(b.multiply(sr.matrixInverse).multiply(c));
        MultivariateNormalDistribution dist = new MultivariateNormalDistribution(mu, sigma.getDataRef());
        double[][] smpls = dist.sample(sr.samples.size());

        for (int i = 0; i < smpls.length; ++i) {
          sr.samples.get(i)[level] = smpls[i];
        }

        sr.matrixInverse = new Array2DRowRealMatrix(new LUDecomposition(sigma).getSolver().getInverse().getData());
      }
    }

    return null;
  }

  public static MCSimNode createSimulationChain(List<List<Integer>> gbyIds,
      List<List<UdafType>> udafTypes,
      List<List<Integer>> gbyIdsInPreds, List<List<Integer>> colsInPreds,
      List<List<PredicateType>> predTypes,
      TupleMap[] srvs, InnerCovMap[] inners, InterCovMap[][] inters,
      boolean simpleQuery) {
    int lastLevel = gbyIds.size();
    MCSimNode parent = null;
    List<List<Integer>> parentGbyIds = new ArrayList<List<Integer>>();
    for (int i = 0; i <= lastLevel; ++i) {
      boolean simpleReturn = (i == lastLevel && predTypes.get(i).size() <= 1);

      MCSimNode node = new MCSimNode(gbyIds.get(i), udafTypes, gbyIdsInPreds.get(i),
          colsInPreds.get(i), predTypes.get(i), parentGbyIds, srvs, inners, inters, simpleQuery
              || simpleReturn);
      node.setParent(parent);

      parent = node;
      parentGbyIds.add(gbyIds.get(i));
    }

    return parent;
  }

}
