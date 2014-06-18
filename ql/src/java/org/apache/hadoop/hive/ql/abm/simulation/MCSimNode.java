package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InnerCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InterCovMap;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class MCSimNode {

  private final int[] gbys;
  private final IntArrayList[] target;
  private final OffsetInfo[] offInfos;
  private final int[] numAggrs;
  private int dimension = 0;

  private final InnerDistOracle[] within1;
  private final InterDistOracle[][] within2;
  private final List<InterDistOracle[][]> between;

  private final KeyReader reader;

  private MCSimNode parent = null;

  public MCSimNode(List<Integer> gbyIds, List<List<UdafType>> udafTypes,
      List<Integer> gbyIdsInPreds, List<Integer> colsInPreds, List<PredicateType> predTypes,
      List<MCSimNode> parents,
      TupleMap[] srvs, InnerCovMap[] inners, InterCovMap[][] inters,
      boolean independent) {
    int numGbys1 = gbyIds.size();

    gbys = new int[numGbys1];
    target = new IntArrayList[numGbys1];
    offInfos = new OffsetInfo[numGbys1];
    numAggrs = new int[numGbys1];
    for (int i = 0; i < numGbys1; ++i) {
      gbys[i] = gbyIds.get(i);
      numAggrs[i] = udafTypes.get(gbyIds.get(i)).size();
      offInfos[i] = new OffsetInfo();
    }

    // Initialize distribution oracles
    int lastContinuousGby = inners.length - 1;
    within1 = new InnerDistOracle[numGbys1];
    within2 = new InterDistOracle[numGbys1][numGbys1];
    for (int i = 0; i < numGbys1; ++i) {
      int gby1 = gbys[i];
      List<UdafType> udaf1 = udafTypes.get(gby1);
      boolean continuous1 = (gby1 <= lastContinuousGby);

      if (independent || !continuous1) {
        within1[i] = new IndependentInnerDistOracle(srvs[gby1], continuous1, target[i], udaf1, offInfos[i]);
      } else {
        within1[i] = new CorrelatedInnerDistOracle(srvs[gby1], continuous1, target[i], inners[gby1], udaf1, offInfos[i]);
      }

      InterDistOracle[] level = new InterDistOracle[numGbys1];
      within2[i] = level;

      for (int j = i + 1; j < numGbys1; ++j) {
        int gby2 = gbys[j];
        List<UdafType> udaf2 = udafTypes.get(gby2);
        boolean continuous2 = (gby2 <= lastContinuousGby);
        if (independent || !continuous1 || !continuous2) {
          level[i] = new IndependentInterDistOracle(target[i], target[j], udaf1, udaf2, offInfos[i], offInfos[j]);
        } else {
          level[i] = new CorrelatedInterDistOracle(target[i], target[j], inters[i][j], udaf1, udaf2, offInfos[i], offInfos[j]);
        }
      }
    }

    between = new ArrayList<InterDistOracle[][]>(parents.size());
    for (MCSimNode parent : parents) {
      int numGbys2 = parent.target.length;
      InterDistOracle[][] cur = new InterDistOracle[numGbys1][numGbys2];
      between.add(cur);
      for (int i = 0; i < numGbys1; ++i) {
        InterDistOracle[] level = new InterDistOracle[numGbys2];
        cur[i] = level;

        int gby1 = gbys[i];
        List<UdafType> udaf1 = udafTypes.get(gby1);
        boolean continuous1 = (gby1 <= lastContinuousGby);

        for (int j = 0; j < numGbys2; ++j) {
          int gby2 = parent.gbys[j];
          List<UdafType> udaf2 = udafTypes.get(gby2);
          boolean continuous2 = (gby2 <= lastContinuousGby);
          if (independent || !continuous1 || !continuous2) {
            level[i] = new IndependentInterDistOracle(target[i], parent.target[j], udaf1, udaf2, offInfos[i], parent.offInfos[j]);
          } else {
            level[i] = new CorrelatedInterDistOracle(target[i], parent.target[j], inters[i][j], udaf1, udaf2, offInfos[i], parent.offInfos[j]);
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

    dimension = reader.init(target, offInfos, parent.target);

    int pLevel = level - 1;
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

        boolean[] fake = new boolean[dimension];
        double[] mu = new double[dimension];
        double[][] A = new double[dimension][dimension];
        double[][] B = new double[dimension][sr.dimension];

        for (int i = 0; i < within1.length; ++i) {
          within1[i].fill(condIds, fake, mu, A);
          InterDistOracle[] w2s = within2[i];
          for (int j = i + 1; j < within2.length; ++j) {
            w2s[j].fillSym(condIds, fake, mu, A);
          }
        }

        for (int k = 0; k < between.size(); ++k) {
          InterDistOracle[][] oss = between.get(k);
          IntArrayList condIds2 = sr.condIds.get(k);
          double[] mu2 = sr.means.get(k);
          for (int i = 0; i < oss.length; ++i) {
            InterDistOracle[] os = oss[i];
            for (int j = 0; j < os.length; ++j) {
              os[j].fillAsym(condIds, condIds2, fake, mu, mu2, A);
            }
          }
        }

        ArrayRealVector x = new ArrayRealVector(mu);
        Array2DRowRealMatrix a = new Array2DRowRealMatrix(A);
        Array2DRowRealMatrix b = new Array2DRowRealMatrix(B);
        Array2DRowRealMatrix c = (Array2DRowRealMatrix) b.transpose();
        Array2DRowRealMatrix tmp = b.multiply(sr.ivSigma);

        Array2DRowRealMatrix sigma = a.subtract(tmp.multiply(c));
        MultivariateNormalDistribution dist = new MultivariateNormalDistribution(mu, sigma.getDataRef());
        double[][] smpls = dist.sample(sr.samples.size());
        for (int i = 0; i < smpls.length; ++i) {
          // TODO
//          sr.samples.get(i)[level] = new ArrayRealVector(smpls[i]);
        }

        sr.ivSigma = new Array2DRowRealMatrix(new LUDecomposition(sigma).getSolver().getInverse().getData());
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
    List<MCSimNode> parents = new ArrayList<MCSimNode>();
    for (int i = 0; i <= lastLevel; ++i) {
      boolean simpleReturn = (i == lastLevel && predTypes.get(i).size() <= 1);
      MCSimNode node = new MCSimNode(gbyIds.get(i), udafTypes, gbyIdsInPreds.get(i),
          colsInPreds.get(i), predTypes.get(i), parents, srvs, inners, inters, simpleQuery
              || simpleReturn);
      node.setParent(parent);

      parent = node;
      parents.add(node);
    }

    return parent;
  }

}

class OffsetInfo {

  public int pos;
  public int offset;

}
