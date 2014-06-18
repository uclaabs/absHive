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

  private static int NUM_SIMULATIONS = 0;
  private static int NUM_LEVEL = 0;

  private final int[] gbys;
  private final IntArrayList[] targets;
  private final OffsetInfo[] offInfos;
  private final int[] numAggrs;
  private int dimension = 0;

  private final InnerDistOracle[] within1;
  private final InterDistOracle[][] within2;
  private final List<InterDistOracle[][]> between;

  private final KeyReader reader;

  private MCSimNode parent = null;

  public MCSimNode(int[] gbys, UdafType[][] udafTypes,
      int[][] gbyIdsInPreds, int[][] colsInPreds, PredicateType[][] predTypes,
      List<MCSimNode> parents,
      TupleMap[] srvs, InnerCovMap[] inners, InterCovMap[][] inters,
      boolean independent) {
    int len1 = gbys.length;

    this.gbys = gbys;
    numAggrs = new int[len1];

    targets = new IntArrayList[len1];
    offInfos = new OffsetInfo[len1];
    for (int i = 0; i < len1; ++i) {
      numAggrs[i] = udafTypes[i].length;

      targets[i] = new IntArrayList();
      offInfos[i] = new OffsetInfo();
    }

    // Initialize distribution oracles
    int lastContinuousGby = inners.length - 1;
    within1 = new InnerDistOracle[len1];
    within2 = new InterDistOracle[len1][len1];
    for (int i = 0; i < len1; ++i) {
      int gby1 = gbys[i];
      UdafType[] udaf1 = udafTypes[i];
      boolean continuous1 = (gby1 <= lastContinuousGby);

      if (independent || !continuous1) {
        within1[i] = new IndependentInnerDistOracle(srvs[gby1], continuous1, targets[i], udaf1,
            offInfos[i]);
      } else {
        within1[i] = new CorrelatedInnerDistOracle(srvs[gby1], continuous1, targets[i],
            inners[gby1], udaf1, offInfos[i]);
      }

      InterDistOracle[] level = new InterDistOracle[len1];
      within2[i] = level;

      for (int j = i + 1; j < len1; ++j) {
        int gby2 = gbys[j];
        UdafType[] udaf2 = udafTypes[j];
        boolean continuous2 = (gby2 <= lastContinuousGby);
        if (independent || !continuous1 || !continuous2) {
          level[i] = new IndependentInterDistOracle(targets[i], targets[j], udaf1, udaf2,
              offInfos[i], offInfos[j]);
        } else {
          level[i] = new CorrelatedInterDistOracle(targets[i], targets[j], inters[i][j], udaf1,
              udaf2, offInfos[i], offInfos[j]);
        }
      }
    }

    between = new ArrayList<InterDistOracle[][]>(parents.size());
    for (MCSimNode parent : parents) {
      int len2 = parent.targets.length;
      InterDistOracle[][] cur = new InterDistOracle[len1][len2];
      between.add(cur);
      for (int i = 0; i < len1; ++i) {
        InterDistOracle[] level = new InterDistOracle[len2];
        cur[i] = level;

        int gby1 = gbys[i];
        UdafType[] udaf1 = udafTypes[i];
        boolean continuous1 = (gby1 <= lastContinuousGby);

        for (int j = 0; j < len2; ++j) {
          int gby2 = parent.gbys[j];
          UdafType[] udaf2 = udafTypes[j];
          boolean continuous2 = (gby2 <= lastContinuousGby);
          if (independent || !continuous1 || !continuous2) {
            level[i] = new IndependentInterDistOracle(targets[i], parent.targets[j], udaf1, udaf2,
                offInfos[i], parent.offInfos[j]);
          } else {
            level[i] = new CorrelatedInterDistOracle(targets[i], parent.targets[j], inters[i][j],
                udaf1, udaf2, offInfos[i], parent.offInfos[j]);
          }
        }
      }
    }

    // Initialize condition reader
    reader = new KeyReader(gbys, numAggrs, gbyIdsInPreds, colsInPreds, predTypes, srvs);
  }

  public void setParent(MCSimNode parent) {
    this.parent = parent;
  }

  public List<SimulationResult> simulate(int level) {
    initOffsetInfos();

    if (parent == null) {
      boolean[] fake = new boolean[dimension];
      double[] mu = new double[dimension];
      double[][] sigma = new double[dimension][dimension];

      IntArrayList condIds = new IntArrayList();
      condIds.add(0);

      for (int i = 0; i < within1.length; ++i) {
        within1[i].fill(condIds, fake, mu, sigma);
        InterDistOracle[] w2s = within2[i];
        for (int j = i + 1; j < within2.length; ++j) {
          w2s[j].fillSym(condIds, fake, mu, sigma);
        }
      }

      MultivariateNormalDistribution dist = new MultivariateNormalDistribution(mu, sigma);
      double[][] smpls = dist.sample(NUM_SIMULATIONS);

      SimulationResult ret = new SimulationResult();
      for (int i = 0; i < smpls.length; ++i) {
        double[][] samples = new double[NUM_LEVEL][];
        samples[0] = smpls[i];
        ret.samples.add(samples);
      }

      ArrayList<SimulationResult> retSet = new ArrayList<SimulationResult>();
      retSet.add(ret);
      return retSet;
    }

    List<SimulationResult> ret = new ArrayList<SimulationResult>();

    int pLevel = level - 1;
    reader.init(targets, parent.targets);
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
        MultivariateNormalDistribution dist = new MultivariateNormalDistribution(mu,
            sigma.getDataRef());
        double[][] smpls = dist.sample(sr.samples.size());
        for (int i = 0; i < smpls.length; ++i) {
          // TODO
          // sr.samples.get(i)[level] = new ArrayRealVector(smpls[i]);
        }

        sr.ivSigma = new Array2DRowRealMatrix(new LUDecomposition(sigma).getSolver().getInverse()
            .getData());

        ret.add(sr);
      }
    }

    return ret;
  }

  public static MCSimNode createSimulationChain(int[][] gbyIds, UdafType[][][] udafTypes,
      int[][][] gbyIdsInPreds, int[][][] colsInPreds, PredicateType[][][] predTypes,
      TupleMap[] srvs, InnerCovMap[] inners, InterCovMap[][] inters,
      boolean simpleQuery) {
    int last = gbyIds.length - 1;
    MCSimNode parent = null;
    List<MCSimNode> parents = new ArrayList<MCSimNode>();
    for (int i = 0; i <= last; ++i) {
      boolean simpleReturn = (i == last && predTypes[i].length <= 1);
      MCSimNode node = new MCSimNode(gbyIds[i], udafTypes[i], gbyIdsInPreds[i],
          colsInPreds[i], predTypes[i], parents, srvs, inners, inters, simpleQuery
              || simpleReturn);
      node.setParent(parent);

      parent = node;
      parents.add(node);
    }

    return parent;
  }

  private void initOffsetInfos() {
    int cumPos = 0;
    int cumOff = 0;
    for (int i = 0; i < numAggrs.length; ++i) {
      offInfos[i].pos = cumPos;
      offInfos[i].offset = cumOff;
      cumPos += targets[i].size();
      cumOff += targets[i].size() * numAggrs[i];
    }
    dimension = cumOff;
  }

}

class OffsetInfo {

  public int pos;
  public int offset;

}
