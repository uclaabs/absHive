package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;
import org.apache.hadoop.hive.ql.abm.simulation.PartialCovMap.InnerCovMap;
import org.apache.hadoop.hive.ql.abm.simulation.PartialCovMap.InterCovMap;

public class MCSimNode {

  private static int NUM_SIMULATIONS = 0;
  private static int NUM_LEVEL = 0;

  private final int level;

  private final int[] gbys;
  private final IntArrayList[] targets;
  private final Offset[] offsets;
  private final int[] numAggrs;

  private int dimension = 0;
  private int pdimension = 0;

  private final InnerDistOracle[] within1;
  private final InterDistOracle[][] within2;
  private final List<InterDistOracle[][]> between;

  private final Port port;

  private final LinkedHashMap<ArrayList<IntArrayList>, SimulationResult> map =
      new LinkedHashMap<ArrayList<IntArrayList>, SimulationResult>();

  private MCSimNode parent = null;

  public MCSimNode(int level, int[] reqGbys, TupleMap[] reqSrvs, PredicateSet[] predSets,
      int[] gbys, UdafType[][] udafTypes, List<MCSimNode> parents,
      TupleMap[] srvs, InnerCovMap[] inners, InterCovMap[][] inters,
      boolean independent) {
    this.level = level;

    int len1 = gbys.length;

    this.gbys = gbys;
    numAggrs = new int[len1];
    targets = new IntArrayList[len1];
    offsets = new Offset[len1];
    for (int i = 0; i < len1; ++i) {
      numAggrs[i] = udafTypes[i].length;
      targets[i] = new IntArrayList();
      offsets[i] = new Offset();
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
            offsets[i]);
      } else {
        within1[i] = new CorrelatedInnerDistOracle(srvs[gby1], continuous1, targets[i],
            inners[gby1], udaf1, offsets[i]);
      }

      InterDistOracle[] lev = new InterDistOracle[len1];
      within2[i] = lev;

      for (int j = i + 1; j < len1; ++j) {
        int gby2 = gbys[j];
        UdafType[] udaf2 = udafTypes[j];
        boolean continuous2 = (gby2 <= lastContinuousGby);
        if (independent || !continuous1 || !continuous2) {
          lev[i] = new IndependentInterDistOracle(targets[i], targets[j], udaf1, udaf2,
              offsets[i], offsets[j]);
        } else {
          lev[i] = new CorrelatedInterDistOracle(targets[i], targets[j], inters[i][j], udaf1,
              udaf2, offsets[i], offsets[j]);
        }
      }
    }

    between = new ArrayList<InterDistOracle[][]>(parents.size());
    for (MCSimNode parent : parents) {
      int len2 = parent.targets.length;
      InterDistOracle[][] cur = new InterDistOracle[len1][len2];
      between.add(cur);
      for (int i = 0; i < len1; ++i) {
        InterDistOracle[] lev = new InterDistOracle[len2];
        cur[i] = lev;

        int gby1 = gbys[i];
        UdafType[] udaf1 = udafTypes[i];
        boolean continuous1 = (gby1 <= lastContinuousGby);

        for (int j = 0; j < len2; ++j) {
          int gby2 = parent.gbys[j];
          UdafType[] udaf2 = udafTypes[j];
          boolean continuous2 = (gby2 <= lastContinuousGby);
          if (independent || !continuous1 || !continuous2) {
            lev[i] = new IndependentInterDistOracle(targets[i], parent.targets[j], udaf1, udaf2,
                offsets[i], parent.offsets[j]);
          } else {
            lev[i] = new CorrelatedInterDistOracle(targets[i], parent.targets[j], inters[i][j],
                udaf1, udaf2, offsets[i], parent.offsets[j]);
          }
        }
      }
    }

    // Initialize condition reader
    port = new Port(reqGbys, reqSrvs, predSets, targets, numAggrs, offsets);
  }

  public void setParent(MCSimNode parent) {
    this.parent = parent;
  }

  public List<SimulationResult> simulate(IntArrayList[] requests) {
    dimension = port.init(requests);

    if (dimension == 0) {
      return null;
    }

    if (parent == null) {
      return defaultSimulate();
    }

    pdimension = parent.pdimension + parent.dimension;

    List<SimulationResult> parRet = parent.simulate(targets);

    if (parRet == null) {
      return defaultSimulate();
    }

    List<SimulationResult> ret = new ArrayList<SimulationResult>();

    for (SimulationResult res : parRet) {
      ArrayList<IntArrayList> condIds = res.lastCondId;

      boolean[] fake = new boolean[dimension];
      double[] mu = new double[dimension];
      double[][] A = new double[dimension][dimension];
      double[][] B = new double[dimension][pdimension];

      for (int i = 0; i < within1.length; ++i) {
        IntArrayList cIds = condIds.get(i);
        within1[i].fill(cIds, fake, mu, A);
        InterDistOracle[] w2s = within2[i];
        for (int j = i + 1; j < within2.length; ++j) {
          w2s[j].fillSym(cIds, condIds.get(j), fake, mu, A);
        }
      }

      for (int k = 0; k < between.size(); ++k) {
        InterDistOracle[][] oss = between.get(k);
        ArrayList<IntArrayList> condIds2 = res.condIds.get(k);
        double[] mu2 = res.means.get(k);
        for (int i = 0; i < oss.length; ++i) {
          IntArrayList cIds = condIds.get(i);
          InterDistOracle[] os = oss[i];
          for (int j = 0, cum = 0; j < os.length; ++j) {
            cum += os[j].fillAsym(cIds, condIds2.get(j), fake, mu, mu2, A, cum);
          }
        }
      }

      Array2DRowRealMatrix a = new Array2DRowRealMatrix(A);
      Array2DRowRealMatrix b = new Array2DRowRealMatrix(B);
      Array2DRowRealMatrix c = (Array2DRowRealMatrix) b.transpose();
      Array2DRowRealMatrix id = new Array2DRowRealMatrix(new LUDecomposition(res.sigma)
          .getSolver().getInverse().getData());
      Array2DRowRealMatrix tmp = b.multiply(id);

      Array2DRowRealMatrix sigma = a.subtract(tmp.multiply(c));
      MultivariateNormalDistribution dist = new MultivariateNormalDistribution(mu,
          sigma.getDataRef());

      double[][] smpls = dist.sample(res.samples.size());


      double[] pmu = res.getMean(pdimension);
      int pos = 0;
      for (double[] smpl : smpls) {
        fixFake(fake, mu, smpl);
        double[] s = res.getSample(pos, pdimension);
        subtract(s, pmu);
        add(smpl, tmp.operate(s));
        res.samples.get(pos)[level] = smpl;
        ++pos;
      }
      res.means.add(mu);
      res.sigma = concat(a, b, c, res.sigma);

      dispatch(res, ret);
    }

    return ret;
  }

  private List<SimulationResult> defaultSimulate() {
    List<SimulationResult> ret = new ArrayList<SimulationResult>();

    boolean[] fake = new boolean[dimension];
    double[] mu = new double[dimension];
    double[][] A = new double[dimension][dimension];

    ArrayList<IntArrayList> condIds = zero();

    for (int i = 0; i < within1.length; ++i) {
      IntArrayList cIds = condIds.get(i);
      within1[i].fill(cIds, fake, mu, A);
      InterDistOracle[] w2s = within2[i];
      for (int j = i + 1; j < within2.length; ++j) {
        w2s[j].fillSym(cIds, condIds.get(j), fake, mu, A);
      }
    }

    MultivariateNormalDistribution dist = new MultivariateNormalDistribution(mu, A);
    double[][] smpls = dist.sample(NUM_SIMULATIONS);

    SimulationResult res = new SimulationResult();
    for (double[] smpl : smpls) {
      fixFake(fake, mu, smpl);
      double[][] samples = new double[NUM_LEVEL][];
      samples[level] = smpl;
      res.samples.add(samples);
    }
    res.means.add(mu);
    res.sigma = new Array2DRowRealMatrix(A);

    dispatch(res, ret);
    return ret;
  }

  private ArrayList<IntArrayList> zero() {
    ArrayList<IntArrayList> ret = new ArrayList<IntArrayList>();
    for (int i = 0; i < targets.length; ++i) {
      IntArrayList list = new IntArrayList();
      IntArrayList ts = targets[i];
      for (int j = 0; j < ts.size(); ++j) {
        list.add(0);
      }
      ret.add(list);
    }
    return ret;
  }

  private void dispatch(SimulationResult res, List<SimulationResult> ret) {
    map.clear();
    boolean last = (level == NUM_LEVEL - 1);
    for (double[][] sample : res.samples) {
      ArrayList<IntArrayList> cId = port.parse(sample[level]);

      if (last && cId.get(0).getInt(0) != 0) {
        continue;
      }

      SimulationResult sr = map.get(cId);
      if (sr == null) {
        sr = res.clone();
        sr.lastCondId = cloneKey(cId);
        map.put(sr.lastCondId, sr);
      }
      sr.samples.add(sample);
    }
  }

  private static ArrayList<IntArrayList> cloneKey(ArrayList<IntArrayList> key) {
    ArrayList<IntArrayList> ret = new ArrayList<IntArrayList>();
    for (IntArrayList k : key) {
      ret.add(k.clone());
    }
    return ret;
  }

  private void fixFake(boolean[] fake, double[] mu, double[] sample) {
    for (int i = 0; i < fake.length; ++i) {
      if (fake[i]) {
        sample[i] = mu[i];
      }
    }
  }

  private void add(double[] x, double[] y) {
    for (int i = 0; i < x.length; ++i) {
      x[i] += y[i];
    }
  }

  private void subtract(double[] x, double[] y) {
    for (int i = 0; i < x.length; ++i) {
      x[i] += y[i];
    }
  }

  private Array2DRowRealMatrix concat(Array2DRowRealMatrix A, Array2DRowRealMatrix B,
      Array2DRowRealMatrix C, Array2DRowRealMatrix D) {
    int dim = A.getRowDimension() + D.getRowDimension();
    Array2DRowRealMatrix ret = new Array2DRowRealMatrix(dim, dim);

    ret.setSubMatrix(A.getData(), 0, 0);
    ret.setSubMatrix(B.getData(), 0, A.getColumnDimension());
    ret.setSubMatrix(C.getData(), A.getRowDimension(), 0);
    ret.setSubMatrix(D.getData(), A.getRowDimension(), A.getColumnDimension());

    return ret;
  }

  public static void setNumSimulations(int numSimulations) {
    NUM_SIMULATIONS = numSimulations;
  }

  public static MCSimNode createSimulationChain(SrvTuple driver,
      int[][] gbys, UdafType[][][] udafTypes,
      PredicateSet[][] allPreds, TupleMap[] srvs, InnerCovMap[] inners, InterCovMap[][] inters,
      boolean simpleQuery) {
    MCSimNode.NUM_LEVEL = gbys.length;

    int last = gbys.length - 1;
    MCSimNode parent = null;
    List<MCSimNode> parents = new ArrayList<MCSimNode>();
    for (int i = 0; i < last; ++i) {
      MCSimNode node = new MCSimNode(i, gbys[i + 1], srvs, allPreds[i], gbys[i], udafTypes[i],
          parents, srvs, inners, inters, simpleQuery);
      node.setParent(parent);

      parent = node;
      parents.add(node);
    }

    DriverTupleMap driverMap = new DriverTupleMap();
    driverMap.setDefaultTuple(driver);
    MCSimNode node = new MCSimNode(last, new int[1], new TupleMap[] {driverMap}, allPreds[last],
        gbys[last], udafTypes[last], parents, srvs, inners, inters,
        simpleQuery || allPreds[last][0].isSimpleCondition());
    node.setParent(parent);

    return node;
  }

}

class Offset {

  public int pos;
  public int offset;

}
