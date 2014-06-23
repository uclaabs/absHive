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
  private final ArrayList<IntArrayList> zeroCondIds;

  private int dimension = 0;

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
    zeroCondIds = new ArrayList<IntArrayList>();
    for (int i = 0; i < len1; ++i) {
      numAggrs[i] = udafTypes[i].length;
      targets[i] = new IntArrayList();
      offsets[i] = new Offset();
      zeroCondIds.add(new IntArrayList());
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
          lev[j] = new IndependentInterDistOracle(targets[i], targets[j], udaf1, udaf2,
              offsets[i], offsets[j]);
        } else {
          lev[j] = new CorrelatedInterDistOracle(targets[i], targets[j], inters[gby1][gby2], udaf1,
              udaf2, offsets[i], offsets[j]);
        }
      }
    }

    between = new ArrayList<InterDistOracle[][]>(parents.size());
    for (MCSimNode parent : parents) {
      int len2 = parent.targets.length;
      InterDistOracle[][] cur = new InterDistOracle[len2][len1];
      between.add(cur);
      for (int i = 0; i < len2; ++i) {
        InterDistOracle[] lev = new InterDistOracle[len1];
        cur[i] = lev;

        int gby1 = parent.gbys[i];
        UdafType[] udaf1 = udafTypes[i];
        boolean continuous2 = (gby1 <= lastContinuousGby);

        for (int j = 0; j < len1; ++j) {
          int gby2 = gbys[j];
          UdafType[] udaf2 = udafTypes[j];
          boolean continuous1 = (gby2 <= lastContinuousGby);
          if (independent || !continuous1 || !continuous2) {
            lev[j] = new IndependentInterDistOracle(parent.targets[i], targets[j], udaf1, udaf2,
                parent.offsets[i], offsets[j]);
          } else {
            lev[j] = new CorrelatedInterDistOracle(parent.targets[i], targets[j],
                inters[gby1][gby2],
                udaf1, udaf2, parent.offsets[i], offsets[j]);
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
    init(requests);

    if (dimension == 0) {
      return null;
    }

    if (parent == null) {
      return defaultSimulate();
    }

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
      double[][] B = new double[res.sigma.getColumnDimension()][dimension];
      double[] zero = new double[dimension];

      for (int i = 0; i < within1.length; ++i) {
        IntArrayList cIds = condIds.get(i);
        within1[i].fill(cIds, fake, mu, A);
        InterDistOracle[] w2s = within2[i];
        for (int j = i + 1; j < within2.length; ++j) {
          w2s[j].fillSym(cIds, condIds.get(j), fake, mu, A);
        }
      }

      int dif = between.size() - res.condIds.size();
      double[] pmu = res.getMean();
      for (int i = 0; i < targets.length; ++i) {
        int cum = 0;
        IntArrayList cIds = condIds.get(i);
        for (int k = 0; k < res.condIds.size(); ++k) {
          ArrayList<IntArrayList> condIds2 = res.condIds.get(k);
          InterDistOracle[] os = between.get(k + dif)[i];
          for (int j = 0; j < os.length; ++j) {
            cum += os[j].fillAsym(cIds, condIds2.get(j), fake, mu, pmu, B, cum);
          }
        }
      }

      for (int k = 0, cum = 0; k < res.condIds.size(); ++k) {
        ArrayList<IntArrayList> pCIds = res.condIds.get(k);
        InterDistOracle[][] oss = between.get(k + dif);
        for (int j = 0; j < oss.length; ++j) {
          IntArrayList cIds = pCIds.get(j);
          InterDistOracle[] os = oss[j];
          int off = 0;
          for (int i = 0; i < os.length; ++i) {
            off = os[i].fillAsym(cIds, condIds.get(i), fake, pmu, mu, B, cum);
          }
          cum += off;
        }
      }

      Array2DRowRealMatrix a = new Array2DRowRealMatrix(A);
      Array2DRowRealMatrix b = new Array2DRowRealMatrix(B);
      Array2DRowRealMatrix c = (Array2DRowRealMatrix) b.transpose();
      Array2DRowRealMatrix id = new Array2DRowRealMatrix(new LUDecomposition(res.sigma)
          .getSolver().getInverse().getData());
      Array2DRowRealMatrix tmp = c.multiply(id);

      Array2DRowRealMatrix sigma = a.subtract(tmp.multiply(b));
      double[] scale = correct(sigma.getDataRef());

      MultivariateNormalDistribution dist = new MultivariateNormalDistribution(zero,
          sigma.getDataRef());

      double[][] smpls = dist.sample(res.samples.size());

      int pos = 0;
      for (double[] smpl : smpls) {
        if (scale != null) {
          restore(smpl, scale);
        }
        fixFake(fake, mu, smpl);
        double[] s = res.getSample(pos);
        subtract(s, pmu);
        add(smpl, tmp.operate(s));
        res.samples.get(pos)[level] = smpl;
        ++pos;
      }
      res.means.add(mu);
      res.sigma = concat(a, c, b, res.sigma);

      dispatch(res, ret);
    }

    return ret;
  }

  private List<SimulationResult> defaultSimulate() {
    List<SimulationResult> ret = new ArrayList<SimulationResult>();

    boolean[] fake = new boolean[dimension];
    double[] mu = new double[dimension];
    double[][] A = new double[dimension][dimension];
    double[] zero = new double[dimension];

    ArrayList<IntArrayList> condIds = zeroCondIds;

    for (int i = 0; i < within1.length; ++i) {
      IntArrayList cIds = condIds.get(i);
      within1[i].fill(cIds, fake, mu, A);
      InterDistOracle[] w2s = within2[i];
      for (int j = i + 1; j < within2.length; ++j) {
        w2s[j].fillSym(cIds, condIds.get(j), fake, mu, A);
      }
    }

    double[] scale = correct(A);

    MultivariateNormalDistribution dist = new MultivariateNormalDistribution(zero, A);
    double[][] smpls = dist.sample(NUM_SIMULATIONS);

    SimulationResult res = new SimulationResult();
    for (double[] smpl : smpls) {
      if (scale != null) {
        restore(smpl, scale);
      }
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

  private void init(IntArrayList[] requests) {
    dimension = port.init(requests);
    for (int i = 0; i < targets.length; ++i) {
      IntArrayList zci = zeroCondIds.get(i);
      IntArrayList ts = targets[i];
      if (zci.size() > ts.size()) {
        zci.removeElements(ts.size(), zci.size());
      }
      for (int j = zci.size(); j < ts.size(); ++j) {
        zci.add(0);
      }
    }
  }

  private double[] correct(double[][] A) {
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;
    for (int i = 0; i < A.length; ++i) {
      double v = A[i][i];
      if (v > max) {
        max = v;
      }
      if (v < min) {
        min = v;
      }
    }

    if (max > min * 1000000) {
      double[] ret = new double[A.length];
      for (int i = 0; i < A.length; ++i) {
        ret[i] = Math.sqrt(max / A[i][i]);
      }
      scale(A, ret);
      return ret;
    }

    return null;
  }

  private void scale(double[][] A, double[] scale) {
    for (int i = 0; i < A.length; ++i) {
      double[] sub = A[i];
      double s1 = scale[i];
      sub[i] *= (s1 * s1);
      for (int j = i + 1; j < A.length; ++j) {
        sub[j] *= (s1 * scale[j]);
        A[j][i] = sub[j];
      }
    }
  }

  private void restore(double[] sample, double[] scale) {
    for (int i = 0; i < sample.length; ++i) {
      sample[i] /= scale[i];
    }
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
        if (res.lastCondId != null) {
          sr.condIds.add(res.lastCondId);
        }
        sr.lastCondId = cloneKey(cId);
        map.put(sr.lastCondId, sr);
      }
      sr.samples.add(sample);
    }
    ret.addAll(map.values());
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
        sample[i] = 0;
      }
      sample[i] += mu[i];
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
