package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;

public class SimulationResult {

  public ArrayList<IntArrayList> lastCondId = null;
  public final ArrayList<double[]> means = new ArrayList<double[]>();
  public final ArrayList<ArrayList<IntArrayList>> condIds = new ArrayList<ArrayList<IntArrayList>>();
  public ArrayList<double[][]> samples = new ArrayList<double[][]>();
  public Array2DRowRealMatrix sigma;

  @Override
  public SimulationResult clone() {
    SimulationResult ret = new SimulationResult();
    ret.means.addAll(means);
    ret.condIds.addAll(condIds);
    ret.sigma = sigma;
    return ret;
  }

  public double[] getSample(int idx, int len) {
    double[] res = new double[len];
    double[][] smpls = samples.get(idx);
    for(int i = 0, ind = 0; ind < len; ) {
      double[] smpl = smpls[i++];
      System.arraycopy(smpl, 0, res, ind, smpl.length);
      ind += smpl.length;
    }
    return res;
  }

  public double[] getMean(int len) {
    double[] res = new double[len];
    int pos = 0;
    for (double[] mean : means) {
      System.arraycopy(mean, 0, res, pos, mean.length);
      pos += mean.length;
    }
    return res;
  }

}
