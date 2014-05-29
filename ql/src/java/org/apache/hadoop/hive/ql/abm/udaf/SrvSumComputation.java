package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.List;

public class SrvSumComputation extends UDAFComputation {
  
  protected DoubleArrayList result = new DoubleArrayList();
  protected List<DoubleArrayList> doubleMatrix = new ArrayList<DoubleArrayList>();
  protected DoubleArrayList currentList = new DoubleArrayList();
  protected double baseSum = 0;
  protected double baseSsum = 0;
  protected int N = 0;
  protected int groupCnt = -1;
  
  protected double currentSum = 0;
  protected double currentSsum = 0;
  protected double confidenceLower = 0;
  protected double confidenceUpper = 0;

  public void setTotalNumber(int N) {
    this.N = N;
  }
  
  public void setBase(double sum, double ssum) {
    this.baseSum = sum;
    this.baseSsum = ssum;
  }
  
  public void setCurrentList(DoubleArrayList list) {
    this.doubleMatrix.add(new DoubleArrayList());
    this.currentList = list;
    this.currentSum = 0;
    this.currentSsum = 0;
    this.groupCnt ++;
  }
  
  @Override
  public void iterate(int index) {
    double value = this.currentList.getDouble(index);
    currentSum += value;
    currentSsum += (value * value);
  }

  @Override
  public void partialTerminate(int level, int start, int end) { 
  }

  @Override
  public void terminate() {
    doubleMatrix.get(groupCnt).add(currentSum);
    doubleMatrix.get(groupCnt).add(currentSsum);
  }

  @Override
  public void reset() {
    currentSum = 0;
    currentSsum = 0;
  }

  @Override
  public void unfold() {
    
    this.result.add(confidenceLower);
    this.result.add(confidenceUpper);
    
    unfoldSrvList(0, this.baseSum, this.baseSsum);
    
    addDistribution(this.baseSum,this.baseSsum);
    this.result.add(0, this.confidenceLower);
    this.result.add(1, this.confidenceUpper);
    // update the first two values of array
  }
  
  protected void addDistribution(double sum, double ssum) {
    double variance = ssum - sum * sum / N;
    double std = Math.sqrt(variance);
    
    this.result.add(sum);
    this.result.add(variance);
    
    double lower = sum - 3 * std;
    double upper = sum + 3 * std;
    
    if(lower < this.confidenceLower) {
      this.confidenceLower = lower;
    }
    if(upper > this.confidenceUpper) {
      this.confidenceUpper = upper;
    }
  }
  
  protected void unfoldSrvList(int level, double sum, double ssum) {
    
    boolean leaf = (level == this.groupCnt);
    for(int i = 0; i < this.doubleMatrix.get(level).size() / 2; i ++) {
      
      double tmpSum = sum + this.doubleMatrix.get(level).getDouble(i * 2);
      double tmpSsum = ssum + this.doubleMatrix.get(level).getDouble(i * 2 + 1);
      
      if(leaf) {
        addDistribution(tmpSum, tmpSsum);
      } else {
        unfoldSrvList(level + 1, tmpSum, tmpSsum);
      }  
    }
  }
  

  @Override
  public Object serializeResult() {

    return result;
  }
  
  

}
