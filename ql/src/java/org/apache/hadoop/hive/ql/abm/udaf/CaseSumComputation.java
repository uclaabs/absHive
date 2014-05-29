package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.List;

public class CaseSumComputation extends UDAFComputation {
  
  protected DoubleArrayList result = new DoubleArrayList();
  protected List<DoubleArrayList> doubleMatrix = new ArrayList<DoubleArrayList>();
  protected DoubleArrayList currentList = new DoubleArrayList();
  protected double baseSum = 0;
  protected double baseCnt = 0;
  protected int N = 0;
  protected int groupCnt = -1;
  
  protected double currentSum = 0;
  protected double currentCnt = 0;

  public void setTotalNumber(int N) {
    this.N = N;
  }
  
  public void setBase(double sum, double cnt) {
    this.baseSum = sum;
    this.baseCnt = cnt;
  }
  
  public void setCurrentList(DoubleArrayList list) {
    this.doubleMatrix.add(new DoubleArrayList());
    this.currentList = list;
    this.currentSum = 0;
    this.currentCnt = 0;
    this.groupCnt ++;
  }
  
  @Override
  public void iterate(int index) {
    double value = this.currentList.getDouble(index);
    currentSum += value;
    currentCnt += 1;
  }

  @Override
  public void partialTerminate(int level, int start, int end) { 
  }

  @Override
  public void terminate() {
    doubleMatrix.get(groupCnt).add(currentSum);
    doubleMatrix.get(groupCnt).add(currentCnt);
  }

  @Override
  public void reset() {
    currentSum = 0;
    currentCnt = 0;
  }

  @Override
  public void unfold() {
    
    unfoldSrvList(0, this.baseSum, this.baseCnt);
    this.result.add(this.baseSum/this.baseCnt);
  }
  
  protected void unfoldSrvList(int level, double sum, double cnt) {
    
    boolean leaf = (level == this.groupCnt);
    for(int i = 0; i < this.doubleMatrix.get(level).size() / 2; i ++) {
      
      double tmpSum = sum + this.doubleMatrix.get(level).getDouble(i * 2);
      double tmpCnt = cnt + this.doubleMatrix.get(level).getDouble(i * 2 + 1);
      
      if(leaf) {
        this.result.add(this.baseSum/this.baseCnt);
      } else {
        unfoldSrvList(level + 1, tmpSum, tmpCnt);
      }  
    }
  }

  @Override
  public Object serializeResult() {

    return result;
  }
}
