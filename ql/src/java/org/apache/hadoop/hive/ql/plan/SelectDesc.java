/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;
import org.apache.hadoop.hive.ql.abm.simulation.PredicateType;


/**
 *
 * ABM modified file
 *
 * SelectDesc.
 *
 */
@Explain(displayName = "Select Operator")
public class SelectDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList;
  private List<java.lang.String> outputColumnNames;
  private boolean selectStar;
  private boolean selStarNoCompute;

  // ABM
  private boolean cached = false;
  private TableDesc tableDesc = null;
  private String tableName = null;

  // ABM
  private boolean simulated = false;
  private List<Integer> numKeysContinuous = null;
  private List<List<UdafType>> aggrTypesContinuous = null;
  private List<Integer> numKeysDiscrete = null;
  private List<String> cachedOutputs = null;
  private List<String> cachedInputs = null;

  private boolean simpleQuery = false;

  private int[][] gbyIds;
  private UdafType[][][] udafTypes;
  private int[][][] gbyIdsInPreds;
  private int[][][] colsInPreds;
  private PredicateType[][][] predTypes;

  private int[][] gbyIdsInPorts;
  private int[][] colsInPorts;
  private PredicateType[][] predTypesInPorts;

  private int[] aggIdxs;
  private int numSimulationSample;

  public SelectDesc() {
  }

  public SelectDesc(final boolean selStarNoCompute) {
    this.selStarNoCompute = selStarNoCompute;
  }

  public SelectDesc(
      final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
      final List<java.lang.String> outputColumnNames) {
    this(colList, outputColumnNames, false);
  }

  public SelectDesc(
      final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
      List<java.lang.String> outputColumnNames,
      final boolean selectStar) {
    this.colList = colList;
    this.selectStar = selectStar;
    this.outputColumnNames = outputColumnNames;
  }

  public SelectDesc(
      final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
      final boolean selectStar, final boolean selStarNoCompute) {
    this.colList = colList;
    this.selectStar = selectStar;
    this.selStarNoCompute = selStarNoCompute;
  }

  @Override
  public Object clone() {
    SelectDesc ret = new SelectDesc();
    ret.setColList(getColList() == null ? null : new ArrayList<ExprNodeDesc>(getColList()));
    ret.setOutputColumnNames(getOutputColumnNames() == null ? null :
        new ArrayList<String>(getOutputColumnNames()));
    ret.setSelectStar(selectStar);
    ret.setSelStarNoCompute(selStarNoCompute);
    return ret;
  }

  @Explain(displayName = "expressions")
  public List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> getColList() {
    return colList;
  }

  public void setColList(
      final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList) {
    this.colList = colList;
  }

  @Explain(displayName = "outputColumnNames")
  public List<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      List<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  @Explain(displayName = "SELECT * ")
  public String explainNoCompute() {
    if (isSelStarNoCompute()) {
      return "(no compute)";
    } else {
      return null;
    }
  }

  /**
   * @return the selectStar
   */
  public boolean isSelectStar() {
    return selectStar;
  }

  /**
   * @param selectStar
   *          the selectStar to set
   */
  public void setSelectStar(boolean selectStar) {
    this.selectStar = selectStar;
  }

  /**
   * @return the selStarNoCompute
   */
  public boolean isSelStarNoCompute() {
    return selStarNoCompute;
  }

  /**
   * @param selStarNoCompute
   *          the selStarNoCompute to set
   */
  public void setSelStarNoCompute(boolean selStarNoCompute) {
    this.selStarNoCompute = selStarNoCompute;
  }

  public void cache(String tableName, TableDesc tableDesc) {
    cached = true;
    assert !selStarNoCompute;
    this.tableDesc = tableDesc;
    this.tableName = tableName;
  }

  public boolean isCached() {
    return cached;
  }

  public void setCached(boolean cached) {
    this.cached = cached;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public void setTableDesc(TableDesc tableDesc) {
    this.tableDesc = tableDesc;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public boolean isSimulated() {
    return simulated;
  }

  public void setSimulated(boolean simulated) {
    this.simulated = simulated;
  }

  public List<Integer> getNumKeysContinuous() {
    return numKeysContinuous;
  }

  public void setNumKeysContinuous(List<Integer> numKeysContinuous) {
    this.numKeysContinuous = numKeysContinuous;
  }

  public List<Integer> getNumKeysDiscrete() {
    return numKeysDiscrete;
  }

  public void setNumKeysDiscrete(List<Integer> numKeysDiscrete) {
    this.numKeysDiscrete = numKeysDiscrete;
  }

  public List<List<UdafType>> getAggrTypesContinuous() {
    return aggrTypesContinuous;
  }

  public void setAggrTypesContinuous(List<List<UdafType>> aggrTypesContinuous) {
    this.aggrTypesContinuous = aggrTypesContinuous;
  }

  public List<String> getCachedOutputs() {
    return cachedOutputs;
  }

  public void setCachedOutputs(List<String> cachedOutputs) {
    this.cachedOutputs = cachedOutputs;
  }

  public List<String> getCachedInputs() {
    return cachedInputs;
  }

  public void setCachedInputs(List<String> cachedInputs) {
    this.cachedInputs = cachedInputs;
  }

  public boolean isSimpleQuery() {
    return simpleQuery;
  }

  public void setSimpleQuery(boolean simpleQuery) {
    this.simpleQuery = simpleQuery;
  }

  public int[][] getGbyIds() {
    return gbyIds;
  }

  public void setGbyIds(int[][] gbyIds) {
    this.gbyIds = gbyIds;
  }

  public UdafType[][][] getUdafTypes() {
    return udafTypes;
  }

  public void setUdafTypes(UdafType[][][] udafTypes) {
    this.udafTypes = udafTypes;
  }

  public int[][][] getGbyIdsInPreds() {
    return gbyIdsInPreds;
  }

  public void setGbyIdsInPreds(int[][][] gbyIdsInPreds) {
    this.gbyIdsInPreds = gbyIdsInPreds;
  }

  public int[][][] getColsInPreds() {
    return colsInPreds;
  }

  public void setColsInPreds(int[][][] colsInPreds) {
    this.colsInPreds = colsInPreds;
  }

  public PredicateType[][][] getPredTypes() {
    return predTypes;
  }

  public void setPredTypes(PredicateType[][][] predTypes) {
    this.predTypes = predTypes;
  }

  public int[][] getGbyIdsInPorts() {
    return gbyIdsInPorts;
  }

  public void setGbyIdsInPorts(int[][] gbyIdsInPorts) {
    this.gbyIdsInPorts = gbyIdsInPorts;
  }

  public int[][] getColsInPorts() {
    return colsInPorts;
  }

  public void setColsInPorts(int[][] colsInPorts) {
    this.colsInPorts = colsInPorts;
  }

  public PredicateType[][] getPredTypesInPorts() {
    return predTypesInPorts;
  }

  public void setPredTypesInPorts(PredicateType[][] predTypesInPorts) {
    this.predTypesInPorts = predTypesInPorts;
  }

  public int[] getAggIdxs() {
    return aggIdxs;
  }

  public void setAggIdxs(int[] aggIdxs) {
    this.aggIdxs = aggIdxs;
  }

  public int getNumSimulationSample() {
    return numSimulationSample;
  }

  public void setNumSimulationSample(int numSimulationSample) {
    this.numSimulationSample = numSimulationSample;
  }

  public void setMCSim(List<Integer> numKeysContinuous, List<List<UdafType>> aggrTypesContinuous,
      List<Integer> numKeysDiscrete,
      List<String> cachedOutputs, List<String> cachedInputs, boolean simpleQuery,
      int[][] gbyIds, UdafType[][][] udafTypes,
      int[][][] gbyIdsInPreds, int[][][] colsInPreds, PredicateType[][][] predTypes,
      int[][] gbyIdsInPorts, int[][] colsInPorts, PredicateType[][] predTypesInPorts, int[] aggIdxs, int numSimulationSample) {
    simulated = true;

    this.numKeysContinuous = numKeysContinuous;
    this.aggrTypesContinuous = aggrTypesContinuous;
    this.numKeysDiscrete = numKeysDiscrete;

    this.cachedOutputs = cachedOutputs;
    this.cachedInputs = cachedInputs;

    this.simpleQuery = simpleQuery;

    this.gbyIds = gbyIds;
    this.udafTypes = udafTypes;
    this.gbyIdsInPreds = gbyIdsInPreds;
    this.colsInPreds = colsInPreds;
    this.predTypes = predTypes;

    this.gbyIdsInPorts = gbyIdsInPorts;
    this.colsInPorts = colsInPorts;
    this.predTypesInPorts = predTypesInPorts;

    this.aggIdxs = aggIdxs;
    this.numSimulationSample = numSimulationSample;
  }

}
