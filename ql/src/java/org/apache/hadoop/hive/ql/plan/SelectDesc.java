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

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;
import org.apache.hadoop.hive.serde.serdeConstants;


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
  private boolean sim = false;
  private ArrayList<Integer> cTags = null;
  private ArrayList<Integer> dTags = null;

  private ArrayList<ArrayList<ExprNodeDesc>> inKeys = null;
  private ArrayList<ArrayList<ExprNodeDesc>> inVals = null;
  private ArrayList<ExprNodeDesc> inTids = null;

  private ArrayList<ArrayList<ExprNodeDesc>> outCKeys = null;
  private ArrayList<ArrayList<ExprNodeDesc>> outCAggrs = null;
  private ArrayList<ExprNodeDesc> outCLins = null;
  private ArrayList<ExprNodeDesc> outCConds = null;
  private ArrayList<ExprNodeDesc> outCGbyIds = null;
  private ArrayList<ArrayList<UdafType>> outCTypes = null;

  private ArrayList<ArrayList<ExprNodeDesc>> outDAggrs = null;
  private ArrayList<ExprNodeDesc> outDConds = null;
  private ArrayList<ExprNodeDesc> outDGbyIds = null;

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

  public void cache(String tableName) {
    cached = true;
    assert !selStarNoCompute;
    tableDesc = generateTableDescToCache();
    this.tableName = tableName;
  }

  private TableDesc generateTableDescToCache() {
    String cols = "";
    String colTypes = "";
    boolean first = true;
    for (int i = 0; i < colList.size(); i++) {
      if (!first) {
        cols = cols.concat(",");
        colTypes = colTypes.concat(":");
      }
      first = false;
      cols = cols.concat(outputColumnNames.get(i));
      String tName = colList.get(i).getTypeInfo().getTypeName();
      if (tName.equals(serdeConstants.VOID_TYPE_NAME)) {
        colTypes = colTypes.concat(serdeConstants.STRING_TYPE_NAME);
      } else {
        colTypes = colTypes.concat(tName);
      }
    }
    return PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes,
        AbmUtilities.getQueryResultFileFormat());
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

  public boolean getSim() {
    return sim;
  }

  public void setSim(boolean sim) {
    this.sim = sim;
  }

  public ArrayList<Integer> getCTags() {
    return cTags;
  }

  public void setCTags(ArrayList<Integer> cTags) {
    this.cTags = cTags;
  }

  public ArrayList<Integer> getDTags() {
    return dTags;
  }

  public void setDTags(ArrayList<Integer> dTags) {
    this.dTags = dTags;
  }

  public ArrayList<ArrayList<ExprNodeDesc>> getInKeys() {
    return inKeys;
  }

  public void setInKeys(ArrayList<ArrayList<ExprNodeDesc>> inKeys) {
    this.inKeys = inKeys;
  }

  public ArrayList<ArrayList<ExprNodeDesc>> getInVals() {
    return inVals;
  }

  public void setInVals(ArrayList<ArrayList<ExprNodeDesc>> inVals) {
    this.inVals = inVals;
  }

  public ArrayList<ExprNodeDesc> getInTids() {
    return inTids;
  }

  public void setInTids(ArrayList<ExprNodeDesc> inTids) {
    this.inTids = inTids;
  }

  public ArrayList<ArrayList<ExprNodeDesc>> getOutCKeys() {
    return outCKeys;
  }

  public void setOutCKeys(ArrayList<ArrayList<ExprNodeDesc>> outCKeys) {
    this.outCKeys = outCKeys;
  }

  public ArrayList<ArrayList<ExprNodeDesc>> getOutCAggrs() {
    return outCAggrs;
  }

  public void setOutCAggrs(ArrayList<ArrayList<ExprNodeDesc>> outCAggrs) {
    this.outCAggrs = outCAggrs;
  }

  public ArrayList<ExprNodeDesc> getOutCLins() {
    return outCLins;
  }

  public void setOutCLins(ArrayList<ExprNodeDesc> outCLins) {
    this.outCLins = outCLins;
  }

  public ArrayList<ExprNodeDesc> getOutCConds() {
    return outCConds;
  }

  public void setOutCConds(ArrayList<ExprNodeDesc> outCConds) {
    this.outCConds = outCConds;
  }

  public ArrayList<ExprNodeDesc> getOutCGbyIds() {
    return outCGbyIds;
  }

  public void setOutCGbyIds(ArrayList<ExprNodeDesc> outCGbyIds) {
    this.outCGbyIds = outCGbyIds;
  }

  public ArrayList<ArrayList<UdafType>> getOutCTypes() {
    return outCTypes;
  }

  public void setOutCTypes(ArrayList<ArrayList<UdafType>> outCTypes) {
    this.outCTypes = outCTypes;
  }

  public ArrayList<ArrayList<ExprNodeDesc>> getOutDAggrs() {
    return outDAggrs;
  }

  public void setOutDAggrs(ArrayList<ArrayList<ExprNodeDesc>> outDAggrs) {
    this.outDAggrs = outDAggrs;
  }

  public ArrayList<ExprNodeDesc> getOutDConds() {
    return outDConds;
  }

  public void setOutDConds(ArrayList<ExprNodeDesc> outDConds) {
    this.outDConds = outDConds;
  }

  public ArrayList<ExprNodeDesc> getOutDGbyIds() {
    return outDGbyIds;
  }

  public void setOutDGbyIds(ArrayList<ExprNodeDesc> outDGbyIds) {
    this.outDGbyIds = outDGbyIds;
  }

  public void setMCSim(ArrayList<Integer> cTags, ArrayList<Integer> dTags,
      ArrayList<ArrayList<ExprNodeDesc>> inKeys, ArrayList<ArrayList<ExprNodeDesc>> inVals,
      ArrayList<ExprNodeDesc> inTids, ArrayList<ArrayList<ExprNodeDesc>> outCKeys,
      ArrayList<ArrayList<ExprNodeDesc>> outCAggrs, ArrayList<ExprNodeDesc> outCLins,
      ArrayList<ExprNodeDesc> outCConds, ArrayList<ExprNodeDesc> outCGbyIds,
      ArrayList<ArrayList<UdafType>> outCTypes, ArrayList<ArrayList<ExprNodeDesc>> outDAggrs,
      ArrayList<ExprNodeDesc> outDConds, ArrayList<ExprNodeDesc> outDGbyIds) {
    sim = true;

    this.cTags = cTags;
    this.dTags = dTags;

    this.inKeys = inKeys;
    this.inVals = inVals;
    this.inTids = inTids;

    this.outCKeys = outCKeys;
    this.outCAggrs = outCAggrs;
    this.outCLins = outCLins;
    this.outCConds = outCConds;
    this.outCGbyIds = outCGbyIds;
    this.outCTypes = outCTypes;

    this.outDAggrs = outDAggrs;
    this.outDConds = outDConds;
    this.outDGbyIds = outDGbyIds;
  }

}
