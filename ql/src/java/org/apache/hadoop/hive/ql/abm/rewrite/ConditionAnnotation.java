package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.algebra.Transform;
import org.apache.hadoop.hive.ql.abm.lib.TopologicalSort;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;

public class ConditionAnnotation {

  private final HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates =
      new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
  private final HashMap<GroupByOperator, Transform[]> dependencies =
      new HashMap<GroupByOperator, Transform[]>();
  private final ArrayList<Transform> transforms = new ArrayList<Transform>();

  private final HashMap<GroupByOperator, SelectOperator> inputs =
      new HashMap<GroupByOperator, SelectOperator>();
  private final HashMap<GroupByOperator, SelectOperator> outputs =
      new HashMap<GroupByOperator, SelectOperator>();

  // <-- Used by TraceProcCtx

  public void groupByAt(GroupByOperator gby) {
    dependencies.put(gby, transforms.toArray(new Transform[transforms.size()]));
    transforms.clear();
  }

  public void conditionOn(Transform trans) {
    transforms.add(trans);
    for (AggregateInfo ai : trans.getAggregatesInvolved()) {
      GroupByOperator gby = ai.getGroupByOperator();
      TreeSet<AggregateInfo> buf = aggregates.get(gby);
      if (buf == null) {
        buf = new TreeSet<AggregateInfo>();
        aggregates.put(gby, buf);
      }
      buf.add(ai);
    }
  }

  public void combine(ConditionAnnotation other) {
    aggregates.putAll(other.aggregates);
    transforms.addAll(other.transforms);
    dependencies.putAll(other.dependencies);
  }

  // -->

  // <-- Used by RewriteProcCtx

  public void putGroupByInput(GroupByOperator gby, SelectOperator input) {
    input.getConf().cache(AbmUtilities.ABM_CACHE_INPUT_PREFIX + gby.toString());
    inputs.put(gby, input);
  }

  public void putGroupByOutput(GroupByOperator gby, SelectOperator output) {
    output.getConf().cache(AbmUtilities.ABM_CACHE_OUTPUT_PREFIX + gby.toString());
    outputs.put(gby, output);
  }

  public void f() {
    Map<GroupByOperator, Set<GroupByOperator>> map = getDependencyGraph();
    List<List<GroupByOperator>> sorted = TopologicalSort.getOrderByLevel(map);
    int numGbys = dependencies.size();

    // Assign ids to GroupByOperators
    HashMap<GroupByOperator, Integer> gby2Id = new HashMap<GroupByOperator, Integer>();
    GroupByOperator[] id2Gby = new GroupByOperator[numGbys];
    int index = 0;
    for (List<GroupByOperator> level : sorted) {
      for (GroupByOperator gby : level) {
        gby2Id.put(gby, gby2Id.size());
        id2Gby[index++] = gby;
      }
    }

    // Continuous input (no input cached for discrete GBYs)
    ArrayList<ArrayList<ExprNodeDesc>> allIKeys = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ArrayList<ExprNodeDesc>> allIVals = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ExprNodeDesc> allITids = new ArrayList<ExprNodeDesc>();
    for (GroupByOperator gby : getAllContinousGbys()) {
      ArrayList<ExprNodeDesc> keys = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> vals = new ArrayList<ExprNodeDesc>();
      ExprNodeDesc tid = null;
      GroupByDesc desc = gby.getConf();
      SelectOperator input = inputs.get(gby);

      int i = 0;
      for (; i < desc.getKeys().size(); ++i) {
        keys.add(Utils.generateColumnDescs(input, i).get(0));
      }
      for (AggregateInfo ai : aggregates.get(gby)) {
        if (!ai.getUdafType().equals(UdafType.COUNT)) {
          vals.add(Utils.generateColumnDescs(input, i++).get(0));
        }
      }
      tid = Utils.generateColumnDescs(input, i).get(0);

      allIKeys.add(keys);
      allIVals.add(vals);
      allITids.add(tid);
    }

    // Continuous output
    ArrayList<ArrayList<ExprNodeDesc>> allOCKeys = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ArrayList<ExprNodeDesc>> allOCAggrs = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ExprNodeDesc> allOCLins = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> allOCConds = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> allOCGbyIds = new ArrayList<ExprNodeDesc>();
    for (GroupByOperator gby : getAllContinousGbys()) {
      ArrayList<ExprNodeDesc> keys = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> aggrs = new ArrayList<ExprNodeDesc>();
      ExprNodeDesc lin = null;
      ExprNodeDesc cond = null;
      ExprNodeDesc gbyId = null;
      GroupByDesc desc = gby.getConf();
      SelectOperator output = outputs.get(gby);

      int i = 0;
      for (; i < desc.getKeys().size(); ++i) {
        keys.add(Utils.generateColumnDescs(output, i).get(0));
      }
      for (int j = 0, sz = aggregates.get(gby).size(); j < sz; ++j) {
        aggrs.add(Utils.generateColumnDescs(output, i++).get(0));
      }
      lin = Utils.generateColumnDescs(output, i++).get(0);
      cond = Utils.generateColumnDescs(output, i++).get(0);
      gbyId = Utils.generateColumnDescs(output, i).get(0);

      allOCKeys.add(keys);
      allOCAggrs.add(aggrs);
      allOCLins.add(lin);
      allOCConds.add(cond);
      allOCGbyIds.add(gbyId);
    }

    // Discrete output
    ArrayList<ArrayList<ExprNodeDesc>> allODKeys = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ArrayList<ExprNodeDesc>> allODAggrs = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ExprNodeDesc> allODConds = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> allODGbyIds = new ArrayList<ExprNodeDesc>();
    for (GroupByOperator gby : getAllDiscreteGbys()) {
      ArrayList<ExprNodeDesc> keys = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> aggrs = new ArrayList<ExprNodeDesc>();
      ExprNodeDesc cond = null;
      ExprNodeDesc gbyId = null;
      GroupByDesc desc = gby.getConf();
      SelectOperator output = outputs.get(gby);

      int i = 0;
      for (; i < desc.getKeys().size(); ++i) {
        keys.add(Utils.generateColumnDescs(output, i).get(0));
      }
      for (int j = 0, sz = aggregates.get(gby).size(); j < sz; ++j) {
        aggrs.add(Utils.generateColumnDescs(output, i++).get(0));
      }
      cond = Utils.generateColumnDescs(output, i++).get(0);
      gbyId = Utils.generateColumnDescs(output, i).get(0);

      allODKeys.add(keys);
      allODAggrs.add(aggrs);
      allODConds.add(cond);
      allODGbyIds.add(gbyId);
    }

    // TODO: GBYs' dependency structure
    // TODO: Detailed structure (of each predicate) of every condition column

    // TODO: Type of each aggregate
  }

  private Map<GroupByOperator, Set<GroupByOperator>> getDependencyGraph() {
    Map<GroupByOperator, Set<GroupByOperator>> map =
        new HashMap<GroupByOperator, Set<GroupByOperator>>();
    for (Map.Entry<GroupByOperator, Transform[]> entry : dependencies.entrySet()) {
      Set<GroupByOperator> parents = new HashSet<GroupByOperator>();
      for (Transform trans : entry.getValue()) {
        for (AggregateInfo ai : trans.getAggregatesInvolved()) {
          parents.add(ai.getGroupByOperator());
        }
      }
    }
    return map;
  }

  private Set<GroupByOperator> getAllContinousGbys() {
    return inputs.keySet();
  }

  private Set<GroupByOperator> discrete = null;

  private Set<GroupByOperator> getAllDiscreteGbys() {
    if (discrete == null) {
      discrete = new HashSet<GroupByOperator>(outputs.keySet());
      discrete.removeAll(getAllContinousGbys());
    }
    return discrete;
  }

  // -->

}
