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

  private List<List<GroupByOperator>> sorted = null;
  private Dictionary<GroupByOperator> gbyDict = null;
  private Dictionary<AggregateInfo> aggrDict = null;

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

  public void returnVal(AggregateInfo ai) {
    GroupByOperator gby = ai.getGroupByOperator();
    TreeSet<AggregateInfo> buf = aggregates.get(gby);
    if (buf == null) {
      buf = new TreeSet<AggregateInfo>();
      aggregates.put(gby, buf);
    }
    buf.add(ai);
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

  public int getAggregateId(AggregateInfo ai) {
    if (aggrDict == null) {
      setupIds();
    }
    return aggrDict.get(ai);
  }

  private void setupIds() {
    Map<GroupByOperator, Set<GroupByOperator>> map = getDependencyGraph();
    sorted = TopologicalSort.getOrderByLevel(map);

    // Assign ids to GroupByOperators
    List<GroupByOperator> gbys = new ArrayList<GroupByOperator>();
    for (List<GroupByOperator> level : sorted) {
      gbys.addAll(level);
    }
    gbyDict = new Dictionary<GroupByOperator>(gbys);

    // Assign ids to AggregateInfos
    List<AggregateInfo> aggrs = new ArrayList<AggregateInfo>();
    for (List<GroupByOperator> level : sorted) {
      for (GroupByOperator gby : level) {
        aggrs.addAll(aggregates.get(gby));
      }
    }
    aggrDict = new Dictionary<AggregateInfo>(aggrs);
  }

  public void setupMCSim(SelectOperator select) {
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
    ArrayList<ArrayList<ExprNodeDesc>> allODAggrs = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ExprNodeDesc> allODConds = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> allODGbyIds = new ArrayList<ExprNodeDesc>();
    for (GroupByOperator gby : getAllDiscreteGbys()) {
      ArrayList<ExprNodeDesc> aggrs = new ArrayList<ExprNodeDesc>();
      ExprNodeDesc cond = null;
      ExprNodeDesc gbyId = null;
      GroupByDesc desc = gby.getConf();
      SelectOperator output = outputs.get(gby);

      int i = desc.getKeys().size();
      for (int j = 0, sz = aggregates.get(gby).size(); j < sz; ++j) {
        aggrs.add(Utils.generateColumnDescs(output, i++).get(0));
      }
      cond = Utils.generateColumnDescs(output, i++).get(0);
      gbyId = Utils.generateColumnDescs(output, i).get(0);

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
      map.put(entry.getKey(), parents);
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

class Dictionary<T> {

  private HashMap<T, Integer> elem2Id = null;
  private ArrayList<T> id2Elem = null;

  public Dictionary(List<T> elements) {
    elem2Id = new HashMap<T, Integer>(elements.size());
    id2Elem = new ArrayList<T>(elements.size());

    for (T elem : elements) {
      elem2Id.put(elem, elem2Id.size());
      id2Elem.add(elem);
    }
  }

  public T get(int index) {
    return id2Elem.get(index);
  }

  public int get(T element) {
    System.out.println("hehehe" + elem2Id);
    System.out.println("hahaha " + element);
    return elem2Id.get(element);
  }

}
