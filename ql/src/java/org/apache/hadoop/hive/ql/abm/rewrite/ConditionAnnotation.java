package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.algebra.ComparisonTransform;
import org.apache.hadoop.hive.ql.abm.lib.TopologicalSort;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ConditionAnnotation {

  private final HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates =
      new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
  private final HashMap<GroupByOperator, ComparisonTransform[]> dependencies =
      new HashMap<GroupByOperator, ComparisonTransform[]>();
  private final ArrayList<ComparisonTransform> transforms = new ArrayList<ComparisonTransform>();

  private final HashMap<GroupByOperator, SelectOperator> inputs =
      new HashMap<GroupByOperator, SelectOperator>();
  private final HashMap<GroupByOperator, SelectOperator> outputs =
      new HashMap<GroupByOperator, SelectOperator>();

  private List<List<GroupByOperator>> sorted = null;
  private Dictionary<GroupByOperator> gbyDict = null;
  private Dictionary<AggregateInfo> aggrDict = null;

  // <-- Used by TraceProcCtx

  public void groupByAt(GroupByOperator gby) {
    dependencies.put(gby, transforms.toArray(new ComparisonTransform[transforms.size()]));
    transforms.clear();
  }

  public void conditionOn(ComparisonTransform trans) {
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
    ArrayList<GroupByOperator> cGbys = new ArrayList<GroupByOperator>(getAllContinousGbys());
    ArrayList<GroupByOperator> dGbys = new ArrayList<GroupByOperator>(getAllDiscreteGbys());

    ArrayList<Operator<? extends OperatorDesc>> inputOps = new ArrayList<Operator<? extends OperatorDesc>>();
    ArrayList<Operator<? extends OperatorDesc>> outputCOps = new ArrayList<Operator<? extends OperatorDesc>>();
    ArrayList<Integer> cTags = new ArrayList<Integer>();
    for (GroupByOperator gby : cGbys) {
      inputOps.add(inputs.get(gby));
      outputCOps.add(outputs.get(gby));
      cTags.add(gbyDict.get(gby));
    }

    ArrayList<Operator<? extends OperatorDesc>> outputDOps = new ArrayList<Operator<? extends OperatorDesc>>();
    ArrayList<Integer> dTags = new ArrayList<Integer>();
    for (GroupByOperator gby : dGbys) {
      outputDOps.add(outputs.get(gby));
      dTags.add(gbyDict.get(gby));
    }

    List<Operator<? extends OperatorDesc>> parents = select.getParentOperators();
    parents.addAll(inputOps);
    parents.addAll(outputCOps);
    parents.addAll(outputDOps);
    select.setParentOperators(parents);
    for (Operator<? extends OperatorDesc> p : inputOps) {
      p.getChildOperators().add(select);
    }
    for (Operator<? extends OperatorDesc> p : outputCOps) {
      p.getChildOperators().add(select);
    }
    for (Operator<? extends OperatorDesc> p : outputDOps) {
      p.getChildOperators().add(select);
    }

    // Continuous input (no input cached for discrete GBYs)
    ArrayList<ArrayList<ExprNodeDesc>> inKeys = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ArrayList<ExprNodeDesc>> inVals = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ExprNodeDesc> inTids = new ArrayList<ExprNodeDesc>();
    for (GroupByOperator gby : cGbys) {
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

      inKeys.add(keys);
      inVals.add(vals);
      inTids.add(tid);
    }

    // Continuous output
    ArrayList<ArrayList<ExprNodeDesc>> outCKeys = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ArrayList<ExprNodeDesc>> outCAggrs = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ExprNodeDesc> outCLins = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> outCConds = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> outCGbyIds = new ArrayList<ExprNodeDesc>();
    ArrayList<ArrayList<UdafType>> outCTypes = new ArrayList<ArrayList<UdafType>>();
    for (GroupByOperator gby : cGbys) {
      ArrayList<ExprNodeDesc> keys = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> aggrs = new ArrayList<ExprNodeDesc>();
      ExprNodeDesc lin = null;
      ExprNodeDesc cond = null;
      ExprNodeDesc gbyId = null;
      ArrayList<UdafType> types = new ArrayList<UdafType>();
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
      for (AggregateInfo ai : aggregates.get(gby)) {
        types.add(ai.getUdafType());
      }

      outCKeys.add(keys);
      outCAggrs.add(aggrs);
      outCLins.add(lin);
      outCConds.add(cond);
      outCGbyIds.add(gbyId);
      outCTypes.add(types);
    }

    // Discrete output
    ArrayList<ArrayList<ExprNodeDesc>> outDAggrs = new ArrayList<ArrayList<ExprNodeDesc>>();
    ArrayList<ExprNodeDesc> outDConds = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> outDGbyIds = new ArrayList<ExprNodeDesc>();
    for (GroupByOperator gby : dGbys) {
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

      outDAggrs.add(aggrs);
      outDConds.add(cond);
      outDGbyIds.add(gbyId);
    }

    select.getConf().setMCSim(cTags, dTags, inKeys, inVals, inTids,
        outCKeys, outCAggrs, outCLins, outCConds, outCGbyIds, outCTypes, outDAggrs, outDConds,
        outDGbyIds);

    // TODO: GBYs' dependency structure
    // TODO: Detailed structure (of each predicate) of every condition column
  }

  private Map<GroupByOperator, Set<GroupByOperator>> getDependencyGraph() {
    Map<GroupByOperator, Set<GroupByOperator>> map =
        new HashMap<GroupByOperator, Set<GroupByOperator>>();
    for (Map.Entry<GroupByOperator, ComparisonTransform[]> entry : dependencies.entrySet()) {
      Set<GroupByOperator> parents = new HashSet<GroupByOperator>();
      for (ComparisonTransform trans : entry.getValue()) {
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

  private Set<GroupByOperator> getAllDiscreteGbys() {
    Set<GroupByOperator> ret = new HashSet<GroupByOperator>(outputs.keySet());
    ret.removeAll(getAllContinousGbys());
    return ret;
  }

  public List<Boolean> getCondFlags(GroupByOperator gby) {
    ComparisonTransform[] trans = dependencies.get(gby);
    List<Boolean> flags = new ArrayList<Boolean>(trans.length);
    for (ComparisonTransform tran : trans) {
      flags.add(tran.isAscending());
    }
    return flags;
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

  public int size() {
    return id2Elem.size();
  }

}
