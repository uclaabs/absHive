package org.apache.hadoop.hive.ql.abm.rewrite;

import it.unimi.dsi.fastutil.ints.IntArrayList;

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
import org.apache.hadoop.hive.ql.abm.simulation.PredicateType;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ConditionAnnotation {

  private final HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates =
      new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
  private final HashMap<GroupByOperator, ComparisonTransform[]> dependencies =
      new HashMap<GroupByOperator, ComparisonTransform[]>();
  private final ArrayList<ComparisonTransform> transforms = new ArrayList<ComparisonTransform>();

  private final HashSet<GroupByOperator> continuous = new HashSet<GroupByOperator>();
  private final HashMap<GroupByOperator, SelectOperator> inputs =
      new HashMap<GroupByOperator, SelectOperator>();
  private final HashMap<GroupByOperator, SelectOperator> outputs =
      new HashMap<GroupByOperator, SelectOperator>();

  private final List<String> cachedOutputs = new ArrayList<String>();
  private final List<String> cachedInputs = new ArrayList<String>();

  private List<List<GroupByOperator>> sorted = null;
  private List<GroupByOperator> sortedContinuous = null;
  private List<GroupByOperator> sortedDiscrete = null;
  private Dictionary<GroupByOperator> gbyDict = null;

  // <-- Used by TraceProcCtx

  public void groupByAt(GroupByOperator gby, boolean isContinuous) {
    dependencies.put(gby, transforms.toArray(new ComparisonTransform[transforms.size()]));
    transforms.clear();
    if (isContinuous) {
      continuous.add(gby);
    }
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
    continuous.addAll(other.continuous);
  }

  // -->

  // <-- Used by RewriteProcCtx

  // A query is either a simple query, in which case we do not need to cache any input;
  // or is a complex query, in which case we have to cache the inputs of all continuous gbys.
  // This is because the dependency graph is a connected tree for complex queries, and thus
  // every continuous gby is connected with another.
  public boolean isSimpleQuery() {
    if (!AbmUtilities.isCovarianceNegligible()) {
      return false;
    }

    for (ComparisonTransform[] val : dependencies.values()) {
      if (val.length != 0) {
        return false;
      }
    }

    HashSet<AggregateInfo> hash = new HashSet<AggregateInfo>();
    for (ComparisonTransform ct : transforms) {
      hash.addAll(ct.getAggregatesInvolved());
    }
    if (hash.size() <= 2) {
      // It must contain COUNT(*) > 0
      return true;
    }

    return false;
  }

  public boolean isContinuous(GroupByOperator gby) {
    return continuous.contains(gby);
  }

  public void putGroupByInput(GroupByOperator gby, SelectOperator input) {
    String tableName = AbmUtilities.ABM_CACHE_INPUT_PREFIX + gby.toString();
    input.getConf().cache(tableName,
        AbmUtilities.fixSerDe(input.getSchema().getSignature()));
    inputs.put(gby, input);
    cachedInputs.add(tableName);
  }

  public void putGroupByOutput(GroupByOperator gby, SelectOperator output) {
    String tableName = AbmUtilities.ABM_CACHE_OUTPUT_PREFIX + gby.toString();
    output.getConf().cache(tableName,
        AbmUtilities.fixSerDe(output.getSchema().getSignature()));
    outputs.put(gby, output);
    cachedOutputs.add(tableName);
  }

  public int[] getAggregateId(AggregateInfo ai) {
    if (sorted == null) {
      setupIds();
    }
    return new int[] {
        gbyDict.get(ai.getGroupByOperator()),
        ai.getIndex() != -1 ? ai.getIndex() : aggregates.get(ai.getGroupByOperator()).size() - 1
    };
  }

  private void setupIds() {
    Map<GroupByOperator, Set<GroupByOperator>> map = getDependencyGraph();
    sorted = TopologicalSort.getOrderByLevel(map);

    sortedContinuous = new ArrayList<GroupByOperator>();
    sortedDiscrete = new ArrayList<GroupByOperator>();
    for (List<GroupByOperator> level : sorted) {
      for (GroupByOperator gby : level) {
        if (continuous.contains(gby)) {
          sortedContinuous.add(gby);
        } else {
          sortedDiscrete.add(gby);
        }
      }
    }

    // Assign ids to GroupByOperators
    List<GroupByOperator> gbys = new ArrayList<GroupByOperator>();
    gbys.addAll(sortedContinuous);
    gbys.addAll(sortedDiscrete);
    gbyDict = new Dictionary<GroupByOperator>(gbys);
  }

  public void setupMCSim(SelectOperator select) {
    if (sorted == null) {
      setupIds();
    }

    // Rearrange the connections
    List<Operator<? extends OperatorDesc>> inputOps = new ArrayList<Operator<? extends OperatorDesc>>();
    boolean simpleQuery = inputs.isEmpty();
    if (!simpleQuery) {
      for (GroupByOperator gby : sortedContinuous) {
        inputOps.add(inputs.get(gby));
      }
    }
    List<Operator<? extends OperatorDesc>> outputContinuousOps = new ArrayList<Operator<? extends OperatorDesc>>();
    for (GroupByOperator gby : sortedContinuous) {
      outputContinuousOps.add(outputs.get(gby));
    }
    List<Operator<? extends OperatorDesc>> outputDiscreteOps = new ArrayList<Operator<? extends OperatorDesc>>();
    for (GroupByOperator gby : sortedDiscrete) {
      outputDiscreteOps.add(outputs.get(gby));
    }

    List<Operator<? extends OperatorDesc>> parents = select.getParentOperators();
    parents.addAll(inputOps);
    parents.addAll(outputContinuousOps);
    parents.addAll(outputDiscreteOps);
    for (Operator<? extends OperatorDesc> p : inputOps) {
      p.getChildOperators().add(select);
    }
    for (Operator<? extends OperatorDesc> p : outputContinuousOps) {
      p.getChildOperators().add(select);
    }
    for (Operator<? extends OperatorDesc> p : outputDiscreteOps) {
      p.getChildOperators().add(select);
    }

    // Continuous input (no input cached for discrete GBYs): keys, vals, tid
    // Continuous output:
    // 1. in simple queries: aggregates, condition, group-id
    // 2. in complex queries: keys, aggregates, lineage, condition, group-id
    List<Integer> numKeysContinuous = new ArrayList<Integer>();
    List<List<UdafType>> aggrTypes = new ArrayList<List<UdafType>>();
    for (GroupByOperator gby : sortedContinuous) {
      numKeysContinuous.add(gby.getConf().getKeys().size());
      List<UdafType> types = new ArrayList<UdafType>();
      for (AggregateInfo ai : aggregates.get(gby)) {
        types.add(ai.getUdafType());
      }
      aggrTypes.add(types);
    }

    // Discrete output: aggregates, condition, group-id
    List<Integer> numKeysDiscrete = new ArrayList<Integer>();
    for (GroupByOperator gby : sortedDiscrete) {
      numKeysDiscrete.add(gby.getConf().getKeys().size());
    }

    select.getConf().setMCSim(simpleQuery, numKeysContinuous, aggrTypes, numKeysDiscrete,
        cachedOutputs, cachedInputs);

    // TODO: GBYs' dependency structure

    int[][][] gby3d = new int[sorted.size()][][];
    int[][][] index3d = new int[sorted.size()][][];
    PredicateType[][][] type3d = new PredicateType[sorted.size()][][];

    for (int i = 0; i < sorted.size(); ++i) {
      List<GroupByOperator> level = sorted.get(i);
      int[][] gby2d = new int[level.size()][];
      int[][] index2d = new int[level.size()][];
      PredicateType[][] type2d = new PredicateType[level.size()][];

      for (int j = 0; j < level.size(); ++j) {
        IntArrayList gby1d = new IntArrayList();
        IntArrayList index1d = new IntArrayList();
        List<PredicateType> type1d = new ArrayList<PredicateType>();

        // unfold it to array
        for (ComparisonTransform transform : dependencies.get(level.get(j))) {
          Set<AggregateInfo> ais = transform.getAggregatesInvolved();
          for (AggregateInfo ai : ais) {
            gby1d.add(level.indexOf(ai.getGroupByOperator()));
            index1d.add(ai.getIndex());
          }
          type1d.add(transform.getPredicateType());
        }

        gby2d[j] = gby1d.toIntArray();
        index2d[j] = index1d.toIntArray();
        type2d[j] = type1d.toArray(new PredicateType[type1d.size()]);
      }

      gby3d[i] = gby2d;
      index3d[i] = index2d;
      type3d[i] = type2d;
    }
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
    return elem2Id.get(element);
  }

  public int size() {
    return id2Elem.size();
  }

}
