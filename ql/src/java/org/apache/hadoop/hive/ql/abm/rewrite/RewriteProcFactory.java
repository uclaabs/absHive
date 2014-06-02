package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.lib.PostOrderPlanWalker;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;

/**
 *
 * RewriteProcFactory
 * (1) adds "tid" to the trace from TS to GBY.
 * (2) adds conditions columns to the operator tree: esp. FILTER/JOIN/GBY.
 * (3) adds lineage column to GBY.
 * (4) adds gen_id for GBY.
 * (5) adds select before and after GBY in order to cache.
 *
 */
public class RewriteProcFactory {

  private static final String TID = "tid";

  private static final String SRV_SUM = "srv_sum";
  private static final String SRV_AVG = "srv_avg";
  private static final String SRV_COUNT = "srv_count";
  private static final String CASE_SUM = "case_sum";
  private static final String CASE_AVG = "case_avg";
  private static final String CASE_COUNT = "case_count";

  private static final String COND_JOIN = "cond_join";
  private static final String COND_MERGE = "cond_merge";

  private static final String LIN_SUM = "lin_sum";

  private static final String GEN_ID = "gen_id";

  private static final String EXIST_PROB = "exist_prob";

  public static GenericUDF getUdf(String udfName) {
    // Remember to register the functions:
    // in org.apache.hadoop.hive.ql.exec.FunctionRegistry, use registerUDF
    FunctionInfo fi = FunctionRegistry.getFunctionInfo(udfName);
    assert fi != null;
    // getGenericUDF() actually clones the UDF. Just call it once and reuse.
    return fi.getGenericUDF();
  }

  public static class SelectFactory {

    private final RewriteProcCtx ctx;

    private final List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    private final List<String> outputColumnNames = new ArrayList<String>();
    private final HashMap<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();
    private final RowResolver rowResolver = new RowResolver();

    private Integer countIndex = null;
    private Integer tidIndex = null;
    private final ArrayList<Integer> condIndex = new ArrayList<Integer>();
    private final HashMap<GroupByOperator, Integer> gbyIdIndex = new HashMap<GroupByOperator, Integer>();
    private Integer lineageIndex = null;

    private final HashMap<String, AggregateInfo> lineage = new HashMap<String, AggregateInfo>();

    public SelectFactory(RewriteProcCtx ctx) {
      this.ctx = ctx;
    }

    public int forwardColumn(Operator<? extends OperatorDesc> op, int index, boolean keepName) {
      return addColumn(Utils.generateColumnDescs(op, index).get(0),
          keepName ? op.getSchema().getSignature().get(index).getInternalName()
              : Utils.getColumnInternalName(colList.size()));
    }

    public int addColumn(ExprNodeDesc column) {
      return addColumn(column, Utils.getColumnInternalName(colList.size()));
    }

    private int addColumn(ExprNodeDesc column, String outputName) {
      colList.add(column);
      outputColumnNames.add(outputName);
      columnExprMap.put(outputName, column);
      rowResolver.put("", outputName, new ColumnInfo(outputName, column.getTypeInfo(), "", false));

      return outputColumnNames.size() - 1;
    }

    public void setCountIndex(int index) {
      countIndex = index;
    }

    public void setTidIndex(int index) {
      tidIndex = index;
    }

    public void addCondIndex(int index) {
      condIndex.add(index);
    }

    public void addGbyIdIndex(GroupByOperator gby, int index) {
      gbyIdIndex.put(gby, index);
    }

    public void setLineageIndex(int index) {
      lineageIndex = index;
    }

    public void putLineage(int index, AggregateInfo linfo) {
      lineage.put(outputColumnNames.get(index), linfo);
    }

    @SuppressWarnings("unchecked")
    public SelectOperator getSelectOperator() {
      // Create SEL
      SelectDesc desc = new SelectDesc(colList, outputColumnNames);
      SelectOperator sel = (SelectOperator) OperatorFactory
          .get((Class<SelectDesc>) desc.getClass());
      sel.setConf(desc);
      sel.setSchema(rowResolver.getRowSchema());
      sel.setColumnExprMap(columnExprMap);

      // Put SEL into ParseContext
      OpParseContext opParseContext = new OpParseContext(rowResolver);
      ctx.getParseContext().getOpParseCtx().put(sel, opParseContext);

      if (countIndex != null) {
        ctx.putCountColumnIndex(sel, countIndex);
      }
      if (tidIndex != null) {
        ctx.putTidColumnIndex(sel, tidIndex);
      }
      for (int index : condIndex) {
        ctx.addCondColumnIndex(sel, index);
      }
      for (Map.Entry<GroupByOperator, Integer> entry : gbyIdIndex.entrySet()) {
        ctx.addGbyIdColumnIndex(sel, entry.getKey(), entry.getValue());
      }
      if (lineageIndex != null) {
        ctx.putLineageColumnIndex(sel, lineageIndex);
      }

      for (Map.Entry<String, AggregateInfo> entry : lineage.entrySet()) {
        ctx.putLineage(sel, entry.getKey(), entry.getValue());
      }

      return sel;
    }

  }

  public static SelectOperator appendSelect(
      Operator<? extends OperatorDesc> op, RewriteProcCtx ctx, boolean afterGby,
      boolean forwardCondition, ExprNodeDesc... additionalConds) throws SemanticException {
    SelectFactory selFactory = new SelectFactory(ctx);

    // Forward original columns
    ArrayList<ColumnInfo> signature = op.getSchema().getSignature();
    HashSet<Integer> toSkip = ctx.getSpecialColumnIndexes(op);
    for (int i = 0; i < signature.size(); ++i) {
      if (!toSkip.contains(i)) {
        int index = selFactory.forwardColumn(op, i, true);
        AggregateInfo linfo = ctx.getLineage(op, signature.get(i).getInternalName());
        if (linfo != null) {
          selFactory.putLineage(index, linfo);
        }
      }
    }

    // Forward the tid column
    if (ctx.withTid(op)) {
      assert !afterGby;
      selFactory.setTidIndex(selFactory.forwardColumn(op, ctx.getTidColumnIndex(op), false));
    }

    if (afterGby) {
      // Forward the count column
      Integer countIndex = ctx.getCountColumnIndex(op);
      if (countIndex != null) {
        selFactory.setCountIndex(selFactory.forwardColumn(op, countIndex, false));
      }

      // Forward the lineage column
      Integer lineageIndex = ctx.getLineageColumnIndex(op);
      if (lineageIndex != null) {
        selFactory.setLineageIndex(selFactory.forwardColumn(op, lineageIndex, false));
      }
    }

    // Add the condition column
    List<ExprNodeDesc> conds = new ArrayList<ExprNodeDesc>();
    if (forwardCondition) {
      conds.addAll(Utils.generateColumnDescs(op, ctx.getCondColumnIndexes(op)));
    }
    conds.addAll(Arrays.asList(additionalConds));
    selFactory.addCondIndex(selFactory.addColumn(joinConditions(conds)));

    // Add the group-by-id column for this group-by
    if (afterGby) {
      GroupByOperator gby = (GroupByOperator) op;
      selFactory.addGbyIdIndex(gby, selFactory.addColumn(
          ExprNodeGenericFuncDesc.newInstance(getUdf(GEN_ID), new ArrayList<ExprNodeDesc>())));
    }
    // Forward the original group-by-id columns
    Map<GroupByOperator, Integer> gbyIdIndexes = ctx.getGbyIdColumnIndexes(op);
    if (gbyIdIndexes != null) {
      for (Map.Entry<GroupByOperator, Integer> entry : gbyIdIndexes.entrySet()) {
        GroupByOperator gby = entry.getKey();
        if (!ctx.lastUsedBy(gby, op)) {
          int index = entry.getValue();
          selFactory.addGbyIdIndex(gby, selFactory.forwardColumn(op, index, false));
        }
      }
    }

    // Create SEL
    SelectOperator sel = selFactory.getSelectOperator();

    // Change the connection
    sel.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>(Arrays.asList(op)));
    sel.setChildOperators(new ArrayList<Operator<? extends OperatorDesc>>(op.getChildOperators()));
    for (Operator<? extends OperatorDesc> child : op.getChildOperators()) {
      List<Operator<? extends OperatorDesc>> parents = child.getParentOperators();
      parents.set(parents.indexOf(op), sel);
    }
    op.setChildOperators(new ArrayList<Operator<? extends OperatorDesc>>(Arrays.asList(sel)));

    if (afterGby) {
      ctx.putGroupByOutput((GroupByOperator) op, sel);
    }

    return sel;
  }

  private static ExprNodeDesc joinConditions(List<ExprNodeDesc> conditions)
      throws UDFArgumentException {
    return ExprNodeGenericFuncDesc.newInstance(getUdf(COND_JOIN), conditions);
  }

  public static abstract class RewriteProcessor implements NodeProcessor {

    protected RewriteProcCtx ctx = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      initialize(nd, procCtx);
      return null;
    }

    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      ctx = (RewriteProcCtx) procCtx;
    }

  }

  /**
   *
   * BaseProcessor propagates the correct types.
   *
   */
  public static abstract class BaseProcessor extends RewriteProcessor {

    protected Operator<? extends OperatorDesc> op = null;
    protected RowResolver rowResolver = null;
    protected ArrayList<ColumnInfo> signature = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      // Propagates the correct types
      // Hack! We assume the columns generated by aggregates are w/o transformation.
      for (ColumnInfo ci : signature) {
        AggregateInfo ai = ctx.getLineage(op, ci.getInternalName());
        if (ai != null) {
          ci.setType(ai.getTypeInfo());
        }
      }

      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      op = (Operator<? extends OperatorDesc>) nd;
      rowResolver = ctx.getOpParseContext(op).getRowResolver();
      signature = op.getSchema().getSignature();
    }

  }

  public static class TableScanProcessor extends RewriteProcessor {

    protected Operator<? extends OperatorDesc> ts = null;
    protected ArrayList<ColumnInfo> signature = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      if (ctx.withTid(ts)) {
        int numOfTid = 0;
        for (int i = 0; i < signature.size(); ++i) {
          if (signature.get(i).getInternalName().equals(TID)) {
            ctx.putTidColumnIndex(ts, i);
            ++numOfTid;
          }
        }
        if (numOfTid != 1) {
          AbmUtilities.report(ErrorMsg.SAMPLED_TABLE_WRONG_SCHEMA_ABM);
        }
      }

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      ts = (TableScanOperator) nd;
      signature = ts.getSchema().getSignature();
    }

  }

  /**
   *
   * FileSinkProcessor adds a Select before FileSink to Monte-Carlo simulate the final
   * distributions.
   *
   */
  public static class FileSinkProcessor extends RewriteProcessor {

    private FileSinkOperator fs = null;
    private ArrayList<ColumnInfo> signature = null;
    private RowResolver rowResolver = null;

    private Operator<? extends OperatorDesc> parent = null;
    private ArrayList<ColumnInfo> parentSignature = null;
    private RowResolver parentRR = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      int probIndex = insertSelect();
      initialize(nd, procCtx);

      // Propagates the correct types
      for (int i = 0; i < signature.size(); ++i) {
        AggregateInfo linfo = ctx.getLineage(parent, signature.get(i).getInternalName());
        if (linfo != null) {
          signature.get(i).setType(parentSignature.get(i).getType());
        }
      }
      // Add the probability column
      forwardColumn(probIndex);

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);

      fs = (FileSinkOperator) nd;
      rowResolver = ctx.getOpParseContext(fs).getRowResolver();
      signature = fs.getSchema().getSignature();

      parent = fs.getParentOperators().get(0);
      parentSignature = parent.getSchema().getSignature();
      parentRR = ctx.getOpParseContext(parent).getRowResolver();
    }

    // Insert Select as input and cache it
    private int insertSelect() throws UDFArgumentException {
      SelectFactory selFactory = new SelectFactory(ctx);

      // Forward original columns
      HashSet<Integer> toSkip = ctx.getSpecialColumnIndexes(parent);
      for (int i = 0; i < parentSignature.size(); ++i) {
        if (!toSkip.contains(i)) {
          String internalName = parentSignature.get(i).getInternalName();
          AggregateInfo linfo = ctx.getLineage(parent, internalName);
          if (linfo != null) {
            GenericUDF udf = getUdf(getMeasureFuncName(AbmUtilities.getErrorMeasure()));
            ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
            int gbyIdIndex = ctx.getGbyIdColumnIndex(parent, linfo.getGroupByOperator());
            params.add(Utils.generateColumnDescs(parent, gbyIdIndex).get(0));
            params.add(new ExprNodeConstantDesc(ctx.getAggregateId(linfo)));
            selFactory.addColumn(
                ExprNodeGenericFuncDesc.newInstance(udf, params),
                internalName);
          } else {
            selFactory.forwardColumn(parent, i, true);
          }
        }
      }

      int probIndex = selFactory.addColumn(
          ExprNodeGenericFuncDesc.newInstance(getUdf(EXIST_PROB), new ArrayList<ExprNodeDesc>()));

      // Create SEL
      SelectOperator sel = selFactory.getSelectOperator();

      // Change the connection
      sel.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>(fs
          .getParentOperators()));
      sel.setChildOperators(new ArrayList<Operator<? extends OperatorDesc>>(Arrays.asList(fs)));
      for (Operator<? extends OperatorDesc> par : fs.getParentOperators()) {
        List<Operator<? extends OperatorDesc>> children = par.getChildOperators();
        children.set(children.indexOf(fs), sel);
      }
      fs.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>(Arrays.asList(sel)));

      // Set SelectOperator
      ctx.setupMCSim(sel);

      return probIndex;
    }

    private String getMeasureFuncName(ErrorMeasure measure) {
      return measure.toString();
    }

    private int forwardColumn(int index) {
      ColumnInfo ci = parentSignature.get(index);
      String[] name = parentRR.reverseLookup(ci.getInternalName());
      rowResolver.put(name[0], name[1], ci);

      return signature.size() - 1;
    }

  }

  /**
   *
   * DefaultProcessor implicitly forwards the condition column if exists,
   * i.e., we do not need to make new ExprNodeDesc's (they forward their parent's columns).
   * For Forward, FileSink.
   *
   */
  public static class DefaultProcessor extends BaseProcessor {

    // Note: there should be only one parent
    protected Operator<? extends OperatorDesc> parent = null;
    protected RowResolver parentRR = null;
    protected ArrayList<ColumnInfo> parentSignature = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      // Forward the tid column
      if (ctx.withTid(op)) {
        int index = ctx.getTidColumnIndex(parent);
        // Maintain the tid column index
        ctx.putTidColumnIndex(op, forwardColumn(index));
      }

      // <-- Actually it can only happens to ReduceSink
      // Forward the count column
      Integer countIndex = ctx.getCountColumnIndex(parent);
      if (countIndex != null) {
        assert (nd instanceof ReduceSinkOperator);
        ctx.putCountColumnIndex(op, forwardColumn(countIndex));
      }

      // Forward the lineage column
      Integer lineageIndex = ctx.getLineageColumnIndex(parent);
      if (lineageIndex != null) {
        assert (nd instanceof ReduceSinkOperator);
        ctx.putLineageColumnIndex(op, forwardColumn(lineageIndex));
      }
      // -->

      // Forward the condition columns
      List<Integer> condIndexes = ctx.getCondColumnIndexes(parent);
      if (condIndexes != null) {
        for (int index : condIndexes) {
          // Maintain the condition column index
          ctx.addCondColumnIndex(op, forwardColumn(index));
        }
      }

      // Forward the gbyId columns
      Map<GroupByOperator, Integer> gbyIdIndexes = ctx.getGbyIdColumnIndexes(parent);
      if (gbyIdIndexes != null) {
        for (Map.Entry<GroupByOperator, Integer> entry : gbyIdIndexes.entrySet()) {
          GroupByOperator gby = entry.getKey();
          if (!ctx.lastUsedBy(gby, parent)) {
            int index = entry.getValue();
            // Maintain the id column index
            ctx.addGbyIdColumnIndex(op, gby, forwardColumn(index));
          }
        }
      }

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);

      parent = op.getParentOperators().get(0);
      parentRR = ctx.getOpParseContext(parent).getRowResolver();
      parentSignature = parent.getSchema().getSignature();
    }

    // Implicitly forward the column
    protected int forwardColumn(int index) {
      ColumnInfo ci = parentSignature.get(index);
      String[] name = parentRR.reverseLookup(ci.getInternalName());
      rowResolver.put(name[0], name[1], ci);

      return signature.size() - 1;
    }

  }

  /**
   *
   * ReduceSinkProcessor explicitly forwards the condition column if exists.
   *
   */
  public static class ReduceSinkProcessor extends DefaultProcessor {

    private ReduceSinkOperator rs = null;
    private ReduceSinkDesc desc = null;
    private Map<String, ExprNodeDesc> columnExprMap = null;
    private ArrayList<String> outputValueColumnNames = null;
    private ArrayList<ExprNodeDesc> valueCols = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      rs = (ReduceSinkOperator) nd;
      desc = rs.getConf();
      columnExprMap = rs.getColumnExprMap();
      outputValueColumnNames = desc.getOutputValueColumnNames();
      valueCols = desc.getValueCols();
    }

    // Explicitly forward the column
    @Override
    protected int forwardColumn(int index) {
      ExprNodeColumnDesc column = Utils.generateColumnDescs(parent, index).get(0);
      String columnInternalName = Utils.getColumnInternalName(valueCols.size());
      valueCols.add(column);
      outputValueColumnNames.add(columnInternalName);
      String valName = Utilities.ReduceField.VALUE.toString() + "." + columnInternalName;
      columnExprMap.put(valName, column);
      rowResolver.put("", valName, new ColumnInfo(valName, column.getTypeInfo(), "", false));

      return signature.size() - 1;
    }

  }

  /**
   *
   * SelectProcessor explicitly forwards the condition column if exists.
   *
   */
  public static class SelectProcessor extends DefaultProcessor {

    private SelectOperator sel = null;
    private SelectDesc desc = null;
    private Map<String, ExprNodeDesc> columnExprMap = null;
    private List<String> outputColumnNames = null;
    private List<ExprNodeDesc> colList = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      sel = (SelectOperator) nd;
      desc = sel.getConf();
      columnExprMap = sel.getColumnExprMap();
      outputColumnNames = desc.getOutputColumnNames();
      colList = desc.getColList();
    }

    // Explicitly or implicitly forward the column
    @Override
    protected int forwardColumn(int index) {
      if (desc.isSelStarNoCompute()) {
        return super.forwardColumn(index);
      }

      ExprNodeColumnDesc column = Utils.generateColumnDescs(parent, index).get(0);
      colList.add(column);
      String outputName = Utils.getColumnInternalName(colList.size() - 1);
      outputColumnNames.add(outputName);
      columnExprMap.put(outputName, column);
      rowResolver
          .put("", outputName, new ColumnInfo(outputName, column.getTypeInfo(), "", false));

      return signature.size() - 1;
    }

  }

  /**
   *
   * GroupByProcessor:
   * 1. rewrites the aggregates
   * (a) to use the corresponding ABM version and
   * (b) to return the correct type;
   *
   */
  public static class GroupByProcessor extends RewriteProcessor {

    private GroupByOperator gby = null;
    private GroupByDesc desc = null;
    private ArrayList<ColumnInfo> signature = null;
    private ArrayList<ExprNodeDesc> keys = null;
    private int numKeys = 0;
    private ArrayList<AggregationDesc> aggregators = null;
    private RowResolver rowResovler = null;
    private ArrayList<String> outputColumnNames = null;
    private Map<String, ExprNodeDesc> columnExprMap = null;

    private Operator<? extends OperatorDesc> parent = null;
    private boolean firstGby = false;
    private GenericUDAFEvaluator.Mode evaluatorMode = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      if (!ctx.isUncertain(gby)) {
        return null;
      }

      boolean continuous = firstGby ? ctx.withTid(parent)
          : (ctx.getLineageColumnIndex(parent) != null);

      // Insert Select as input and cache it
      if (firstGby && continuous) {
        insertSelect();
        initialize(nd, procCtx);
      }

      // Rewrite AggregationDesc
      for (int i = 0; i < aggregators.size(); ++i) {
        modifyAggregator(i, continuous);
      }

      if (firstGby) {
        // Add the COUNT(*) column
        ctx.putCountColumnIndex(gby,
            addAggregator(convertUdafName("count", continuous), new ArrayList<Integer>()));
        // Add the column to compute lineage
        if (continuous) {
          ctx.putLineageColumnIndex(gby,
              addAggregator(LIN_SUM, Arrays.asList(ctx.getTidColumnIndex(parent))));
        }
      } else {
        // Add the COUNT(*) column
        ctx.putCountColumnIndex(gby, addAggregator(convertUdafName("count", continuous),
            Arrays.asList(ctx.getCountColumnIndex(parent))));
        // Add the column to compute lineage
        if (continuous) {
          ctx.putLineageColumnIndex(gby,
              addAggregator(LIN_SUM, Arrays.asList(ctx.getLineageColumnIndex(parent))));
        }
      }

      // Add the column to compute condition
      List<Integer> condIndexes = ctx.getCondColumnIndexes(parent);
      assert (condIndexes == null || condIndexes.size() < 2);
      ctx.addCondColumnIndex(gby, addAggregator(COND_MERGE, condIndexes));

      desc.setUncertain(true);
      if (!firstGby) {
        desc.setFlags(ctx.getCondFlags(gby));
      }

      // Add select to generate group id
      if (!firstGby) {
        SelectOperator sel = appendSelect(gby, ctx, true, true);
        if (ctx.lastUsedBy(gby, gby)) {
          ctx.usedAt(gby, sel);
        }
        appendSelect(sel, ctx, false, false, ExprNodeGenericFuncDesc.newInstance(
            getUdf("srv_greater_than"),
            Arrays.asList(Utils.generateColumnDescs(sel, ctx.getCountColumnIndex(sel)).get(0),
                new ExprNodeConstantDesc(new Double(0)),
                Utils.generateColumnDescs(sel, ctx.getGbyIdColumnIndex(sel, gby)).get(0))
            ));
      }

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      gby = (GroupByOperator) nd;
      desc = gby.getConf();
      signature = gby.getSchema().getSignature();
      keys = desc.getKeys();
      numKeys = keys.size();
      aggregators = desc.getAggregators();
      rowResovler = ctx.getOpParseContext(gby).getRowResolver();
      outputColumnNames = desc.getOutputColumnNames();
      columnExprMap = gby.getColumnExprMap();

      parent = gby.getParentOperators().get(0);
      firstGby = !(parent instanceof ReduceSinkOperator);
      aggregators = desc.getAggregators();
      evaluatorMode = SemanticAnalyzer.groupByDescModeToUDAFMode(desc.getMode(), false);
    }

    // Insert Select as input and cache it
    private void insertSelect() {
      SelectFactory selFactory = new SelectFactory(ctx);

      // Forward all columns
      for (ExprNodeDesc key : keys) {
        selFactory.addColumn(key);
      }
      // Up to now there is no special column
      for (AggregationDesc aggregator : aggregators) {
        for (ExprNodeDesc parameter : aggregator.getParameters()) {
          selFactory.addColumn(parameter);
        }
      }

      // Forward the tid column
      assert ctx.withTid(parent);
      selFactory
          .setTidIndex(selFactory.forwardColumn(parent, ctx.getTidColumnIndex(parent), false));

      // There is no count/lineage column to forward

      // Forward the condition columns if exist
      if (ctx.getCondColumnIndexes(parent) != null) {
        for (int index : ctx.getCondColumnIndexes(parent)) {
          selFactory.addCondIndex(selFactory.forwardColumn(parent, index, false));
        }
      }

      // There is no gbyId column to forward

      // Create SEL
      SelectOperator sel = selFactory.getSelectOperator();

      // Change the connection
      sel.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>(gby
          .getParentOperators()));
      sel.setChildOperators(new ArrayList<Operator<? extends OperatorDesc>>(Arrays.asList(gby)));
      for (Operator<? extends OperatorDesc> par : gby.getParentOperators()) {
        List<Operator<? extends OperatorDesc>> children = par.getChildOperators();
        children.set(children.indexOf(gby), sel);
      }
      gby.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>(Arrays.asList(sel)));

      // Change group-by to use columns from this select
      int index = 0;
      for (; index < numKeys; ++index) {
        ExprNodeDesc newKey = Utils.generateColumnDescs(sel, index).get(0);
        keys.set(index, newKey);
        columnExprMap.put(outputColumnNames.get(index), newKey);
      }
      for (int i = 0; i < aggregators.size(); ++i) {
        ArrayList<ExprNodeDesc> parameters = aggregators.get(i).getParameters();
        for (int j = 0; j < parameters.size(); ++j, ++index) {
          ExprNodeDesc newParam = Utils.generateColumnDescs(sel, index).get(0);
          parameters.set(j, newParam);
        }
      }

      // Cache it!
      ctx.putGroupByInput(
          (GroupByOperator) gby.getChildOperators().get(0).getChildOperators().get(0), sel);
    }

    // Rewrite AggregationDesc to:
    // (1) Use the ABM version of SUM/COUNT/AVERAGE
    // (2) Return the correct type
    private void modifyAggregator(int index, boolean continuous) throws SemanticException {
      // (1)
      AggregationDesc aggregator = aggregators.get(index);
      String oldUdafName = aggregator.getGenericUDAFName();
      String udafName = convertUdafName(oldUdafName, continuous);
      ArrayList<ExprNodeDesc> parameters = aggregator.getParameters();
      boolean distinct = aggregator.getDistinct();
      GenericUDAFEvaluator udafEvaluator =
          SemanticAnalyzer.getGenericUDAFEvaluator(udafName,
              parameters, null, distinct, false);
      assert (udafEvaluator != null);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
          udafEvaluator, evaluatorMode, parameters);
      AggregationDesc aggregationDesc = new AggregationDesc(udafName,
          udaf.genericUDAFEvaluator, udaf.convertedParameters, distinct, evaluatorMode);

      aggregators.set(index, aggregationDesc);
      // (2): We only need to change the ColumnInfo in the schema
      // as other places (e.g., RowResolver) reference to the same ColumnInfo
      signature.get(numKeys + index).setType(udaf.returnType);
    }

    private String convertUdafName(String udaf, boolean continuous) {
      if (udaf.equalsIgnoreCase("sum")) {
        return continuous ? SRV_SUM : CASE_SUM;
      } else if (udaf.equalsIgnoreCase("avg")) {
        return continuous ? SRV_AVG : CASE_AVG;
      } else {
        assert udaf.equalsIgnoreCase("count");
        return continuous ? SRV_COUNT : CASE_COUNT;
      }
    }

    private int addAggregator(String udafName, List<Integer> parameterIndexes)
        throws SemanticException {
      ArrayList<ExprNodeDesc> parameters = new ArrayList<ExprNodeDesc>(
          Utils.generateColumnDescs(parent, parameterIndexes));
      GenericUDAFEvaluator udafEvaluator =
          SemanticAnalyzer.getGenericUDAFEvaluator(udafName,
              parameters, null, false, false);
      assert (udafEvaluator != null);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
          udafEvaluator, evaluatorMode, parameters);
      AggregationDesc aggregationDesc = new AggregationDesc(udafName,
          udaf.genericUDAFEvaluator, udaf.convertedParameters, false, evaluatorMode);

      // colExprMap only has keys in it, so don't add this aggregation
      String columnInternalName = Utils.getColumnInternalName(numKeys + aggregators.size());
      aggregators.add(aggregationDesc);
      rowResovler.put("", columnInternalName, new ColumnInfo(columnInternalName, udaf.returnType,
          "", false));
      outputColumnNames.add(columnInternalName);

      return signature.size() - 1;
    }

  }

  /**
   *
   * FilterProcessor.
   *
   */
  public static class FilterProcessor extends DefaultProcessor {

    private FilterOperator fil = null;
    private FilterDesc desc = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      ExprNodeDesc ret = rewrite(desc.getPredicate(), fil, ctx, null);
      if (ret != null) {
        desc.setPredicate(ret);
      }

      // Add a SEL after FIL to transform the condition column
      if (ctx.getTransform(fil) != null) {
        appendSelect(fil, ctx, false, true,
            ctx.getTransform(fil).toArray(new ExprNodeDesc[0]));
      }

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      fil = (FilterOperator) nd;
      desc = fil.getConf();
    }

    private ExprNodeDesc rewrite(ExprNodeDesc expr, FilterOperator fil,
        RewriteProcCtx ctx, List<ExprNodeDesc> extra) throws SemanticException {
      if (expr instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr;
        GenericUDF udf = func.getGenericUDF();

        if (udf instanceof GenericUDFOPAnd) {
          List<ExprNodeDesc> children = func.getChildExprs();
          for (int i = 0; i < children.size(); ++i) {
            ExprNodeDesc ret = rewrite(children.get(i), fil, ctx, null);
            if (ret != null) {
              children.set(i, ret);
            }
          }
          return null;
        }

        if ((udf instanceof GenericUDFOPEqualOrGreaterThan)
            || (udf instanceof GenericUDFOPEqualOrLessThan)
            || (udf instanceof GenericUDFOPGreaterThan)
            || (udf instanceof GenericUDFOPLessThan)) {
          // Code indicating which child(ren) is(are) Srv
          // The i-th bit is 1 if the i-th child is a Srv
          int changeCode = 0;

          List<ExprNodeDesc> children = func.getChildExprs();
          ArrayList<ExprNodeDesc> gbyIds = new ArrayList<ExprNodeDesc>();
          assert children.size() == 2;
          for (int i = 0; i < children.size(); ++i) {
            ExprNodeDesc ret = rewrite(children.get(i), fil, ctx, gbyIds);
            changeCode = changeCode << 1;
            if (ret != null) {
              changeCode = (changeCode | 1);
              children.set(i, ret);
            }
          }

          // Convert comparison to special ABM comparison
          if (changeCode != 0) {
            normalizeParameters(children, changeCode);

            // Add to ctx: filter transforms the condition set of the annotation
            ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>(children);
            params.addAll(gbyIds);
            ctx.addTransform(fil, ExprNodeGenericFuncDesc.newInstance(
                getUdf(getCondUdfName(udf, changeCode)), params));

            // Rewrite the predicate.
            return ExprNodeGenericFuncDesc.newInstance(
                getUdf(getPredUdfName(udf, changeCode)),
                children);
          }
        }

        return null;
      }

      if (expr instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc column = (ExprNodeColumnDesc) expr;
        AggregateInfo aggr = ctx.getLineage(parent, column.getColumn());
        if (aggr != null) {
          column.setTypeInfo(aggr.getTypeInfo());
          int idx = ctx.getGbyIdColumnIndex(fil, aggr.getGroupByOperator());
          extra.add(Utils.generateColumnDescs(fil, idx).get(0));
          return column;
        }
        return null;
      }

      return null;
    }

    // Normalize the parameter list to move Srv to the left hand side
    private void normalizeParameters(List<ExprNodeDesc> children, int code) {
      // code 1: left is not a Srv, right is a Srv
      // code 2: left is a Srv, right is not a Srv
      // code 3: both left and right are Srvs
      if (code == 1) {
        // Swap
        ExprNodeDesc tmp = children.get(0);
        children.set(0, children.get(1));
        children.set(1, tmp);
      }
    }

    private String getCondUdfName(GenericUDF udf, int code) {
      // code 1: left is not a Srv, right is a Srv
      // code 2: left is a Srv, right is not a Srv
      // code 3: both left and right are Srvs
      switch (code) {
      case 1:
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_less_than";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_greater_than";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_less_than";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_greater_than";
        }

      case 2:
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_greater_than";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_less_than";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_greater_than";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_less_than";
        }

      default: // 3
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_greater_than_srv";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_less_than_srv";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_greater_than_srv";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_less_than_srv";
        }
      }
    }

    private String getPredUdfName(GenericUDF udf, int code) {
      // code 1: left is not a Srv, right is a Srv
      // code 2: left is a Srv, right not is a Srv
      // code 3: both left and right are Srvs
      switch (code) {
      case 1:
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_less_than_f";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_greater_than_f";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_less_than_f";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_greater_than_f";
        }

      case 2:
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_greater_than_f";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_less_than_f";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_greater_than_f";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_less_than_f";
        }

      default: // 3
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_greater_than_srv_f";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_less_than_srv_f";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_greater_than_srv_f";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_less_than_srv_f";
        }
      }
    }
  }

  public static class JoinProcessor extends BaseProcessor {

    private JoinOperator join = null;
    private JoinDesc desc = null;
    private List<String> outputColumnNames = null;
    private Map<String, Byte> reversedExprs = null;
    private Map<Byte, List<ExprNodeDesc>> exprMap = null;
    private Map<String, ExprNodeDesc> columnExprMap = null;

    private ReduceSinkOperator parentRS = null;
    private RowResolver parentRR = null;
    private Byte tag = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      int countofTidCols = 0;
      int countOfCondCols = 0;
      for (Operator<? extends OperatorDesc> parent : join.getParentOperators()) {
        parentRS = (ReduceSinkOperator) parent;
        parentRR = ctx.getOpParseContext(parentRS).getRowResolver();
        tag = (byte) parentRS.getConf().getTag();

        // Forward the tid column
        if (ctx.withTid(join)) {
          Integer index = ctx.getTidColumnIndex(parentRS);
          if (index != null) {
            ++countofTidCols;
            ctx.putTidColumnIndex(join, forwardColumn(index));
            // This is the table we must not shuffle during mapjoin
            desc.setToPin(join.getParentOperators().indexOf(parent));
          }
        }

        // There is no count/lineage column to forward

        // Forward the condition columns
        List<Integer> condIndexes = ctx.getCondColumnIndexes(parent);
        if (condIndexes != null) {
          for (int index : condIndexes) {
            // Maintain the condition column index
            ctx.addCondColumnIndex(op, forwardColumn(index));
            ++countOfCondCols;
          }
        }

        // Forward the gbyId columns
        Map<GroupByOperator, Integer> gbyIdIndexes = ctx.getGbyIdColumnIndexes(parent);
        if (gbyIdIndexes != null) {
          for (Map.Entry<GroupByOperator, Integer> entry : gbyIdIndexes.entrySet()) {
            GroupByOperator gby = entry.getKey();
            if (!ctx.lastUsedBy(gby, parent)) {
              int index = entry.getValue();
              ctx.addGbyIdColumnIndex(join, gby, forwardColumn(index));
            }
          }
        }
      }

      assert countofTidCols <= 1;
      if (countOfCondCols > 1) {
        appendSelect(join, ctx, false, true);
      }

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      join = (JoinOperator) nd;
      desc = join.getConf();
      outputColumnNames = desc.getOutputColumnNames();
      reversedExprs = desc.getReversedExprs();
      exprMap = desc.getExprs();
      columnExprMap = join.getColumnExprMap();
    }

    protected int forwardColumn(int index) throws SemanticException {
      ExprNodeColumnDesc column = Utils.generateColumnDescs(parentRS, index).get(0);
      String columnInternalName = Utils.getColumnInternalName(outputColumnNames.size());
      outputColumnNames.add(columnInternalName);
      reversedExprs.put(columnInternalName, tag);
      List<ExprNodeDesc> exprList = exprMap.get(tag);
      exprList.add(column);
      columnExprMap.put(columnInternalName, column);

      String[] names = parentRR.reverseLookup(column.getColumn());
      ColumnInfo ci = parentRR.get(names[0], names[1]);
      rowResolver.put(names[0], names[1],
          new ColumnInfo(columnInternalName, ci.getType(), names[0],
              ci.getIsVirtualCol(), ci.isHiddenVirtualCol()));

      return signature.size() - 1;
    }

  }

  public static NodeProcessor getFileSinkProc() {
    return new FileSinkProcessor();
  }

  public static NodeProcessor getTableScanProc() {
    return new TableScanProcessor();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultProcessor();
  }

  public static NodeProcessor getReduceSinkProc() {
    return new ReduceSinkProcessor();
  }

  public static NodeProcessor getSelectProc() {
    return new SelectProcessor();
  }

  public static NodeProcessor getGroupByProc() {
    return new GroupByProcessor();
  }

  public static NodeProcessor getFilterProc() {
    return new FilterProcessor();
  }

  public static NodeProcessor getJoinProc() {
    return new JoinProcessor();
  }

  public static ParseContext rewritePlan(TraceProcCtx tctx) throws SemanticException {
    RewriteProcCtx ctx = new RewriteProcCtx(tctx);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"),
        getFilterProc());
    opRules.put(new RuleRegExp("R2", ReduceSinkOperator.getOperatorName() + "%"),
        getReduceSinkProc());
    opRules.put(new RuleRegExp("R3", SelectOperator.getOperatorName() + "%"),
        getSelectProc());
    opRules.put(new RuleRegExp("R4", GroupByOperator.getOperatorName() + "%"),
        getGroupByProc());
    opRules.put(new RuleRegExp("R5", CommonJoinOperator.getOperatorName() + "%"),
        getJoinProc());
    opRules.put(new RuleRegExp("R6", TableScanOperator.getOperatorName() + "%"),
        getTableScanProc());
    opRules.put(new RuleRegExp("R7", FileSinkOperator.getOperatorName() + "%"),
        getFileSinkProc());

    // The dispatcher fires the processor corresponding to the closest matching rule
    // and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, ctx);
    GraphWalker walker = new PostOrderPlanWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(ctx.getParseContext().getTopOps().values());
    walker.startWalking(topNodes, null);

    return ctx.getParseContext();
  }

}
