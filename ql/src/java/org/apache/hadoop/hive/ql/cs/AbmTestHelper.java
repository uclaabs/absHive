package org.apache.hadoop.hive.ql.cs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Test class for ABM
 *
 * AbmTestHelper.
 *
 */
public class AbmTestHelper {

  private static final String planFileFolder = "plan";
  private static final String exceptionFile = planFileFolder + "/exceptions.txt";
  private static final String jsonPlanFile = "json_plan.txt";

  static String planFile;

  static LineageCtx ctx;
  static ParseContext pCtx;
  static boolean printExprMap = true;
  static boolean needLogToFile = false;
  static int rootOpId = 0;

  static {
    File f = new File(planFileFolder);
    if (!f.exists()) {
      f.mkdir();
    }

    if (!f.isDirectory()) {
      f.delete();
      f.mkdir();
    }

    f = new File(exceptionFile);
    f.delete();
    try {
      f.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String getOpBriefInfo(Operator<? extends OperatorDesc> op) {
    int id = Integer.parseInt(op.getIdentifier());
    if (id <= rootOpId || op.getName().equals("FIL")) {
      return op.toString();
    }
    else {
      //newly added operators
      return "**" + op.toString();
    }
  }

  private static String getSampledTable() {
    String sampledTable = AbmUtilities.getSampledTable();
    if (AbmUtilities.inAbmMode()) {
      return sampledTable;
    }
    else {
      return "";
    }
  }

  /**
   * a small tree with minimal info
   * @param op
   * @param level
   */
  private static void printTree(Operator<? extends OperatorDesc> op, int level) {
    if (op instanceof TableScanOperator) {
      String name = pCtx.getTopToTable().get(op).getTableName();
      println(level, getOpBriefInfo(op) + " " +  name.toLowerCase().equals(getSampledTable()));
    } else {
      println(level, getOpBriefInfo(op));
    }

    List<Operator<? extends OperatorDesc>> lst = op.getParentOperators();
    if (lst != null) {
      for (Operator<? extends OperatorDesc> l: lst) {
        printTree(l, level + 1);
        /**
         * for selectOp, we only print its first parent currently
         */
        if (op instanceof SelectOperator) {
          break;
        }
      }
    }
  }

  private static void asserts(Operator<? extends OperatorDesc> op) {
    RowSchema schema = op.getSchema();
    Map<String, ExprNodeDesc> columnExprMap = op.getColumnExprMap();

    assert(schema != null);

    if (op instanceof GroupByOperator) {
      GroupByOperator gby = (GroupByOperator) op;
      int keySize = gby.getConf().getKeys().size();
      int aggrSize = gby.getConf().getAggregators().size();
      assert(keySize == columnExprMap.keySet().size());
      assert(keySize + aggrSize == schema.getSignature().size());
    }
    else if (op instanceof SelectOperator) {
      assert(schema.getSignature().size() == ((SelectOperator) op).getConf().getColList().size());
    }
    else if (op instanceof FilterOperator) {
    }
    else if (op instanceof JoinOperator) {
    }
    else if (op instanceof TableScanOperator) {
    }
    else {
    }
  }

  static class PrintSchema {
    static List<ColumnInfo> columnInfos = null;
    static int level;

    public static void process(int level, Operator<? extends OperatorDesc> op) {
      PrintSchema.level = level;
      if ((columnInfos = op.getSchema().getSignature()) == null) {
        return;
      }

      println(level, "Schema: ");
      if (op instanceof GroupByOperator) {
        perform((GroupByOperator) op);
      } else if (op instanceof SelectOperator) {
        perform((SelectOperator) op);
      } else {
        perform(op);
      }
    }

    private static void perform(GroupByOperator gby) {
      ArrayList<ExprNodeDesc> keys = gby.getConf().getKeys();
      ArrayList<AggregationDesc> aggrs = gby.getConf().getAggregators();

      boolean isDistinct = false;
      if (aggrs != null) {
        for (AggregationDesc desc : aggrs) {
          isDistinct = isDistinct || desc.getDistinct();
        }
      }

      if (isDistinct) {
        println(level, "Distinct as Group By");
        return;
      }

      //keys
      for (int i=0; i<keys.size(); i++) {
        println(level, columnInfos.get(i).getInternalName() + " <- " + keys.get(i).getExprString() + " (key)");
      }

      //aggrs
      for (int i=keys.size(); i<columnInfos.size(); i++) {
        println(level, columnInfos.get(i).getInternalName() + " <- " + aggrs.get(i-keys.size()).getExprString());
      }
    }

    private static void perform(SelectOperator sel) {
      int i = 0;
      for (ColumnInfo info: columnInfos) {
        println(level, info.getInternalName() + " <- " + sel.getConf().getColList().get(i).getExprString());
        i += 1;
      }
    }

    private static void perform(Operator<? extends OperatorDesc> op) {
      for (ColumnInfo info: columnInfos) {
        println(level, info.getInternalName());
      }
    }

  }

  private static void visit(Operator<? extends OperatorDesc> op, int level) {
    //asserts
    asserts(op);

    //print Op info
    println(level, getOpBriefInfo(op));

    //print op child/parent
//    println(level, "Children");
//    if (op.getChildOperators() != null) {
//      for (Operator<? extends OperatorDesc> p: op.getChildOperators()) {
//        println(level, p);
//      }
//    }
//    else  {
//      println(level, "null");
//    }
//
//    println(level, "Parents");
//    if (op.getParentOperators() != null) {
//      for (Operator<? extends OperatorDesc> p: op.getParentOperators()) {
//        println(level, p);
//      }
//    }
//    else  {
//      println(level, "null");
//    }

    //print Schema
    PrintSchema.process(level, op);

    //print Column Expr Map
    //not for Select, GroupBy
    if (printExprMap && !(op instanceof SelectOperator) && !(op instanceof GroupByOperator)) {
      Map<String, ExprNodeDesc> columnExprMap = op.getColumnExprMap();
      if (columnExprMap != null && columnExprMap.entrySet() != null) {
        println(level, "Column Expr Map: ");
        for (Entry<String, ExprNodeDesc> entry: columnExprMap.entrySet()) {
          if (entry.getValue() instanceof ExprNodeColumnDesc) {
            //ExprNodeColumnDesc expr = (ExprNodeColumnDesc) entry.getValue();
            //String[] names = expr.getColumn().split("\\.");

            println(level, entry.getKey() + ": "
                + ((ExprNodeColumnDesc)entry.getValue()).getTabAlias()
                + ((ExprNodeColumnDesc)entry.getValue()).getCols());
          } else if (entry.getValue() instanceof ExprNodeConstantDesc) {
            println(level, entry.getKey() + ":: "
                + ((ExprNodeConstantDesc)entry.getValue()).getExprString());
          } else {
            println(level, entry.getKey() + ":"
                + entry.getValue().toString());
          }
        }
      }
    }

    //print additional info
    if (op instanceof TableScanOperator) {
      String name = pCtx.getTopToTable().get(op).getTableName();
      println(level, "[TableScan] TabName: " + name + " isSampleTable: " + name.toLowerCase().equals(getSampledTable()));
    }
    else if (op instanceof FilterOperator) {
      println(level, "[Filter] Predicate:");
      println(level, ((FilterOperator)op).getConf().getPredicate().getExprString());
    }
    else if (op instanceof JoinOperator) {
      println(level, "[Join]");
    }

    //print lineage info
    //println(level, ctx.get(sinkOp));
    println();

    List<Operator<? extends OperatorDesc>> lst = op.getParentOperators();
    if (lst != null) {
      for (Operator<? extends OperatorDesc> l: lst) {
        visit(l, level + 1);
        /**
         * for selectOp, we only print its first parent currently
         */
        if (op instanceof SelectOperator) {
          break;
        }
      }
    }

  }


  private static void constructJsonMap(Operator<? extends OperatorDesc> op, Map<Operator<? extends OperatorDesc>, JsonNode> nodeMap) {
    JsonNode node = new JsonNode(op);
    nodeMap.put(op, node);

    List<Operator<? extends OperatorDesc>> lst = op.getParentOperators();
    if (lst != null) {
      for (Operator<? extends OperatorDesc> l: lst) {
        constructJsonMap(l, nodeMap);
      }
    }
  }

  private static String getJSONPlan(Operator<? extends OperatorDesc> op) {
    Map<Operator<? extends OperatorDesc>, JsonNode> nodeMap = new HashMap<Operator<? extends OperatorDesc>, JsonNode>();
    constructJsonMap(op, nodeMap);

    for (Entry<Operator<? extends OperatorDesc>, JsonNode> entry : nodeMap.entrySet()) {
      //append children
      List<Operator<? extends OperatorDesc>> children = entry.getKey().getParentOperators();
      if (children != null) {
        for (Operator<? extends OperatorDesc> child: children) {
          entry.getValue().addChild(nodeMap.get(child));
        }
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(nodeMap.get(op).getJSONObject().toString());
    sb.append("]");

    return sb.toString();
    /*
    StringBuilder sb = new StringBuilder();
    sb.append(b);

    for (T key : map.keySet()) {
      nodeMap.put(key, new Node<T>(key));
    }
     */
  }

  public static void printBeforeRewritePlan(Operator<? extends OperatorDesc> op) {
    try {
      needLogToFile = false;
      println(0, "####### Before Rewrite #########");
      analyzeHelper(op, 0);
      //System.out.println(getJSONPlan(op));
      //json array cast error!

      //logPlan(getJSONPlan(op));
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void initPlanFile() throws IOException {
    planFile = planFileFolder + "/" + "q" + AbmUtilities.getLabel() + "_plan.txt";
    File f = new File(planFile);
    f.delete();
    f.createNewFile();
  }

  public static void printAfterRewritePlan(Operator<? extends OperatorDesc> op, ParseContext pCtx) {
    try {
      initPlanFile();

      AbmTestHelper.pCtx = pCtx;
      rootOpId = Integer.parseInt(op.getIdentifier());

      needLogToFile = true;
      try {
        println(0, "####### After Rewrite #########");
        visit(op, 0);
      }
      catch (Exception e) {
        String s = e.getMessage() + " " + Arrays.asList(e.getStackTrace()).toString();
        //logExceptions won't take effect here since exceptions happen in the function checker which is in transformation
        //logExceptions(s);
        logToFile(s);
        e.printStackTrace();
      }

      println(0, "####### Tree #########");
      printTree(op, 0);

      //println(0, "####### Op relations #########");

      /*
      println(0, "####### Start Test #########");
      new AbmTestCases(sinkOp).test(sinkOp, 0);
      println(0, "####### Test Passed #########");
       */
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Deprecated
  public static void analyzeHelper(Operator<? extends OperatorDesc> sinkOp, int level) {

    println(level, sinkOp.getClass() + " " + sinkOp.toString());
    println(level, "Column Expr Map: ");

    Map<String, ExprNodeDesc> map = sinkOp.getColumnExprMap();
    if (map != null && map.entrySet() != null) {
      for (Entry<String, ExprNodeDesc> entry: map.entrySet()) {
        if (entry.getValue() instanceof ExprNodeColumnDesc) {
          //ExprNodeColumnDesc expr = (ExprNodeColumnDesc) entry.getValue();
          //String[] names = expr.getColumn().split("\\.");
          //println(level, "@@@"+ expr.getColumn()+ "@@@" + Arrays.asList(names) +"@@@@");

          println(level, entry.getKey() + ": "
              + ((ExprNodeColumnDesc)entry.getValue()).getTabAlias()
              + ((ExprNodeColumnDesc)entry.getValue()).getCols());
        } else if (entry.getValue() instanceof ExprNodeConstantDesc) {
          println(level, entry.getKey() + ":: "
              + ((ExprNodeConstantDesc)entry.getValue()).getExprString());
          //+ ((ExprNodeConstantDesc)entry.getValue()).getCols());
        } else {
          println(level, entry.getValue().getExprString());
          //throw new RuntimeException("ExprNode Type does not supported!");
        }
      }
    }

    println(level, "Schema: ");
    RowSchema schema = sinkOp.getSchema();
    if (schema != null) {
      if (schema.getSignature() != null) {
        for (ColumnInfo info: schema.getSignature()) {
          println(level, info.getTabAlias() + "[" + info.getInternalName() + "]");
        }
      }
    }

    if (sinkOp instanceof TableScanOperator) {
      //println("=========== " + opParseCtx.get(sinkOp).getRowResolver().tableOriginalName);

      //println("========= " + ((TableScanOperator)(sinkOp)).getNeededColumnIDs());
      //println("========= " + ((TableScanOperator)(sinkOp)).getNeededColumns());
      //println("======Table Desc " + ((TableScanOperator)(sinkOp)).getTableDesc());
      //println(qb.getTabNameForAlias("a"));
      //println(qb.getTabNameForAlias("b"));

      //TableScanDesc desc = ((TableScanOperator)sinkOp).getConf();
      //println(level, desc.getAlias());

      //println(level, desc.getFilterExpr());
      //println(level, desc.getBucketFileNameMapping());
      //println(level, desc.getVirtualCols());
      //println(level, desc.getPartColumns());
    }


    if (sinkOp instanceof JoinOperator) {

      JoinDesc desc = ((JoinOperator)sinkOp).getConf();

      List<String> outputNames = desc.getOutputColumnNames();
      println(level, "outputColumnNames size: " + outputNames.size());
      println(level, "Exprs: " + desc.getExprs());
      println(level, "Exprs String Map: " + desc.getExprsStringMap());
      for (String name: outputNames) {
        println(level, name);
      }

      //println(level, ((JoinOperator) sinkOp).getPosToAliasMap());
      //println(level, "Reversed Mapping: " + ((JoinOperator)sinkOp).getConf().getReversedExprs());

      /*
      println(level, "JOIN");

      for (List<ExprNodeDesc> lst : ((JoinOperator)sinkOp).getConf().getExprs().values()) {
        printLevel(level);
        for (ExprNodeDesc desc: lst) {
          print(((ExprNodeColumnDesc)desc).getTabAlias() + " " + ((ExprNodeColumnDesc)desc).getCols());
        }
        println();
      }

      //for filters
      Map<Byte, List<ExprNodeDesc>> filterMap = ((JoinOperator)sinkOp).getConf().getFilters();

      //if (filterMap != null && filterMap.size() != 0) {
      //print("HAS JOIN FILTERS");
      //}

      for (List<ExprNodeDesc> lst : ((JoinOperator)sinkOp).getConf().getFilters().values()) {
        printLevel(level);
        //print(((JoinOperator)sinkOp).getConf().getFilters());
        for (ExprNodeDesc desc: lst) {
          print(desc.getClass() + " ");
          if (desc instanceof ExprNodeGenericFuncDesc) {
            print(((ExprNodeGenericFuncDesc)desc));
          }
        }
        println();
      }

       */

      //println(level, "output");

      //println(level, ((JoinOperator)sinkOp).getConf().getOutputColumnNames());


      //println(level, ((JoinOperator)sinkOp).getConf().getExprsStringMap());
    }

    if (sinkOp instanceof ReduceSinkOperator) {
      //println(level, ((ReduceSinkOperator)sinkOp).getConf().getOutputKeyColumnNames());
      /*
      for (ExprNodeDesc desc: ((ReduceSinkOperator)sinkOp).getConf().getValueCols()) {
        println(level, ((ExprNodeColumnDesc)desc).getTabAlias() + " "
                + ((ExprNodeColumnDesc)desc).getCols());
      }
       */
      //ReduceSinkOperator op = (ReduceSinkOperator) sinkOp;
      //out.println("@@@@@@@@@@@@@" + op.toString() +  " " + op.getConf().getKeyCols().size() + " " + op.getConf().getValueCols().size());
      //out.println("@@@@@@@@@@@" + op.getConf().getNumDistributionKeys() + " " + op.getConf().getKeyCols().toString());
      //out.println("@@@@@@@@@@@" + op.getConf().getValueCols().toString());

    }

    if (sinkOp instanceof SelectOperator) {
      println(level, "Select " + ((SelectOperator)sinkOp).getConf().getColList());
      //println(level, "Select" + ((SelectOperator)sinkOp).getConf().getOutputColumnNames());
      /*
      for (ExprNodeDesc desc: ((SelectOperator)sinkOp).getConf().getColList()) {
        println(level, ((ExprNodeColumnDesc)desc).getTabAlias() + " "
                + ((ExprNodeColumnDesc)desc).getCols());
      }*/
      //println(level, ((SelectOperator)sinkOp).getConf().getColList());
      //println(level, ((SelectOperator)sinkOp).getConf().getOutputColumnNames());
    }

    if (sinkOp instanceof FilterOperator) {
      println(level, ((FilterOperator)sinkOp).getConf().getPredicate().getExprString());
      //ExprNodeDesc desc = ((FilterOperator)sinkOp).getConf().getPredicate();
      //(ExprNodeGenericFuncDesc)((FilterOperator)sinkOp).getConf().getPredicate()
      //println(level, ((ExprNodeGenericFuncDesc)desc).getExprString());
      //println(level, ((ExprNodeGenericFuncDesc)desc).getCols());
    }

    if (sinkOp instanceof LimitOperator) {
      println(level, ((LimitOperator)sinkOp).getConf().getClass());
      //ExprNodeDesc desc = ((FilterOperator)sinkOp).getConf().getPredicate();
      //(ExprNodeGenericFuncDesc)((FilterOperator)sinkOp).getConf().getPredicate()
      //println(level, ((ExprNodeGenericFuncDesc)desc).getExprString());
      //println(level, ((ExprNodeGenericFuncDesc)desc).getCols());
    }

    if (sinkOp instanceof GroupByOperator) {
      println(level, "Aggregators: ");
      ArrayList<AggregationDesc> aggrs =
          ((GroupByOperator)sinkOp).getConf().getAggregators();
      if (aggrs != null) {
        for (AggregationDesc desc : aggrs) {
          println(level, "isDistinct: " + " " + desc.getDistinct() + " ExprString: " + desc.getExprString());
        }
      }
    }

    List<Operator<? extends OperatorDesc>> lst = sinkOp.getParentOperators();
    if (lst != null) {
      for (Operator<? extends OperatorDesc> l: lst) {
        analyzeHelper(l, level + 1);
      }
    }

  }

  private static void println(int level, Object content) {
    for (int i=0; i< level; i++) {
      System.out.print("  ");
      if (needLogToFile) {
        logToFile("  ");
      }
    }
    System.out.println(content);
    if (needLogToFile) {
      logToFile(content + "\n");
    }
  }

  private static void println() {
    System.out.println();
    if (needLogToFile) {
      logToFile("\n");
    }
  }

  private static void printLevel(int level) {
    for (int i=0; i< level; i++) {
      System.out.print("  ");
      if (needLogToFile) {
        logToFile("  ");
      }
    }
  }

  private static void print(Object content) {
    System.out.print(content + " ");
    if (needLogToFile) {
      logToFile(content + " ");
    }
  }

  private static void logPlan(String msg) {
    File f = new File(jsonPlanFile);
    f.delete();
    try {
      f.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(jsonPlanFile, true)));
      out.print(msg);
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void logToFile(String msg) {
    try {
      if (AbmUtilities.inAbmMode()) {
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(planFile, true)));
        out.print(msg);
        out.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void logExceptions(String msg) {
    try {
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(exceptionFile, true)));
      if (AbmUtilities.inAbmMode()) {
        out.println("q" + AbmUtilities.getLabel() + ":");
      }
      out.println(msg);
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
