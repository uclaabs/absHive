package org.apache.hadoop.hive.ql.cs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonNode {
  //private JsonNode parent;
  private final Operator<? extends OperatorDesc> op;
  private final List<JsonNode> children;
  private final JSONObject json;

  class SchemaHelper {
    List<ColumnInfo> columnInfos = null;
    StringBuilder sb = new StringBuilder();
    static final int interval = 3;
    static final String inlineSep = "=";

    public String process() {
      if ((columnInfos = op.getSchema().getSignature()) != null) {
        if (op instanceof GroupByOperator) {
          perform((GroupByOperator) op);
        } else if (op instanceof SelectOperator) {
          perform((SelectOperator) op);
        } else if (op instanceof TableScanOperator) {
          perform((TableScanOperator) op);
        }
        else if (op instanceof FilterOperator) {
          perform((FilterOperator) op);
        }
        else {
          perform(op);
        }
      }

      return sb.toString();
    }

    private boolean needNewLine(int i) {
      /*
      if (i % interval == interval - 1) {
        return true;
      }
      return false;*/
      return true;
    }

    private void perform(TableScanOperator tab) {
      perform((Operator<? extends OperatorDesc>) tab);
      String name = AbmTestHelper.pCtx.getTopToTable().get(op).getTableName();
      record("TableName: " + name + " isSampledTable: " + name.toLowerCase().equals(AbmTestHelper.getSampledTable()), true);
    }

    private void perform(FilterOperator fil) {
      record("Predicate: " + ((FilterOperator)op).getConf().getPredicate().getExprString(), true);
    }

    private void perform(GroupByOperator gby) {
      ArrayList<ExprNodeDesc> keys = gby.getConf().getKeys();
      ArrayList<AggregationDesc> aggrs = gby.getConf().getAggregators();

      boolean isDistinct = false;
      if (aggrs != null) {
        for (AggregationDesc desc : aggrs) {
          isDistinct = isDistinct || desc.getDistinct();
        }
      }

      if (isDistinct) {
        //println("Distinct as Group By");
        return;
      }

      record("Schema", true);
      //keys
      for (int i=0; i<keys.size(); i++) {
        record(columnInfos.get(i).getInternalName() + inlineSep + " " + keys.get(i).getExprString() + " (key)", needNewLine(i));
      }
      record("",true);

      //aggrs
      for (int i=keys.size(); i<columnInfos.size(); i++) {
        record(columnInfos.get(i).getInternalName() + inlineSep + " " + aggrs.get(i-keys.size()).getExprString(), needNewLine(i));
      }
      record("",true);
    }

    private void perform(SelectOperator sel) {
      record("Schema", true);
      int i = 0;
      for (ColumnInfo info: columnInfos) {
        record(info.getInternalName() + inlineSep + " " + sel.getConf().getColList().get(i).getExprString(), needNewLine(i));
        i += 1;
      }
      record("",true);
    }

    private void perform(Operator<? extends OperatorDesc> op) {
      record("Schema", true);
      int i = 0;
      for (ColumnInfo info: columnInfos) {
        record(info.getInternalName(), needNewLine(i));
        i += 1;
      }
      record("",true);
    }

    private void record(String s, boolean newLine) {
      sb.append(s);
      if (newLine) {
        sb.append(";");
      }
      else {
        sb.append(",  ");
      }
    }

  }

  private Boolean isAbmSelect(Operator<? extends OperatorDesc> op) {
    int id = Integer.parseInt(op.getIdentifier());
    if (id > AbmTestHelper.rootOpId && op.getName().equals("SEL")) {
      return true;
    } else {
      return false;
    }
  }

  private String isAbmOp() {
    int id = Integer.parseInt(op.getIdentifier());

    try {
      if (op.getName().equals("FIL")) {
        if (op.getChildOperators() != null) {
          return isAbmSelect(op.getChildOperators().get(0)) + "";
        }
      } else if (op.getName().equals("GBY")) {
        if (op.getChildOperators() != null) {
          //firstGby
          if (isAbmSelect(op.getChildOperators().get(0))) {
            return "true";
          }
          //secondGby
          else if (op.getChildOperators().get(0).getName().equals("RS")) {
            return isAbmSelect(op.getChildOperators().get(0).
                                  getChildOperators().get(0).
                                  getChildOperators().get(0)) + "";
          }
        }
      }
    }
    catch (NullPointerException e) {
      e.printStackTrace();
      return "false";
    }

    return isAbmSelect(op) + "";
  }

  private String expand(String s) {
    s = s.toLowerCase();

    if (s.startsWith("fs")) {
      return "FileSink";
    }
    else if (s.startsWith("sel")) {
      return "Select";
    }
    else if (s.startsWith("rs")) {
      return "ReduceSink";
    }
    else if (s.startsWith("fil")) {
      return "Filter";
    }
    else if (s.startsWith("ts")) {
      return "TableScan";
    }
    else if (s.startsWith("gby")) {
      return "GroupBy";
    }
    else if (s.startsWith("join")) {
      return "Join";
    }
    else if (s.startsWith("mapjoin")) {
      return "MapJoin";
    }

    return "";
  }

  public JsonNode(Operator<? extends OperatorDesc> op) {
    super();
    //this.parent = parent;
    this.children = new ArrayList<JsonNode>();
    this.json = new JSONObject();
    this.op = op;

    try {
      //json.put("name", op.toString());
      json.put("name", expand(op.toString()));
      json.put("val", new SchemaHelper().process());
      json.put("isAbmOp", isAbmOp());
      json.put("children", new JSONArray());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  /*
  public JsonNode getParent() {
    return parent;
  }

  public void setParent(JsonNode parent) {
    this.parent = parent;
  }*/

  /*
  public List<JsonNode> getChildren() {
    return children;
  }*/

  public void addChild(JsonNode child) {
    this.children.add(child);

    try {
      child.json.put("parent", this.op.toString());
      JSONArray childrenJson = (JSONArray) json.get("children");
      childrenJson.put(child.json);
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  public JSONObject getJSONObject() {
    return json;
  }

  public String getName() {
    return op.toString();
  }

  @Override
  public String toString() {
    String res = "";
    for (JsonNode child: children) {
      res += child.getName();
      res += ", ";
    }
    return op.toString() + ": " + res;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof JsonNode)) {
      return false;
    }
    JsonNode that = (JsonNode) obj;
    return this.op.equals(that.op);
  }
}