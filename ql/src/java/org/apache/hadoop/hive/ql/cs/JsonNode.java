package org.apache.hadoop.hive.ql.cs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonNode {
  //private JsonNode parent;
  private final Operator<? extends OperatorDesc> op;
  private final List<JsonNode> children;
  private final JSONObject json;

  public JsonNode(Operator<? extends OperatorDesc> op) {
    super();
    //this.parent = parent;
    this.children = new ArrayList<JsonNode>();
    this.json = new JSONObject();
    this.op = op;

    try {
      json.put("name", op.toString());
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