package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;


public class ConditionList {

  private final int dimension = 0;
  private final List<List<Condition>> conditions;

  public static ObjectInspector arrayObjectInspectorType =  ObjectInspectorFactory.getStandardListObjectInspector(
      ObjectInspectorFactory.getStandardStructObjectInspector(Condition.columnName, Condition.objectInspectorType));

  public static ObjectInspector objectInspectorType = ObjectInspectorFactory.getStandardListObjectInspector(arrayObjectInspectorType);


  public ConditionList()
  {
    conditions = new ArrayList<List<Condition>>();
  }

  public ConditionList(int d)
  {
    // this.dimension = d;
    conditions = new ArrayList<List<Condition>>();
    for(int i = 0; i < d; i ++) {
      conditions.add(new ArrayList<Condition>());
    }
  }

  public ConditionList(Condition cond)
  {
    conditions = new ArrayList<List<Condition>>();
    joinCondition(cond);
  }

  public void clear()
  {
    this.conditions.clear();
  }

  public void print()
  {
    System.out.println("Condition List: " + this.dimension);
    for(List<Condition> conditionList:this.conditions)
    {
      for(Condition con: conditionList)
      {
        System.out.print(con.toString() + "\t");
      }
      System.out.println();
    }
  }

  public void update(Object matrixObj)
  {
    clear();

    //
    int colSize = ((ListObjectInspector)objectInspectorType).getListLength(matrixObj);
    for(int i = 0; i <  colSize; i ++)
    {
      Object arrayObj = ((ListObjectInspector)objectInspectorType).getListElement(matrixObj, i);
      List<Condition> conds = new ArrayList<Condition>();
      int rowSize = ((ListObjectInspector)arrayObjectInspectorType).getListLength(arrayObj);

      for(int j = 0; j < rowSize; j ++) {
        conds.add(new Condition(((ListObjectInspector)arrayObjectInspectorType).getListElement(arrayObj, j)));
      }

      this.conditions.add(conds);
    }
  }

  public static void update(Object condListObj, Object condObj)
  {
    Object[] objMatrix = (Object[]) condListObj;
    Object[] objArray = (Object[]) objMatrix[0];
    objArray[0] = condObj;
  }

  public static void update(Object condListObj, Object condObj, int matrixSize)
  {
    Object[] objMatrix = (Object[]) condListObj;
    Object[] objArray = (Object[]) objMatrix[0];
    objArray[0] = condObj;
  }

  public static int[] findPartitionWithStart(List<Condition> conds, int pointer, int id)
  {
    int[] partition = new int[2];
    partition[0] = pointer;

    for(partition[1] = partition[0]; partition[1] < conds.size(); partition[1] ++)
    {
      if(conds.get(partition[1]).getID() != id)
      {
        partition[1] -= 1;
        break;
      }
    }

    return partition;
  }

  public int getPartition(int id, int array)
  {
//    // if idIndex contains this id
//    if(this.idIndex.size() > array)
//      if(this.idIndex.get(array).containsKey(id))
//        return this.idIndex.get(array).get(id);

    // otherwise, we need to find it and put it into idex
    for(Condition cond: this.conditions.get(array))
    {
      if(cond.getID() == id) {
        return cond.getID();
      }
    }

    System.out.println("Error in ConditionList: can't find this id in current array!");
    return -1;
  }

  public void addCondition(int col, Condition cond)
  {
    this.conditions.get(col).add(cond);
  }

  public void joinCondition(Condition cond)
  {
    List<Condition> conds = new ArrayList<Condition>();
    conds.add(cond);
    this.conditions.add(conds);
  }

  public Object toArray()
  {
    Object[][] res = new Object[this.conditions.size()][];
    for(int i = 0; i < this.conditions.size(); i ++)
    {
      res[i] = new Object[this.conditions.get(i).size()];
      for(int j = 0; j < this.conditions.get(i).size(); j ++) {
        res[i][j] = this.conditions.get(i).get(j).toArray();
      }
    }
    return res;
  }

//  public Object toArray()
//  {
//    Object[] res = new Object[this.conditions.size()];
//    for(int i = 0; i < this.conditions.size(); i ++)
//    {
//      Object[] resi = new Object[this.conditions.get(i).size()];
//      for(int j = 0; j < this.conditions.get(i).size(); j ++)
//        resi[j] = this.conditions.get(i).get(j).toArray();
//      res[i] = resi;
//    }
//    return res;
//  }

  public int getColSize()
  {
    return this.conditions.size();
  }

  public int getRowSize()
  {
    if(getColSize() == 0) {
      return 0;
    } else
    {
      return this.conditions.get(0).size();
    }
  }

  public List<Condition> get(int i)
  {
    return this.conditions.get(i);
  }




}
