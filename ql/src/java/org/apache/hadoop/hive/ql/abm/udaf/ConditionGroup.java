package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.Condition;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionList;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class ConditionGroup {

  private int groupID;
  private int dimension;
  private final List<List<Condition>> conditions;
  private List<Merge> merges;
  private List<Integer> keys;

  public static List<String> columnName = Arrays.asList("GID", "DIM","CON");

  final static ObjectInspector[] objectInspector = {
    PrimitiveObjectInspectorFactory.javaIntObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector,
    ConditionList.objectInspectorType};

  public static List<ObjectInspector> objectInspectorType = Arrays.asList(objectInspector);

  public static ObjectInspector groupObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnName, objectInspectorType);

  public static ConditionGroup getConditionGroup(LazyBinaryStruct lazyObj)
  {
    ConditionGroup condGroup = null;
    int gid = ((IntWritable)lazyObj.getField(0)).get();
    int dim = ((IntWritable)lazyObj.getField(1)).get();
    LazyBinaryArray lazyMatrix = (LazyBinaryArray) lazyObj.getField(2);

    condGroup = new ConditionGroup(gid, dim);

//    System.out.println("Parse from LazyStruct " + gid + "\t" + dim);
//    condGroup.print();

    for(int i = 0; i < lazyMatrix.getListLength(); i ++)
    {
      List<Condition> newConds = new ArrayList<Condition>();
      LazyBinaryArray lazyArray = (LazyBinaryArray) lazyMatrix.getListElementObject(i);
      for(int j = 0; j < lazyArray.getListLength(); j ++)
      {
        LazyBinaryStruct condObj = (LazyBinaryStruct) lazyArray.getListElementObject(j);
        newConds.add(Condition.getCondition(condObj));
      }
      if(condGroup.dimension == 0) {
        condGroup.dimension = lazyArray.getListLength();
      }
      condGroup.conditions.add(newConds);
    }


    return condGroup;
  }

  public static ConditionGroup getConditionGroup(Object obj, int unused)
  {
    ConditionGroup condGroup = null;
    StructObjectInspector condGroupInspector = (StructObjectInspector)groupObjectInspector;
    IntObjectInspector intIO = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

    Object gidObj = condGroupInspector.getStructFieldData(obj, condGroupInspector.getStructFieldRef("GID"));
    Object dimObj = condGroupInspector.getStructFieldData(obj, condGroupInspector.getStructFieldRef("DIM"));
    Object conObj = condGroupInspector.getStructFieldData(obj, condGroupInspector.getStructFieldRef("CON"));

    int gid = intIO.get(gidObj);
    int dim = intIO.get(dimObj);

    condGroup = new ConditionGroup(gid, dim);

    int colSize = ((ListObjectInspector)ConditionList.objectInspectorType).getListLength(conObj);
    for(int i = 0; i <  colSize; i ++)
    {
      Object arrayObj = ((ListObjectInspector)ConditionList.objectInspectorType).getListElement(conObj, i);
      List<Condition> conds = new ArrayList<Condition>();
      int rowSize = ((ListObjectInspector)ConditionList.arrayObjectInspectorType).getListLength(arrayObj);

      for(int j = 0; j < rowSize; j ++) {
        conds.add(new Condition(((ListObjectInspector)ConditionList.arrayObjectInspectorType).getListElement(arrayObj, j)));
      }

      condGroup.add(conds);
    }

    return condGroup;
  }

  public void print()
  {
    System.out.println("Condition Group: " + this.dimension + "\t" + this.groupID);
    for(List<Condition> conditionList:this.conditions)
    {
      for(Condition con: conditionList)
      {
        System.out.print(con.toString() + "\t");
      }
      System.out.println();
    }
  }

  public ConditionGroup(int gid, int dim)
  {
    this.groupID = gid;
    this.dimension = dim;
    this.merges = null;
    this.conditions = new ArrayList<List<Condition>>();
  }

  public void setGroupID(int gid)
  {
    this.groupID = gid;
  }

  public void add(List<Condition> cond)
  {
    this.conditions.add(cond);
  }

  public void addAll(List<List<Condition>> conditions)
  {
    this.conditions.addAll(conditions);
  }

  public int getDimension()
  {
    return this.dimension;
  }

  public int getGroupID()
  {
    return this.groupID;
  }

  public Object toArray()
  {
    Object[] ret = new Object[3];
    ret[0] = this.groupID;
    ret[1] = this.dimension;

    System.out.println("ConditionGroup.ToArray: " + ret[0] + "\t" + ret[1] );

    Object[][] conds = new Object[this.conditions.size()][];
    for(int i = 0; i < this.conditions.size(); i ++)
    {
      conds[i] = new Object[this.conditions.get(i).size()];
      for(int j = 0; j < this.conditions.get(i).size(); j ++) {
        conds[i][j] = this.conditions.get(i).get(j).toArray();
      }
    }

    ret[2] = conds;

    return ret;
  }

  public List<List<Condition>> getConditions()
  {
    return this.conditions;
  }

  public void sort()
  {
    if(this.merges == null)
    {
      this.merges = new ArrayList<Merge>();
    }
    else
    {
      this.merges.clear();
    }

    // create a Merge class for every dimension
    for(int i = 0; i < this.dimension; i ++) {
      this.merges.add(new Merge(this.conditions.get(i).get(0).getFlag()));
    }

    // add the point to Merge class
    for(int i = 0; i < this.conditions.size(); i ++)
    {
      for(int j = 0; j < this.dimension; j ++)
      {
        this.merges.get(j).addPoint(this.conditions.get(i).get(j).getRange(), i);
      }
    }

    // sort each dimension
    for(int i = 0; i < this.dimension; i ++) {
      this.merges.get(i).sort();
    }

  }

  public void genConditions(List<Integer> inputKeys, ConditionList condList)
  {
    if(this.dimension == 0) {
      return;
    }

    IntArrayList lineage = new IntArrayList();
    this.keys = new ArrayList<Integer>(inputKeys);

    // start from the first dimension
    for(int i = 0; i < this.merges.get(0).intervalSize(); i ++)
    {
      lineage.clear();
      condList.addCondition(0, this.merges.get(0).getCondition(keys.get(0), i));
      this.merges.get(0).getLineage(lineage, i);

      if(this.dimension > 1)
      {
        int partitionNumber = genCondition(condList, 2, lineage);
        for(int j = 1; j < partitionNumber; j ++) {
          condList.addCondition(0, this.merges.get(0).getCondition(keys.get(0), i));
        }
      }
    }
  }

  /**
   *
   * @param indOfLastDim : variable i in last dimension
   * @param currentDim : current dimension
   * @param lineage : lineage from last dimension
   */
  public int genCondition(ConditionList condList, int currentDim, IntArrayList lineage)
  {
    /*
     * for debug
     * print out the currentDim and lineage
     */
    System.out.println("genCondition: " + currentDim);
    for(int i = 0; i < lineage.size(); i ++)
    {
       System.out.print(lineage.getInt(i) + "\t");
    }
    System.out.println();

    int partitionNumber = 0;
    int start = -1;
    Merge currentMerge = this.merges.get(currentDim - 1);
    IntArrayList matchedLineage = null;

    for(int i = 0; i < currentMerge.intervalSize(); i ++)
    {
      IntArrayList tmpLineage = currentMerge.matchLineage(lineage, i);

      if(tmpLineage.size() > 0)
      {
        if(start >= 0) //find a complete interval
        {
          condList.addCondition(currentDim - 1, currentMerge.getCondition(keys.get(currentDim - 1), start, i));
          // if there is further dimension, we should keep going down
          if(this.dimension > currentDim)
          {
            int tmpNumber = genCondition(condList, currentDim + 1, matchedLineage);
            for(int j = 1; j < tmpNumber; j ++) {
              condList.addCondition(currentDim - 1, currentMerge.getCondition(keys.get(currentDim - 1), start, i));
            }
          }
          start = i;
        } else {
          start = i;
        }

        matchedLineage = tmpLineage;
      }
    }

    if(start >= 0)
    {
      condList.addCondition(currentDim - 1, currentMerge.getCondition(keys.get(currentDim - 1), start, currentMerge.intervalSize()));
      // if there is further dimension, we should keep going down
      if(this.dimension > currentDim)
      {
        int tmpNumber = genCondition(condList, currentDim + 1, lineage);
        for(int j = 1; j < tmpNumber; j ++) {
          condList.addCondition(currentDim - 1, currentMerge.getCondition(keys.get(currentDim - 1), start, currentMerge.intervalSize()));
        }
      }
    }

    return partitionNumber;
  }






}

