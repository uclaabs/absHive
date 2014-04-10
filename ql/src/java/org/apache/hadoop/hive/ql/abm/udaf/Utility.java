package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class Utility {

  class Record {
    Integer id;
    Int2ObjectOpenHashMap<DoubleArrayList> columnMap;
    List<EWAHCompressedBitmap> diffList;

    public Record(Integer id, EWAHCompressedBitmap keys, List<DoubleArrayList> values,
        List<EWAHCompressedBitmap> diffList) {
      this.id = id;
      this.diffList = diffList;
      columnMap = LinMergeUtil.convertToHashMap(keys, values);
    }
  }

  private final Int2ObjectOpenHashMap<Record> map = new Int2ObjectOpenHashMap<Record>();

  private final StructObjectInspector inspector;

  /**
   * Use the default object inspector
   */
  public Utility() {
    inspector = LinMergeUtil.getOutputOI();
  }

  private int getRealId(int fakeId) {
    return fakeId & 0xffffff00;
  }

  private int getOffset(int fakeId) {
    return fakeId & 0xff;
  }

  /**
   * Each time an object comes in, we add it to the map
   * @param obj
   */
  public void deliver(Object[] obj) {
    if (obj.length != 4) {
      throw new RuntimeException("[Utility] input object size should be 4!");
    }

    Integer id = LinMergeUtil.getIdFrom(obj, inspector);
    map.put(id, new Record(id,
        LinMergeUtil.getKeysFrom(obj, inspector),
        LinMergeUtil.getValuesFrom(obj, inspector),
        LinMergeUtil.getDiffsFrom(obj, inspector)));
  }

  public boolean isKeyExist(int lineageId, int key) {
    int id = getRealId(lineageId);
    int offset = getOffset(lineageId);

    Record record = map.get(id);
    if (record == null) {
      return false;
    }

    //record.diffList.get(index);
    return false;
  }

  public double getColumnValue(int lineageId, int key, int columnIndex) {
    if (!isKeyExist(lineageId, key)) {
      throw new RuntimeException("[Utility] key does not exist!");
    }

    int id = getRealId(lineageId);
    //int offset = getOffset(lineageId);

    Record record = map.get(id);
    double value = -1;
    try {
      value = record.columnMap.get(key).get(columnIndex);
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    return value;
  }

}