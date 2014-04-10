package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LinMergeUtil {

  public static final String ID = "id";
  public static final String KEY = "keys";
  public static final String VALUE = "values";
  public static final String DIFF = "diffs";

  public static StructObjectInspector getOutputOI() {
    String[] finalCols = {LinMergeUtil.ID, LinMergeUtil.KEY, LinMergeUtil.VALUE, LinMergeUtil.DIFF};
    ObjectInspector[] finalInspectors = {
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
        ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)),
        ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector)};
    return ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList(finalCols), Arrays.asList(finalInspectors));
  }

  public static StructObjectInspector getPartialOutputOI() {
    String[] interCols = {LinMergeUtil.KEY, LinMergeUtil.VALUE, LinMergeUtil.DIFF};
    ObjectInspector[] interInspectors = {
        PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
        ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)),
        ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector)};
    return ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList(interCols) , Arrays.asList(interInspectors));
  }

  public static Int2ObjectOpenHashMap<DoubleArrayList> convertToHashMap(EWAHCompressedBitmap keys, List<DoubleArrayList> values) {
    Int2ObjectOpenHashMap<DoubleArrayList> map = new Int2ObjectOpenHashMap<DoubleArrayList>();
    Iterator<Integer> keyIter = keys.iterator();
    Iterator<DoubleArrayList> valueIter = values.iterator();

    while (keyIter.hasNext()) {
      map.put(keyIter.next(), valueIter.next());
    }

    return map;
  }

  /**
   * convert intArraylist to bitmap and serialize the bitmap to an object
   * @return
   */
  public static Object getIntArrayListObject(IntArrayList arr) {
//  EWAHCompressedBitmap bitmap = EWAHCompressedBitmap.bitmapOf(this.keys.toIntArray());
    EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
    for(int i = 0; i < arr.size(); i ++) {
      bitmap.set(arr.get(i));
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oo;
    try {
      oo = new ObjectOutputStream(baos);
      bitmap.writeExternal(oo);
      oo.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Object ret = ObjectInspectorUtils.copyToStandardObject(baos.toByteArray(), PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    return ret;
  }

  /**
   * id in the format of Integer
   * @param partialRes
   * @param inspector
   * @return
   */
  public static Integer getIdFrom(Object partialRes, StructObjectInspector inspector) {
    Integer partialId = ((IntWritable) (inspector.getStructFieldData(partialRes, inspector.getStructFieldRef(ID)))).get();
    return partialId;
  }

  /**
   * key in the format of bitmap
   * @param partialRes
   * @param inspector
   * @return
   */
  public static EWAHCompressedBitmap getKeysFrom(Object partialRes, StructObjectInspector inspector) {
    byte[] partialKeys = ((BytesWritable) (inspector.getStructFieldData(partialRes, inspector.getStructFieldRef(KEY)))).getBytes();

    EWAHCompressedBitmap bitMap = new EWAHCompressedBitmap();
    ByteArrayInputStream bis = new ByteArrayInputStream(partialKeys);

    try {
      bitMap.readExternal(new ObjectInputStream(bis));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return bitMap;
  }

  /**
   * value in the format of List<DoubleArrayList>
   * @param partialRes
   * @param inspector
   * @return
   */
  public static List<DoubleArrayList> getValuesFrom(Object partialRes, StructObjectInspector inspector) {
    LazyBinaryArray partialValues = (LazyBinaryArray) inspector.getStructFieldData(partialRes, inspector.getStructFieldRef(VALUE));

    List<DoubleArrayList> res = new ArrayList<DoubleArrayList>();

    for(int i = 0; i < partialValues.getListLength(); i++) {
      LazyBinaryArray doubleListLazyBinaryArray = (LazyBinaryArray) partialValues.getListElementObject(i);
      DoubleArrayList arrayList = new DoubleArrayList();

      for(int j = 0; j < doubleListLazyBinaryArray.getListLength(); j++) {
        arrayList.add(((DoubleWritable) doubleListLazyBinaryArray.getListElementObject(j)).get());
      }
      res.add(arrayList);
    }

    return res;
  }

  /**
   * diff in the format of List<bitmap>
   * @param partialRes
   * @param inspector
   * @return
   */
  public static List<EWAHCompressedBitmap> getDiffsFrom(Object partialRes, StructObjectInspector inspector) {

    LazyBinaryArray partialDiffs = (LazyBinaryArray) inspector.getStructFieldData(partialRes, inspector.getStructFieldRef(DIFF));

    List<EWAHCompressedBitmap> res = new ArrayList<EWAHCompressedBitmap>();

    for(int i = 0; i < partialDiffs.getListLength(); i++) {
      byte[] bytes = ((BytesWritable) partialDiffs.getListElementObject(i)).getBytes();
      EWAHCompressedBitmap bitMap = new EWAHCompressedBitmap();
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

      try {
        bitMap.readExternal(new ObjectInputStream(bis));
      } catch (IOException e) {
        e.printStackTrace();
      }

      res.add(bitMap);
    }

    return res;
  }

}