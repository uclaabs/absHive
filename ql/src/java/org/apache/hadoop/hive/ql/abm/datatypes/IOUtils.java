package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class IOUtils {

  public static final int INT_SIZE = 4;
  public static final int DOUBLE_SIZE = 8;

  public static int[] deserializeIntArray(BytesInput in) throws IOException {
    int len = in.readInt();
    int[] buf = new int[len];
    for (int i = 0; i < len; ++i) {
      buf[i] = in.readInt();
    }
    return buf;
  }

  public static void serializeIntArray(int[] o, BytesOutput out) throws IOException {
    out.writeInt(o.length);
    for (int i = 0; i < o.length; ++i) {
      out.writeInt(o[i]);
    }
  }

  public static int estimateIntArray(int[] o) {
    return INT_SIZE * (o.length + 1);
  }

  public static void deserializeIntArrayListInto(BytesInput in, IntArrayList out) throws IOException {
    int len = in.readInt();
    out.ensureCapacity(len);
    for (int i = 0; i < len; ++i) {
      out.add(in.readInt());
    }
  }

  public static void serializeIntArrayList(IntArrayList o, BytesOutput out) throws IOException {
    int len = o.size();
    out.writeInt(len);
    for (int i = 0; i < len; ++i) {
      out.writeInt(o.getInt(i));
    }
  }

  public static int estimateIntArrayList(IntArrayList o) {
    return INT_SIZE * (o.size() + 1);
  }

  public static double[] deserializeDoubleArray(BytesInput in) throws IOException {
    int len = in.readInt();
    double[] buf = new double[len];
    for (int i = 0; i < len; ++i) {
      buf[i] = in.readDouble();
    }
    return buf;
  }

  public static void serializeDoubleArray(double[] o, BytesOutput out) throws IOException {
    out.writeInt(o.length);
    for (int i = 0; i < o.length; ++i) {
      out.writeDouble(o[i]);
    }
  }

  public static int estimateDoubleArray(double[] o) {
    return DOUBLE_SIZE * o.length + INT_SIZE;
  }

  public static void deserializeDoubleArrayListInto(BytesInput in, DoubleArrayList out) throws IOException {
    int len = in.readInt();
    out.ensureCapacity(len);
    for (int i = 0; i < len; ++i) {
      out.add(in.readDouble());
    }
  }

  public static void serializeDoubleArrayList(DoubleArrayList o, BytesOutput out) throws IOException {
    int len = o.size();
    out.writeInt(len);
    for (int i = 0; i < len; ++i) {
      out.writeDouble(o.getDouble(i));
    }
  }

  public static int estimateDoubleArrayList(DoubleArrayList o) {
    return DOUBLE_SIZE * o.size() + INT_SIZE;
  }

  public static EWAHCompressedBitmap deserialzieBitmap(BytesInput in) throws IOException {
    EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
    bitmap.readExternal(in);
    return bitmap;
  }

  public static void serializeBitmap(EWAHCompressedBitmap o, BytesOutput out) throws IOException {
    o.writeExternal(out);
  }

  public static int estimateBitmap(EWAHCompressedBitmap o) {
    return o.sizeInBytes() + INT_SIZE * 4;
  }

}
