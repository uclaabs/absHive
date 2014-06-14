package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;
import java.io.ObjectInput;

public class BytesInput implements ObjectInput {

  private byte[] buffer = null;
  private int cursor = 0;

  public BytesInput() {
  }

  public BytesInput(byte[] bytes) {
    buffer = bytes;
    cursor = 0;
  }

  public void setBuffer(byte[] buf) {
    buffer = buf;
    cursor = 0;
  }

  @Override
  public int readInt() throws IOException {
    int value = (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    return value;
  }

  @Override
  public long readLong() throws IOException {
    long value = (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    return value;
  }

  @Override
  public boolean readBoolean() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte readByte() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public char readChar() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public double readDouble() throws IOException {
    long value = (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    return Double.longBitsToDouble(value);
  }

  @Override
  public float readFloat() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFully(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void readFully(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public short readShort() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int skipBytes(int arg0) throws IOException {
    cursor += arg0;
    return arg0;
  }

  @Override
  public int available() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object readObject() throws ClassNotFoundException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long skip(long arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void rewind() {
    cursor = 0;
  }

}
