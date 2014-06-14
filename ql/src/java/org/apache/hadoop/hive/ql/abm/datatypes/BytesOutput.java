package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;
import java.io.ObjectOutput;

public class BytesOutput implements ObjectOutput {

  private byte[] buffer = null;
  private int cursor = 0;

  public BytesOutput() {
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public void setBuffer(byte[] buf) {
    buffer = buf;
    cursor = 0;
  }

  @Override
  public void writeInt(int arg0) throws IOException {
    buffer[cursor++] = (byte) (arg0 >> 24);
    buffer[cursor++] = (byte) (arg0 >> 16);
    buffer[cursor++] = (byte) (arg0 >> 8);
    buffer[cursor++] = (byte) (arg0);
  }

  @Override
  public void writeLong(long arg0) throws IOException {
    buffer[cursor++] = (byte) (arg0 >> 56);
    buffer[cursor++] = (byte) (arg0 >> 48);
    buffer[cursor++] = (byte) (arg0 >> 40);
    buffer[cursor++] = (byte) (arg0 >> 32);
    buffer[cursor++] = (byte) (arg0 >> 24);
    buffer[cursor++] = (byte) (arg0 >> 16);
    buffer[cursor++] = (byte) (arg0 >> 8);
    buffer[cursor++] = (byte) (arg0);
  }

  @Override
  public void write(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void write(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void writeObject(Object arg0) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void writeByte(int arg0) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void writeBytes(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeDouble(double arg0) throws IOException {
    long value = Double.doubleToRawLongBits(arg0);
    buffer[cursor++] = (byte) (value >> 56);
    buffer[cursor++] = (byte) (value >> 48);
    buffer[cursor++] = (byte) (value >> 40);
    buffer[cursor++] = (byte) (value >> 32);
    buffer[cursor++] = (byte) (value >> 24);
    buffer[cursor++] = (byte) (value >> 16);
    buffer[cursor++] = (byte) (value >> 8);
    buffer[cursor++] = (byte) (value);
  }

  @Override
  public void writeFloat(float arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeShort(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeUTF(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChar(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChars(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeBoolean(boolean arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

}
