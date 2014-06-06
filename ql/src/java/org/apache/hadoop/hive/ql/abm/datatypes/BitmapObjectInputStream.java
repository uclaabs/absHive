package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;
import java.io.ObjectInput;

public class BitmapObjectInputStream implements ObjectInput {
  byte[] buffer = null;
  int cursor = 0;
  
  public BitmapObjectInputStream(byte[] bytes) {
    buffer = bytes;
    cursor = 0;
  }
  
  @Override
  public int readInt() throws IOException {
    int value = buffer[cursor] << 24;
    value ^= (buffer[cursor + 1] & 0xFF) << 16;
    value ^= (buffer[cursor + 2] & 0xFF) << 8;
    value ^= (buffer[cursor + 3] & 0xFF);
    cursor += 4;
    return value;
  }

  @Override
  public long readLong() throws IOException {    
    long value = buffer[cursor] << 56;
    for(int i = 1; i < 8; i ++) {
      value ^= (buffer[cursor + i] & 0xFF) << (8 * (7 - i));
    }
    cursor += 8;
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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

}
