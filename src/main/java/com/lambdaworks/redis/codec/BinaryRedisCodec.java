package com.lambdaworks.redis.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 *  A {@link RedisCodec} that handles binary keys and values.
 *  This is useful if you are storing non-UTF8 data in Redis
 *  such as serialized objects or multimedia content.
 *  
 * @author Simone Scarduzio
 */
public class BinaryRedisCodec extends RedisCodec<byte[], byte[]> {

    @Override
    public byte[] decodeKey(ByteBuffer bytes) {
        return decode(bytes);
    }

	@Override
    public byte[] decodeValue(ByteBuffer bytes) {
       return decode(bytes);
    }

    @Override
    public byte[] encodeKey(byte[] key) {
        return key;
    }

    @Override
    public byte[] encodeValue(byte[]  value) {
    	return encode(value);
    }
    
    private byte[] encode(byte[] value) {
    	   try {
               if(value instanceof byte[]){
               	return (byte[])value;
               }
               else {
               	throw new IOException("byte[] value was expected");
               }
           } catch (IOException e) {
           	e.printStackTrace();
               return null;
           }
    }

    private byte[] decode(ByteBuffer bytes) {
   	 try {
            byte[] ba = new byte[bytes.remaining()];
            bytes.get(ba);
            return ba;
        } catch (Exception e) {
            return null;
        }
    }
}