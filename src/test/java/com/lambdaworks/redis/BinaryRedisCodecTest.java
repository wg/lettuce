package com.lambdaworks.redis;

import org.junit.BeforeClass;
import org.junit.Test;

import com.lambdaworks.redis.codec.BinaryRedisCodec;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class BinaryRedisCodecTest extends AbstractCommandTest {
	static RedisConnection<byte[], byte[]> binaryRedis = null;
	
	@BeforeClass
	public static void setupBinaryClient(){
		binaryRedis = new RedisClient(host, port).connect(new BinaryRedisCodec());
	}
	
	@Test
    public void decodeHugeBuffer() throws Exception {
        byte[] huge = new byte[8192];
        Arrays.fill(huge, (byte)'A');
        binaryRedis.set(key.getBytes(), huge);
        assertTrue(Arrays.equals(huge, binaryRedis.get(key.getBytes())));
    }
}


