package cs555.dfs.erasure;

import erasure.ReedSolomon;

import java.io.*;

public class SolomonErasure {
	public static final int DATA_SHARDS = 6;
	public static final int PARITY_SHARDS = 3;
	public static final int TOTAL_SHARDS = 9;
	public static final int BYTES_IN_INT = 4;

	public static byte[][] encode(RandomAccessFile randomAccessFile, int size) throws IOException {
		int storedSize = size + BYTES_IN_INT;
		int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

		int bufferSize = shardSize * DATA_SHARDS;
		byte[] bytes = new byte[bufferSize];
		randomAccessFile.read(bytes,BYTES_IN_INT, size);

		byte[][] shards = new byte[TOTAL_SHARDS][shardSize];

		for(int i = 0; i < DATA_SHARDS; i++) {
			System.arraycopy(bytes, i * shardSize, shards[i], 0, shardSize);
		}

		ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
		reedSolomon.encodeParity(shards, 0, shardSize);

		return shards;
	}

	public static byte[] decode(byte[][] shards, boolean[] shardsExist, int numExistingShards) {
		if(numExistingShards < DATA_SHARDS) {
			System.err.println("Error: Not enough shards to reconstruct file");
			return null;
		}
		int shardSize = 0;
		for(int i = 0; i < TOTAL_SHARDS; i++) {
			if(shardsExist[i]) {
				shardSize = shards[i].length;
				break;
			}

		}
		for(int i = 0; i < 	TOTAL_SHARDS; i++) {
			if(!shardsExist[i]) {
				shards[i] = new byte[shardSize];
			}
		}

		ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
		reedSolomon.decodeMissing(shards, shardsExist, 0, shardSize);

		byte[] bytes = new byte[shardSize * DATA_SHARDS];
		for(int i = 0; i < DATA_SHARDS; i++) {
			System.arraycopy(shards[i], 0,bytes, shardSize * i, shardSize);
		}

		return bytes;
	}
}
