package cs555.dfs.erasure;

import erasure.ReedSolomon;

import java.io.*;

public class SolomonErasure {
	public static final int DATA_SHARDS = 6;
	public static final int PARITY_SHARDS = 3;
	public static final int TOTAL_SHARDS = 9;
//	public static int BYTES_INT_INT = 4;
	public static final int BASE_CHUNK_SIZE = 1024 * 64;

	public static final int SHARD_CHUNK_SIZE = BASE_CHUNK_SIZE + (DATA_SHARDS-BASE_CHUNK_SIZE % DATA_SHARDS);
	public static final int SHARD_SIZE = SHARD_CHUNK_SIZE / DATA_SHARDS;

	public static byte[][] encode(RandomAccessFile randomAccessFile, int size) throws IOException {

		int bufferSize = SHARD_SIZE * DATA_SHARDS;
		byte[] bytes = new byte[bufferSize];
		randomAccessFile.read(bytes,0, size);

		byte[][] shards = new byte[TOTAL_SHARDS][SHARD_SIZE];

		for(int i = 0; i < DATA_SHARDS; i++) {
			System.arraycopy(bytes, i * SHARD_SIZE, shards[i], 0, SHARD_SIZE);
		}

		ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
		reedSolomon.encodeParity(shards, 0, SHARD_SIZE);

		return shards;
	}

	public static byte[] decode(byte[][] shards, boolean[] shardsExist, int numExistingShards, int[] shardSize) {
		if(numExistingShards < DATA_SHARDS) {
			System.err.println("Error: Not enough shards to reconstruct file");
			return null;
		}

		int totalSize = 0;
		for(int i = 0; i < 	TOTAL_SHARDS; i++) {
			if(!shardsExist[i]) {
				shards[i] = new byte[SHARD_SIZE];
			}else {
				totalSize = shardSize[i];
			}
		}
		ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
		reedSolomon.decodeMissing(shards, shardsExist, 0, SHARD_SIZE);

		byte[] bytes = new byte[totalSize];
		int remaining = totalSize;
		for(int i = 0; i < DATA_SHARDS; i++) {
			int curSize = SHARD_SIZE;
			if(curSize > remaining) curSize = remaining;
			System.arraycopy(shards[i], 0,bytes, SHARD_SIZE * i, curSize);
			remaining-=curSize;
			if(remaining == 0) break;
		}

		return bytes;
	}
}
