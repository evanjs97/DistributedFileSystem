package cs555.dfs.transport;


import cs555.dfs.erasure.SolomonErasure;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class TCPFileReader {

	private final byte[][] chunks;
	private final byte[][][] shards;
	private final int[] numShards;
	private final boolean[][] shardsExist;

	private final String destination;
	private final String filename;
	private RandomAccessFile file;
	private final long numChunks;
	private final AtomicInteger chunkCount;
	private final int PRINT_INTERVAL;

	public TCPFileReader(String filename, long numChunks, String destination, boolean replication) throws FileNotFoundException {
		this.numChunks = numChunks;
		if(filename.contains("/")) filename = filename.substring(filename.lastIndexOf("/")+1);
		this.filename = filename;
		this.chunks = new byte[(int)numChunks][];
		this.chunkCount = new AtomicInteger(0);
		if(destination.charAt(destination.length()-1) != '/') destination = destination + "/";
		this.destination = destination;
		this.PRINT_INTERVAL = (int) numChunks / 10;
		if(replication) {
			shards = null;
			numShards = null;
			shardsExist = null;
		}
		else {
			shards = new byte[(int)numChunks][SolomonErasure.TOTAL_SHARDS][];
			shardsExist = new boolean[(int)numChunks][SolomonErasure.TOTAL_SHARDS];
			numShards = new int[(int)numChunks];
		}

	}

	private void setupDirectory() {

		Path dir = Paths.get(destination);
		try {
			Files.createDirectories(dir);
			this.file = new RandomAccessFile(destination+filename, "rw");
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}


	}

	public synchronized void addFileBytes(byte[] bytes, int index) {
		chunks[index] = bytes;
		int count = chunkCount.incrementAndGet();
		if(count % PRINT_INTERVAL == 0) {
			System.out.print("---");
		}
		if(count == this.numChunks) {
			System.out.print(">\n");
			setupDirectory();
			readFile();
		}
	}

	public synchronized void addShardBytes(byte[] bytes, int shard, int chunkIndex) {
		if(numShards[chunkIndex] == -1) {
			return;
		}
		shards[chunkIndex][shard] = bytes;
		shardsExist[chunkIndex][shard] = true;
		numShards[chunkIndex]++;
		if(numShards[chunkIndex] > SolomonErasure.DATA_SHARDS) {
			byte[] decoded = SolomonErasure.decode(shards[chunkIndex], shardsExist[chunkIndex], numShards[chunkIndex]);
			numShards[chunkIndex] = -1;
			addFileBytes(decoded, chunkIndex);
		}

	}

	public void close() {
		try {
			file.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private synchronized void readFile() {
		try {
			for(int i = 0; i < numChunks; i++) {
				file.write(chunks[i]);
			}
			file.close();
			System.out.println("File downloaded");
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}


}
