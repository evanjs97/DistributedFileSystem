package cs555.dfs.transport;

import cs555.dfs.util.FileMetadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TCPFileReader {

//	private ArrayBlockingQueue<byte[]> chunks = new ArrayBlockingQueue<>(1000);
	private final byte[][] chunks;

	private final String filename;
	private final RandomAccessFile file;
	private final long numChunks;
	private final AtomicInteger chunkCount;

	public TCPFileReader(String filename, long numChunks, String destination) throws FileNotFoundException {
		this.numChunks = numChunks;
		if(filename.contains("/")) filename = filename.substring(filename.lastIndexOf("/")+1);
		this.filename = filename;
		this.chunks = new byte[(int)numChunks][];
		this.chunkCount = new AtomicInteger(0);

		if(destination.charAt(destination.length()-1) != '/') destination = destination + "/";
		Path dir = Paths.get(destination);
		try {
			Files.createDirectories(dir);
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}

		this.file = new RandomAccessFile(destination+filename, "rw");

	}

	public void addFileBytes(byte[] bytes, int index) {
//		if(bytes != null) {
//			chunks.add(bytes);
//		}else {
//			chunks.add(new byte[0]);
//		}
		int count = chunkCount.incrementAndGet();
		synchronized (chunks) {
			chunks[index] = bytes;
		}
//		int count = chunkCount.incrementAndGet();
		if(count == this.numChunks) {
			readFile();
		}

	}

	private void readFile() {
		try {
			for(int i = 0; i < numChunks; i++) {
				file.write(chunks[i]);
			}
			file.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}


}
