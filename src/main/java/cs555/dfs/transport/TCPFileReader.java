package cs555.dfs.transport;

import cs555.dfs.util.ChunkUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class TCPFileReader {

	private ArrayBlockingQueue<byte[]> chunks = new ArrayBlockingQueue<>(1000);

	private final String filename;
	private final RandomAccessFile file;
	private long bytesRemaining;
	private final long numChunks;

	public TCPFileReader(String filename, long numChunks, String destination) throws FileNotFoundException {
		this.numChunks = numChunks;
		if(filename.contains("/")) filename = filename.substring(filename.lastIndexOf("/")+1);
		this.filename = filename;
//		if(destination.charAt(destination.length()-1) != '/') destination = destination + "/";
		File f = new File(destination);
		f.mkdirs();
//		this.filename =

		this.file = new RandomAccessFile(destination+filename, "rw");
	}

	public void addFileBytes(byte[] bytes) {
		chunks.add(bytes);
	}

	public void readFile() {
		try {
			for(long i = 0; i < numChunks; i++) {
				byte[] chunk = chunks.poll();
				file.write(chunk);
			}
			file.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}

	}


}
