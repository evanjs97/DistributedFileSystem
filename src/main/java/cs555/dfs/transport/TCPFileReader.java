package cs555.dfs.transport;

import cs555.dfs.util.ChunkUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TCPFileReader {

	private ArrayBlockingQueue<byte[]> chunks = new ArrayBlockingQueue<>(1000);

	private final String filename;
	private final RandomAccessFile file;
	private final long numChunks;

	public TCPFileReader(String filename, long numChunks, String destination) throws FileNotFoundException {
		this.numChunks = numChunks;
		if(filename.contains("/")) filename = filename.substring(filename.lastIndexOf("/")+1);
		this.filename = filename;
		if(destination.charAt(destination.length()-1) != '/') destination = destination + "/";
		Path dir = Paths.get(destination);
		try {
			Files.createDirectories(dir);
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}

		this.file = new RandomAccessFile(destination+filename, "rw");
	}

	public void addFileBytes(byte[] bytes) {
		System.out.println("Adding Bytes");
		chunks.add(bytes);
	}

	public void readFile() {
		try {
			for(long i = 0; i < numChunks; i++) {
				try {
					byte[] chunk = chunks.poll(10000, TimeUnit.MILLISECONDS);
					file.write(chunk);
					System.out.println("Finished Writing Chunk");
				}catch(InterruptedException ie) {
					ie.printStackTrace();
				}
			}
			file.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}

	}


}
