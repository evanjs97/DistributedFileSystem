package cs555.dfs.transport;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class TCPFileReader {

	private final byte[][] chunks;

	private final String destination;
	private final String filename;
	private RandomAccessFile file;
	private final long numChunks;
	private final AtomicInteger chunkCount;
	private final int PRINT_INTERVAL;

	public TCPFileReader(String filename, long numChunks, String destination) throws FileNotFoundException {
		this.numChunks = numChunks;
		if(filename.contains("/")) filename = filename.substring(filename.lastIndexOf("/")+1);
		this.filename = filename;
		this.chunks = new byte[(int)numChunks][];
		this.chunkCount = new AtomicInteger(0);
		if(destination.charAt(destination.length()-1) != '/') destination = destination + "/";
		this.destination = destination;
		this.PRINT_INTERVAL = (int) numChunks / 10;
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

	public void addFileBytes(byte[] bytes, int index) {
		synchronized (chunks) {
			chunks[index] = bytes;
		}
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

	public void close() {
		try {
			file.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void readFile() {
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
