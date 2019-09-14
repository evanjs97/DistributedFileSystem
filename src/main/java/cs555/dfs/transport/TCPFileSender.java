package cs555.dfs.transport;

import cs555.dfs.messaging.ChunkWriteRequest;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.FileMetadata;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TCPFileSender {

//	private ArrayBlockingQueue<LinkedList<ChunkUtil>> availableLocations = new ArrayBlockingQueue<>(1000);
	private HashMap<ChunkUtil, TCPSender> senders = new HashMap<>();
	private final LinkedList<ChunkUtil>[] chunks;

//	private final String destination;
//	private final String filename;
//	private RandomAccessFile file;
//	private final long numChunks;
	private final AtomicInteger chunkCount;
//	private final int PRINT_INTERVAL;
	private final String filename;
	private final Instant lastModified;
	private final String destination;
	private final RandomAccessFile file;
	private final long fileSize;
	private final int numChunks;
	private final int bufferSize = 64 * 1024;


	public TCPFileSender(String filename, String destination) throws IOException{
		this.filename = filename;
		this.lastModified = FileMetadata.getLastModifiedTime(filename);

		file = new RandomAccessFile(filename, "r");
		fileSize = file.length();
		long temp = fileSize / bufferSize;
		if(fileSize % bufferSize != 0) temp++;
		numChunks = (int)temp;

		this.destination = destination;
		this.chunks = new LinkedList[numChunks];
		chunkCount = new AtomicInteger(0);
	}

	public long getNumChunks() {
		return this.numChunks;
	}

	public void addLocationList(LinkedList<ChunkUtil> locations, int index) {
		synchronized (chunks) {
			chunks[index] = locations;
		}
		int count = chunkCount.incrementAndGet();
		if(count == this.numChunks) {
			sendFile();
		}
	}

	private void sendFile() {

		try {

			long bytesRemaining = fileSize;
			System.out.println("Sending " + numChunks + " chunks");
			System.out.println("Total file size: " + fileSize);
			for(int i = 0; i < numChunks; i++) {
				byte[] chunk;
				if(bytesRemaining >= bufferSize) {
					chunk = new byte[bufferSize];
				}else {
					chunk = new byte[(int) bytesRemaining];
				}

				LinkedList<ChunkUtil> locations = chunks[i];
				ChunkUtil dest = locations.pollFirst();

				senders.putIfAbsent(dest, new TCPSender(new Socket(dest.getHostname(), dest.getPort())));
				TCPSender sender = senders.get(dest);
				file.readFully(chunk);

				ChunkWriteRequest request = new ChunkWriteRequest(locations,this.destination+"_chunk_"+i, chunk, lastModified);
				sender.sendData(request.getBytes());
				sender.flush();
				bytesRemaining-=chunk.length;
			}
			System.out.println("Finished Sending File");
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
