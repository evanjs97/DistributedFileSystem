package cs555.dfs.transport;

import cs555.dfs.erasure.SolomonErasure;
import cs555.dfs.messaging.ChunkWriteRequest;
import cs555.dfs.util.ChunkMetadata;
import cs555.dfs.util.ChunkUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
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
	private final int PRINT_INTERVAL;
	private boolean replication = true;



	public TCPFileSender(String filename, String destination, boolean replication) throws IOException{
		this.filename = filename;
		this.lastModified = ChunkMetadata.getLastModifiedTime(filename);

		file = new RandomAccessFile(filename, "r");
		fileSize = file.length();
		long temp = fileSize / bufferSize;
		if(fileSize % bufferSize != 0) temp++;
		numChunks = (int)temp;

		this.destination = destination;
		this.chunks = new LinkedList[numChunks];
		chunkCount = new AtomicInteger(0);
		this.PRINT_INTERVAL = numChunks / 10;
		this.replication = replication;
		System.out.println("Creating File Sender with Replication: " + replication);
	}

	public long getNumChunks() {
		return this.numChunks;
	}

	public void addLocationList(LinkedList<ChunkUtil> locations, int index) {
		synchronized (chunks) {
			chunks[index] = locations;
		}
		int count = chunkCount.incrementAndGet();
		if(count % PRINT_INTERVAL == 0) {
			System.out.print("---");
		}
		if(count == this.numChunks) {
			sendFile();
		}
	}

	private void sendChunk(int size, RandomAccessFile file, LinkedList<ChunkUtil> locations, int chunkNum) throws IOException{
		ChunkUtil dest = locations.pollFirst();

		senders.putIfAbsent(dest, new TCPSender(new Socket(dest.getHostname(), dest.getPort())));
		TCPSender sender = senders.get(dest);
		byte[] chunk = new byte[size];

		file.readFully(chunk);
		ChunkWriteRequest request = new ChunkWriteRequest(locations,this.destination+"_chunk_"+chunkNum, chunk, lastModified);
		sender.sendData(request.getBytes());
		sender.flush();
	}

	private void sendShards(int size, RandomAccessFile file, LinkedList<ChunkUtil> locations, int chunkNum) {
		try {
			byte[][] shards = SolomonErasure.encode(file, size);
			for(int i = 0; i < SolomonErasure.TOTAL_SHARDS; i++) {
				ChunkUtil dest = locations.pollFirst();

				senders.putIfAbsent(dest, new TCPSender(new Socket(dest.getHostname(), dest.getPort())));
				TCPSender sender = senders.get(dest);
				ChunkWriteRequest request = new ChunkWriteRequest(new LinkedList<>(),
						this.destination+"_chunk_"+chunkNum+"_"+i, shards[i],lastModified, replication);
				sender.sendData(request.getBytes());
				sender.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void sendFile() {

		try {

			long bytesRemaining = fileSize;
			System.out.println("Sending " + numChunks + " chunks");
			for(int i = 0; i < numChunks; i++) {
				int size = bufferSize;
				if(bytesRemaining < bufferSize) {
					size = (int) bytesRemaining;
				}
				LinkedList<ChunkUtil> locations = chunks[i];

				if(replication) sendChunk(size, file, locations, i);
				else sendShards(size, file, locations, i);

				bytesRemaining-=size;
			}
			for(TCPSender sender : senders.values()) {
				sender.close();
			}
			System.out.println("Finished Sending File");
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
