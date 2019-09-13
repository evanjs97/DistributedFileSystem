package cs555.dfs.transport;

import cs555.dfs.messaging.ChunkWriteRequest;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.FileMetadata;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TCPFileSender implements Runnable{

	private ArrayBlockingQueue<LinkedList<ChunkUtil>> availableLocations = new ArrayBlockingQueue<>(1000);
	private HashMap<ChunkUtil, TCPSender> senders = new HashMap<>();
	private final String filename;
	private final Instant lastModified;
	private final String destination;
	private final RandomAccessFile file;
	private final long fileSize;
	private final long numChunks;
	private final int bufferSize = 64 * 1024;


	public TCPFileSender(String filename, String destination) throws IOException{
		this.filename = filename;
		this.lastModified = FileMetadata.getLastModifiedTime(filename);

		file = new RandomAccessFile(filename, "r");
		fileSize = file.length();
		long temp = fileSize / bufferSize;
		if(fileSize % bufferSize != 0) temp++;
		numChunks = temp;

		this.destination = destination;
	}

	public long getNumChunks() {
		return this.numChunks;
	}

	public void addLocationList(LinkedList<ChunkUtil> locations) {
		availableLocations.offer(locations);
	}

	@Override
	public void run() {

		try {

			long bytesRemaining = fileSize;
			System.out.println("Sending " + numChunks + " chunks");
			System.out.println("Total file size: " + fileSize);
			for(long i = 0; i < numChunks; i++) {
				byte[] chunk;
				if(bytesRemaining >= bufferSize) {
					chunk = new byte[bufferSize];
				}else {
					chunk = new byte[(int) bytesRemaining];
				}

				LinkedList<ChunkUtil> locations = availableLocations.poll(1000, TimeUnit.MILLISECONDS);
				ChunkUtil dest = locations.pollFirst();

				if(!senders.containsKey(dest))
					senders.put(dest, new TCPSender(new Socket(dest.getHostname(), dest.getPort())));
				TCPSender sender = senders.get(dest);
				file.readFully(chunk);

				ChunkWriteRequest request = new ChunkWriteRequest(locations,this.destination+"_chunk_"+i, chunk, lastModified);
				sender.sendData(request.getBytes());
				sender.flush();
				bytesRemaining-=chunk.length;
			}
			System.out.println("Finished Sending File");
		}catch(InterruptedException ie) {
			System.out.println("Interrupted while waiting for server to send chunk server locations: timeout");
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
