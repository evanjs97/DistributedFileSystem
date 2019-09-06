package cs555.dfs.transport;

import cs555.dfs.messaging.ChunkWriteRequest;
import cs555.dfs.util.ChunkUtil;
import sun.security.krb5.internal.crypto.Des;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TCPFileSender implements Runnable{

	private ArrayBlockingQueue<LinkedList<ChunkUtil>> availableLocations = new ArrayBlockingQueue<>(1000);
	private HashMap<ChunkUtil, TCPSender> senders = new HashMap<>();
	private final String filename;
	private final RandomAccessFile file;
	private long bytesRemaining;
	private final long numChunks;
	private final int bufferSize = 64 * 1024;


	public TCPFileSender(String filename) throws IOException{
		this.filename = filename;
		file = new RandomAccessFile(filename, "r");
		bytesRemaining = file.length();
		long temp = bytesRemaining / bufferSize;
		if(bytesRemaining % bufferSize != 0) temp++;
		numChunks = temp;
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

				ChunkWriteRequest request = new ChunkWriteRequest(locations,this.filename+"_chunk_"+i, chunk);
				sender.sendData(request.getBytes());
				sender.flush();
			}
		}catch(InterruptedException ie) {
			System.out.println("Interrupted while waiting for server to send chunk server locations: timeout");
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
