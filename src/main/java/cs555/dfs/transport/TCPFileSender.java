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
//	class Destination {
//		int port;
//		String hostname;
//
//		Destination(String hostname, int port) {
//			this.hostname = hostname;
//			this.port = port;
//		}
//
//		public boolean equals(Object o) {
//			if(o instanceof Destination) {
//				Destination d = (Destination) o;
//				return this.hostname.equals(d.hostname) && this.port == d.port;
//			}else return false;
//		}
//	}
//
//	class ChunkDestination {
//		byte[] chunk;
//		Destination destination;
//
//		ChunkDestination(byte[] chunk, Destination destination) {
//			this.chunk = chunk;
//			this.destination = destination;
//		}
//	}

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

//	public void addChunk(String hostname, int port) {
//		byte[] data =
//		Destination dest = new Destination(hostname, port);
//		if(!senders.containsKey(dest)) {
//			try {
//				senders.put(dest, new TCPSender(new Socket(hostname, port)));
//			}catch(IOException ioe) {
//				ioe.printStackTrace();
//			}
//		}
//		chunks.add(new ChunkDestination(data, dest));
//	}

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
