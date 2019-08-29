package cs555.dfs.server;

import cs555.dfs.messaging.ChunkLocationResponse;
import cs555.dfs.messaging.Event;
import cs555.dfs.transport.TCPSender;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class ControllerServer implements Server{

	class ChunkServer implements Comparable<ChunkServer>{
		int storedChunks;
		String hostname;
		int port;

		ChunkServer(String hostname, int port) {

		}


		@Override
		public int compareTo(ChunkServer other) {
			return Integer.compare(this.storedChunks, other.storedChunks);
		}
	}

	SortedSet<ChunkServer> chunkServers = new TreeSet<ChunkServer>();
	private final int replicationLevel = 3;

	private void sendAvailableServers(Socket socket) throws IOException {
		LinkedList<ChunkServer> replicationServers = new LinkedList<>();
		synchronized (chunkServers) {
			Iterator<ChunkServer> iter = chunkServers.iterator();
			for(int i = 0; i < replicationLevel; i++) {
				if(iter.hasNext()) {

				}
			}
		}
//		ChunkLocationResponse clResponse = new ChunkLocationResponse(chunkServers.)
		TCPSender sender = new TCPSender(socket);
		sender.sendData(new ChunkLocationResponse().getBytes());
		sender.flush();
	}

	@Override
	public void onEvent(Event event, Socket socket) {

	}
}
