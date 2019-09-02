package cs555.dfs.server;

import cs555.dfs.messaging.ChunkWriteRequest;
import cs555.dfs.messaging.Event;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;

import java.net.Socket;
import java.util.LinkedList;

public class ChunkServer implements Server{

	private final String hostname;
	private final int port;

	public ChunkServer(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	private void init() {
		TCPServer tcpServer = new TCPServer(0, this);
		Thread server = new Thread(tcpServer);
		server.start();
	}

	private void register() {
		T
	}

	@Override
	public void onEvent(Event event, Socket socket) {
		switch (event.getType()) {
			case CHUNK_WRITE_REQUEST:
				writeChunk((ChunkWriteRequest) event);
				break;
			default:
				System.err.println("ChunkServer: No supported event of given type");
				break;
		}
	}

	private void writeChunk(ChunkWriteRequest request) {
		byte[] chunk = request.getChunkData();
		String filename = request.getFilename();
		LinkedList<ChunkUtil> locations = request.getLocations();

		System.out.println("ChunkServer: Received chunk of size " + chunk.length);
		System.out.println("ChunkServer: Corresponding filename is " + filename);
		System.out.println("Next location is: " + locations.getFirst());
	}

	public static void main(String[] args) {
		if(args.length < 2) {
			System.err.println("ChunkServer: Error must specify at least 2 arguments");
			System.exit(1);
		}
		try {
			String hostname = args[0];
			int port = Integer.parseInt(args[1]);
			ChunkServer chunkServer = new ChunkServer(hostname, port);
			chunkServer.init();
		}catch(NumberFormatException nfe) {
			System.err.println("ChunkServer: Error invalid port, must be number in range 1024-65535");
		}
	}
}
