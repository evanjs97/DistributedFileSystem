package cs555.dfs.server;

import cs555.dfs.messaging.ChunkLocationRequest;
import cs555.dfs.messaging.ChunkLocationResponse;
import cs555.dfs.messaging.Event;
import cs555.dfs.messaging.RegisterRequest;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class ControllerServer implements Server{


	SortedSet<ChunkUtil> chunkServers = new TreeSet<>();
	private final int replicationLevel = 3;
	private final int port;

	public ControllerServer(int port) {
		this.port = port;
	}

	private void init() {
		TCPServer tcpServer = new TCPServer(port, this);
		Thread server = new Thread(tcpServer);
		server.start();
	}

	private void sendAvailableServers(ChunkLocationRequest request, Socket socket)  {
		LinkedList<ChunkUtil> replicationServers = new LinkedList<>();
		synchronized (chunkServers) {
			Iterator<ChunkUtil> iter = chunkServers.iterator();
			System.out.println(chunkServers.size());
			for(int i = 0; i < replicationLevel; i++) {
				if(iter.hasNext()) {
					ChunkUtil chunk = iter.next();
					chunk.incrementStoredChunks();
					replicationServers.add(chunk);
				}
			}
		}
		try {
			System.out.println("Controller: Sending chunk location response");
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getHostName(), request.getPort()));
			sender.sendData(new ChunkLocationResponse(replicationServers).getBytes());
			sender.flush();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void registerChunkServer(RegisterRequest request) {
		System.out.println("Controller: Received register request from " + request.getHostname() + ":" + request.getPort());
		this.chunkServers.add(new ChunkUtil(request.getHostname(), request.getPort()));
	}

	@Override
	public void onEvent(Event event, Socket socket) {
		switch (event.getType()) {
			case CHUNK_LOCATION_REQUEST:
				System.out.println("Controller: Received chunk location request");
				sendAvailableServers((ChunkLocationRequest) event, socket);
				break;
			case REGISTER_REQUEST:
				registerChunkServer((RegisterRequest) event);
				break;
			default:
				System.err.println("Controller: No event found for request");
				return;
		}
	}

	public static void main(String[] args) {
		if(args.length < 1) {
			System.err.println("Must specify at least 1 argument.");
		}
		try {
			int port = Integer.parseInt(args[0]);
			if(port < 1024 || port > 65535) {
				throw new NumberFormatException();
			}
			ControllerServer server = new ControllerServer(port);
			server.init();

		}catch(NumberFormatException nfe) {
			System.err.println("Error: Must specify valid port number in range [1024-65535]");
		}
	}
}
