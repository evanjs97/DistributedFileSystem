package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.FileMetadata;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class ControllerServer implements Server{


//	HashSet<ChunkUtil> chunkServerMapping = new HashSet<>();
	HashMap<String, ChunkUtil> fileToServer = new HashMap<>();
	HashMap<String, ChunkUtil> hostToServerObject = new HashMap<>();
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

	private void sendAvailableServers(ChunkDestinationRequest request, Socket socket)  {
		LinkedList<ChunkUtil> replicationServers = new LinkedList<>();
		synchronized (chunkServers) {
			Iterator<ChunkUtil> iter = chunkServers.iterator();
			System.out.println(chunkServers.size());
			for(int i = 0; i < replicationLevel; i++) {
				if(iter.hasNext()) {
					ChunkUtil chunk = iter.next();
					chunk.incrementAssignedChunks();
					replicationServers.add(chunk);
				}
			}
		}
		try {
			System.out.println("Controller: Sending chunk location response");
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getHostName(), request.getPort()));
			sender.sendData(new ChunkDestinationResponse(replicationServers).getBytes());
			sender.flush();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void registerChunkServer(RegisterRequest request) {
		System.out.println("Controller: Received register request from " + request.getHostname() + ":" + request.getPort());
		ChunkUtil chunkUtil = new ChunkUtil(request.getHostname(), request.getPort());
		this.chunkServers.add(chunkUtil);
		this.hostToServerObject.put(request.getHostname()+":"+request.getPort(), chunkUtil);
	}

	private void handleMajorHeartbeat(ChunkServerHeartbeat heartbeat, Socket socket) {

		System.out.println(heartbeat);
	}

	private void handleMinorHeartbeat(ChunkServerHeartbeat heartbeat, Socket socket) {
		System.out.println("Heartbeat: " + socket.getInetAddress().getCanonicalHostName()+":"+heartbeat.getPort());
		for(FileMetadata metadata : heartbeat.getFileInfo()) {
			ChunkUtil chunkUtil = hostToServerObject.get(socket.getInetAddress().getCanonicalHostName()+":"+heartbeat.getPort());
			if(chunkUtil != null) {
				fileToServer.put(metadata.getFilename(), chunkUtil);
			}else {
				System.err.println("Error: Chunk server is not registered");
			}
		}
	}

	private void handleChunkLocationRequest(ChunkLocationRequest request, Socket socket) {
		System.out.println(request.getFilename());
		ChunkUtil util = fileToServer.get(request.getFilename());
		try {
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getHostName(), request.getPort()));
			if(util != null) {
				System.out.println("Controller: Found chunk location for " + request.getFilename());
				sender.sendData(new ChunkLocationResponse(util.getHostname(), util.getPort(), true, request.getFilename()).getBytes());
				sender.flush();

			}else {
				System.err.println("Controller: Unable to find the chunks location for" + request.getFilename());
				sender.sendData(new ChunkLocationResponse("", 0, false, request.getFilename()).getBytes());
				sender.flush();
			}
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	@Override
	public void onEvent(Event event, Socket socket) {
		switch (event.getType()) {
			case CHUNK_DESTINATION_REQUEST:
				System.out.println("Controller: Received chunk destination request");
				sendAvailableServers((ChunkDestinationRequest) event, socket);
				break;
			case REGISTER_REQUEST:
				registerChunkServer((RegisterRequest) event);
				break;
			case CHUNK_SERVER_MAJOR_HEARTBEAT:
				handleMajorHeartbeat((ChunkServerHeartbeat) event, socket);
				break;
			case CHUNK_SERVER_MINOR_HEARTBEAT:
				handleMinorHeartbeat((ChunkServerHeartbeat) event, socket);
				break;
			case CHUNK_LOCATION_REQUEST:
				handleChunkLocationRequest((ChunkLocationRequest) event, socket);
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
