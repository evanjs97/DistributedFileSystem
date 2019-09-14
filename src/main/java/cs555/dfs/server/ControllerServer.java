package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.FileMetadata;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;

public class ControllerServer implements Server{

	private final ConcurrentHashMap<String, List<ChunkUtil>> fileToServers = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, ChunkUtil> hostToServerObject = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, List<String>> hostToFiles = new ConcurrentHashMap<>();
	private final ConcurrentSkipListSet<ChunkUtil> chunkServers = new ConcurrentSkipListSet<>();
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
		List<ChunkUtil> util = fileToServers.getOrDefault(request.getFilename(), null);
		LinkedList<ChunkUtil> replicationServers = new LinkedList<>();

		if(util != null) {
			replicationServers.addAll(util);
		}else {
			for (int i = 0; i < replicationLevel; i++) {
				ChunkUtil chunk = chunkServers.pollFirst();
				chunk.incrementAssignedChunks();
				replicationServers.add(chunk);
				chunkServers.add(chunk);
			}
		}

		try {
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getHostName(), request.getPort()));
			sender.sendData(new ChunkDestinationResponse(replicationServers, request.getFilename()).getBytes());
			sender.flush();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void registerChunkServer(RegisterRequest request) {
		System.out.println("Controller: Received register request from " + request.getHostname() + ":" + request.getPort());
		ChunkUtil chunkUtil = new ChunkUtil(request.getHostname(), request.getPort());
			this.chunkServers.add(chunkUtil);
		this.hostToServerObject.put(request.getHostname() + ":" + request.getPort(), chunkUtil);
	}

	private void handleMajorHeartbeat(ChunkServerHeartbeat heartbeat, Socket socket) {
		handleMinorHeartbeat(heartbeat, socket);
		//System.out.println(heartbeat);
	}

	private void handleMinorHeartbeat(ChunkServerHeartbeat heartbeat, Socket socket) {
//		System.out.println("Heartbeat: " + socket.getInetAddress().getCanonicalHostName()+":"+heartbeat.getPort());
		for(FileMetadata metadata : heartbeat.getFileInfo()) {
			ChunkUtil chunkUtil = hostToServerObject.get(socket.getInetAddress().getCanonicalHostName() + ":" + heartbeat.getPort());
			if(chunkUtil != null) {
				fileToServers.putIfAbsent(metadata.getFilename(), new ArrayList<>());
				fileToServers.get(metadata.getFilename()).add(chunkUtil);

				hostToFiles.putIfAbsent(chunkUtil.toString(), new LinkedList<>());
				hostToFiles.get(chunkUtil.toString()).add(metadata.getFilename());
			}else {
				System.err.println("Error: Chunk server is not registered");
			}
		}
	}

	/**
	 * handleChunkLocation request method handles incoming requests from nodes to locate given file
	 * if file cant be found, or if file can only be found on the machine that sent the request a failed response is sent
	 * otherwise a successful response is sent containing the hostname and port of the chunk server hosting the file
	 * @param request the request
	 * @param socket the socket the request was received over
	 */
	private void handleChunkLocationRequest(ChunkLocationRequest request, Socket socket) {
		List<ChunkUtil> fileServers = fileToServers.get(request.getFilename());
		int random = ThreadLocalRandom.current().nextInt(0, fileServers.size());
		ChunkUtil util = fileServers.get(random);

		if(util.getHostname().equals(socket.getInetAddress().getCanonicalHostName()) && util.getPort() == socket.getLocalPort()) {
			if(fileServers.size() <= 1) util = null;
			else if(random == 0) util = fileServers.get(random+1);
			else util = fileServers.get(random-1);
		}

		try {
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getCanonicalHostName(), request.getPort()));
			if(util != null) {
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
