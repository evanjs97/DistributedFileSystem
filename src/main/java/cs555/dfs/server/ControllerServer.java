package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPHeartbeat;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.FileMetadata;
import cs555.dfs.util.Heartbeat;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;

public class ControllerServer implements Server{

	public class SetIndexPair {
		final ConcurrentSkipListSet<ChunkUtil> map;
		final ArrayList<ChunkUtil> index;

		public ArrayList<ChunkUtil> getIndex() {
			 return index;
		}

		public ConcurrentSkipListSet<ChunkUtil> getMap() {
			 return map;
		}

		SetIndexPair(ConcurrentSkipListSet<ChunkUtil> map, ArrayList<ChunkUtil> index) {
			this.map = map;
			this.index = index;
		}
	}

	private final ConcurrentHashMap<String, SetIndexPair> fileToServers = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, ChunkUtil> hostToServerObject = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<ChunkUtil, List<String>> hostToFiles = new ConcurrentHashMap<>();
	private ConcurrentSkipListSet<ChunkUtil> chunkServers = new ConcurrentSkipListSet<>();
	private final int replicationLevel = 3;
	private final int port;

	public ControllerServer(int port) {
		this.port = port;
	}

	private void init() {
		TCPServer tcpServer = new TCPServer(port, this);
		Thread server = new Thread(tcpServer);
		server.start();

		List<Heartbeat> heartbeatList = new LinkedList<>();
		heartbeatList.add(new Heartbeat(45, new ControllerHeartbeatTask(hostToFiles, fileToServers, chunkServers)));
		TCPHeartbeat heartbeat = new TCPHeartbeat(heartbeatList);
		Thread heartbeatThread = new Thread(heartbeat);
		heartbeatThread.start();
	}

	private synchronized void sendAvailableServers(ChunkDestinationRequest request, Socket socket)  {
		SetIndexPair util = fileToServers.getOrDefault(request.getFilename(), null);
		LinkedList<ChunkUtil> replicationServers = new LinkedList<>();

		if(util != null) {
			System.out.println("util null");
			replicationServers.addAll(util.map);
		}else {
			for (int i = 0; i < replicationLevel; i++) {
				replicationServers.add(chunkServers.pollFirst());
//				synchronized (chunkServers) {
//					if (!chunkServers.isEmpty()) {
//						ChunkUtil chunk = chunkServers.pollFirst();
//						chunk.incrementAssignedChunks();
//						replicationServers.add(chunk);
//						System.out.println("Adding server back: " + chunk.toString() + " SIZE: " + chunkServers.size());
//						chunkServers.add(chunk);
//					}
//				}
			}
			System.out.println("SIZE: " + chunkServers.size());
			chunkServers.addAll(replicationServers);
			System.out.println("SIZE: " + chunkServers.size());
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
		addChunkServer(request.getHostname(), request.getPort(), request.getFreeSpace());
	}

	private void addChunkServer(String hostname, int port, long freeSpace) {
		ChunkUtil chunkUtil = new ChunkUtil(hostname, port, freeSpace);

		this.chunkServers.add(chunkUtil);
		this.hostToServerObject.put(hostname + ":" + port, chunkUtil);
	}

//	private void handleMajorHeartbeat(ChunkServerHeartbeat heartbeat, Socket socket) {
//		handleMinorHeartbeat(heartbeat, socket);
		//System.out.println(heartbeat);
//	}

	private void handleChunkServerHeartbeat(ChunkServerHeartbeat heartbeat, Socket socket) {
		System.out.println("Heartbeat: " + socket.getInetAddress().getCanonicalHostName()+":"+heartbeat.getPort() + " Free Space: " + heartbeat.getFreeDiskSpace());
		String key = socket.getInetAddress().getCanonicalHostName() + ":" + heartbeat.getPort();
		if(!hostToServerObject.containsKey(key)) {
			addChunkServer(socket.getInetAddress().getCanonicalHostName(), heartbeat.getPort(), heartbeat.getFreeDiskSpace());
		}
		ChunkUtil chunkUtil = hostToServerObject.get(key);
		chunkUtil.setFreeSpace(heartbeat.getFreeDiskSpace());

		for(FileMetadata metadata : heartbeat.getFileInfo()) {
			fileToServers.putIfAbsent(metadata.getFilename(), new SetIndexPair(new ConcurrentSkipListSet<>(), new ArrayList<>()));
			boolean added = fileToServers.get(metadata.getFilename()).map.add(chunkUtil);
			if(added) {
				fileToServers.get(metadata.getFilename()).index.add(chunkUtil);
			}

			hostToFiles.putIfAbsent(chunkUtil, new LinkedList<>());
			hostToFiles.get(chunkUtil).add(metadata.getFilename());
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
		ArrayList<ChunkUtil> fileServers = fileToServers.get(request.getFilename()).index;
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
			case CHUNK_SERVER_HEARTBEAT:
				handleChunkServerHeartbeat((ChunkServerHeartbeat) event, socket);
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
