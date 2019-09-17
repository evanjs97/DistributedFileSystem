package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPHeartbeat;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.*;

import java.io.IOException;
import java.net.Socket;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
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

	private void sendAvailableServers(ChunkDestinationRequest request, Socket socket)  {
		SetIndexPair util = fileToServers.getOrDefault(request.getFilename(), null);
		LinkedList<ChunkUtil> replicationServers = new LinkedList<>();

		if(util != null) {
			replicationServers.addAll(util.map);
		}else {
			for (int i = 0; i < replicationLevel; i++) {
				replicationServers.add(chunkServers.pollFirst());
			}
			chunkServers.addAll(replicationServers);
		}

		try {
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getHostName(), request.getPort()));
			sender.sendData(new ChunkDestinationResponse(replicationServers, request.getFilename()).getBytes());
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void registerChunkServer(RegisterRequest request) {
		System.out.println("Controller: Received register request from " + request.getHostname() + ":" + request.getPort());
		addChunkServer(request.getHostname(), request.getPort(), request.getFreeSpace());
	}

	private void addChunkServer(String hostname, int port, double freeSpace) {
		ChunkUtil chunkUtil = new ChunkUtil(hostname, port, freeSpace);


		this.chunkServers.add(chunkUtil);
		this.hostToServerObject.put(hostname + ":" + port, chunkUtil);
	}

	private void handleChunkServerHeartbeat(ChunkServerHeartbeat heartbeat, Socket socket) {
//		System.out.println("Heartbeat: " + socket.getInetAddress().getCanonicalHostName()+":"+heartbeat.getPort() + " Free Space: " + heartbeat.getFreeDiskSpace());
		StringBuilder builder = new StringBuilder();
		String hostFormat = Format.format(socket.getInetAddress().getCanonicalHostName()+":"+heartbeat.getPort(), 20);
		String spaceFormat = Format.format(String.format("%.2f",heartbeat.getFreeDiskSpace()), 7);
		builder.append(String.format("Host: %s, Free Space: %s\n",hostFormat, spaceFormat));

		String key = socket.getInetAddress().getCanonicalHostName() + ":" + heartbeat.getPort();
		if(!hostToServerObject.containsKey(key)) {
			addChunkServer(socket.getInetAddress().getCanonicalHostName(), heartbeat.getPort(), heartbeat.getFreeDiskSpace());
		}
		ChunkUtil chunkUtil = hostToServerObject.get(key);
		chunkUtil.setFreeSpace(heartbeat.getFreeDiskSpace());

		for(FileMetadata metadata : heartbeat.getFileInfo()) {
			for(ChunkMetadata chunkMetadata : metadata.getChunks()) {
				String fullFile = metadata.getFilename() + "_chunk_"+chunkMetadata.getChunkNum();
				fileToServers.putIfAbsent(fullFile, new SetIndexPair(new ConcurrentSkipListSet<>(), new ArrayList<>()));
				boolean added = fileToServers.get(fullFile).map.add(chunkUtil);
				if(added) {
					fileToServers.get(fullFile).index.add(chunkUtil);
				}

				hostToFiles.putIfAbsent(chunkUtil, new LinkedList<>());
				hostToFiles.get(chunkUtil).add(fullFile);
			}
			System.out.println(builder.toString()+metadata.toString());
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
				sender.close();

			}else {
				System.err.println("Controller: Unable to find the chunks location for" + request.getFilename());
				sender.sendData(new ChunkLocationResponse("", 0, false, request.getFilename()).getBytes());
				sender.close();
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
