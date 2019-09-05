package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPHeartbeat;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.Heartbeat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ChunkServer implements Server{

	private final String hostname;
	private final int hostPort;
	private int port;
	private final List<String> files = new LinkedList<>();
//	private final HashMap<String, String> filepathToName = new HashMap<>();
	private final List<String> newFiles = new LinkedList<>();

	private static final String BASE_DIR = "/tmp";

	public ChunkServer(String hostname, int port) {
		this.hostname = hostname;
		this.hostPort = port;
	}

	public List<String> getRecentFiles() {
		return this.newFiles;
	}

	public List<String> getAllFiles() {
//		return new LinkedList<>(filepathToName.values());
		return this.files;
	}

	public final int getPort() {
		return port;
	}

	public void clearRecentFiles() {
		synchronized (newFiles) {
			newFiles.clear();
		}
	}

	private void init() {
		TCPServer tcpServer = new TCPServer(0, this);
		System.out.println("ChunkServer: Starting on " + tcpServer.getInetAddress().getCanonicalHostName()+":"+tcpServer.getLocalPort());
		this.port = tcpServer.getLocalPort();
		register(tcpServer.getInetAddress().getCanonicalHostName(), tcpServer.getLocalPort());
		Thread server = new Thread(tcpServer);
		server.start();

		List<Heartbeat> heartbeatList = new LinkedList<>();
		heartbeatList.add(new Heartbeat(30, new HeartbeatTask(hostname, hostPort, this, Event.Type.CHUNK_SERVER_MINOR_HEARTBEAT, BASE_DIR)));
		heartbeatList.add(new Heartbeat(5 * 60, new HeartbeatTask(hostname, hostPort, this, Event.Type.CHUNK_SERVER_MAJOR_HEARTBEAT, BASE_DIR)));
		TCPHeartbeat heartbeat = new TCPHeartbeat(heartbeatList);
		Thread heartbeatThread = new Thread(heartbeat);
		heartbeatThread.start();
	}

	private void register(String hostname, int port) {
		try {
			TCPSender sender = new TCPSender(new Socket(this.hostname, this.hostPort));
			sender.sendData(new RegisterRequest(hostname, port).getBytes());
			sender.flush();
		}catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	@Override
	public void onEvent(Event event, Socket socket) {
		switch (event.getType()) {
			case CHUNK_WRITE_REQUEST:
				writeChunk((ChunkWriteRequest) event);
				break;
			case CHUNK_READ_REQUEST:
				handleFileReadRequest((ChunkReadRequest) event, socket);
			default:
				System.err.println("ChunkServer: No supported event of given type");
				break;
		}
	}

	private void writeChunk(ChunkWriteRequest request) {
		byte[] chunk = request.getChunkData();
		String filename = request.getFilename();
		LinkedList<ChunkUtil> locations = request.getLocations();
		writeFile(request.getChunkData(), request.getFilename());
		System.out.println("ChunkServer: Received chunk of size " + chunk.length);
		System.out.println("ChunkServer: Corresponding filename is " + filename);
		if(!request.getLocations().isEmpty()) {
			System.out.println("Next location is: " + locations.getFirst());
			forwardChunk(request);
		}
	}

	private void writeFile(byte[] chunk, String filename) {
		try {
			String dir = BASE_DIR + filename.substring(0, filename.lastIndexOf("/"));
			File file = new File(dir);
			file.mkdirs();
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "rw");
			raFile.write(chunk);
//			String shortName = filename.substring(filename.lastIndexOf("/"+1));
			synchronized (newFiles) {
				newFiles.add(filename);
			}
			synchronized (files) {
//				filepathToName.put(filename, shortName);
				files.add(filename);
			}
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void handleFileReadRequest(ChunkReadRequest request, Socket socket) {
		System.out.println("Reading file: " + request.getFilename());
		byte[] chunk = readFile(request.getFilename());
		try {

			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getCanonicalHostName(), request.getPort()));
			sender.sendData(new ChunkReadResponse(chunk, request.getFilename()).getBytes());
			sender.flush();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private byte[] readFile(String filename) {
		byte[] bytes = null;
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			if(raFile.length() <= 1024 * 64) {
				bytes = new byte[(int)raFile.length()];
				raFile.readFully(bytes);
			}
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return bytes;
	}

	private void forwardChunk(ChunkWriteRequest request) {
		ChunkUtil chunkUtil = request.getLocations().pollFirst();
		try {
			TCPSender sender = new TCPSender(new Socket(chunkUtil.getHostname(), chunkUtil.getPort()));
			sender.sendData(request.getBytes());
			sender.flush();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
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
