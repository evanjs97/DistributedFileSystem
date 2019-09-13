package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPHeartbeat;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.Heartbeat;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class ChunkServer implements Server{

	private final String hostname;
	private final int hostPort;
	private int port;
	private final HashMap<String, List<String>> fileChecksums = new HashMap<>();
//	private final HashMap<String, List<Integer>> fileToCorruptions = new HashMap<>();
	private final Set<String> newFiles = new HashSet<>();

	private final String BASE_DIR;

	public ChunkServer(String hostname, int port, String dataDir) {
		this.hostname = hostname;
		this.hostPort = port;
		this.BASE_DIR = dataDir;
	}

	public Set<String> getRecentFiles() {
		return this.newFiles;
	}

	public Set<String> getAllFiles() {
		synchronized(fileChecksums) {
			return this.fileChecksums.keySet();
		}
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
			sender.close();
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
				ChunkReadRequest request = (ChunkReadRequest) event;
				handleFileReadRequest(request, socket);
				break;
			case CHUNK_READ_RESPONSE:
				handleChunkReadResponse((ChunkReadResponse) event);
				break;
			case CHUNK_LOCATION_RESPONSE:
				MessagingUtil.handleChunkLocationResponse((ChunkLocationResponse) event, port);
				break;
			default:
				System.err.println("ChunkServer: No supported event of given type");
				break;
		}
	}


	private void writeChunk(ChunkWriteRequest request) {
		if(!request.getLocations().isEmpty()) {
			forwardChunk(request);
		}
		writeFile(request.getChunkData(), request.getFilename());

	}

	private void writeFile(byte[] chunk, String filename) {
		try {
			String dir = BASE_DIR + filename.substring(0, filename.lastIndexOf("/"));
			File file = new File(dir);
			file.mkdirs();

			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "rw");
			List<String> checkSums = ChunkUtil.getChecksums(chunk);
			synchronized (fileChecksums) {
				fileChecksums.put(filename, checkSums);
			}
			raFile.write(chunk);
			raFile.setLength(chunk.length);
			synchronized (newFiles) {
				newFiles.add(filename);
			}
			raFile.close();
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void handleFileReadRequest(ChunkReadRequest request, Socket socket) {
		FileRead chunkRead = readFile(request.getFilename());

		try {
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getCanonicalHostName(), request.getPort()));
			sender.sendData(new ChunkReadResponse(chunkRead.bytes, request.getFilename(), !chunkRead.corrupted).getBytes());
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		if(chunkRead.corrupted) {
			repairCorruptFile(request.getFilename());
		}
	}

	private void handleChunkReadResponse(ChunkReadResponse response) {
		if(response.isSuccess()) {
			System.out.println("Chunk Server: Repairing file corruption: " + response.getFilename());
			byte[] validChunk = response.getChunk();
			try {
				RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + response.getFilename(), "rw");
				raFile.seek(0);
				raFile.write(validChunk);

				raFile.setLength(validChunk.length);
				raFile.close();
				System.out.println("Chunk Server: Fixed file corruption");
			}catch(FileNotFoundException fnfe) {
				fnfe.printStackTrace();
			}catch(IOException ioe) {
				ioe.printStackTrace();
			}
		}else {
			System.out.println("Chunk Server: Unable to repair corruption");
		}
	}

	private void repairCorruptFile(String filename) {
		try {
			TCPSender sender = new TCPSender(new Socket(hostname, hostPort));
			MessagingUtil.handleChunkLocationRequest(sender, filename, port);
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	class FileRead {
		final byte[] bytes;
		final boolean corrupted;

		FileRead(byte[] bytes, boolean corrupted) {
			this.bytes = bytes;
			this.corrupted = corrupted;
		}
	}


	private FileRead readFile(String filename) {
		byte[] bytes = null;
		boolean corrupted = false;
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			int length = (int) raFile.length();

			List<String> checkSums;
			synchronized (fileChecksums) {
				checkSums = fileChecksums.get(filename);
			}

			bytes = new byte[length];
			raFile.readFully(bytes);
			List<String> newChecks = ChunkUtil.getChecksums(bytes);
			corrupted = ChunkUtil.getCorruptedFromChecksums(checkSums, newChecks);

		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return new FileRead(bytes, corrupted);
	}

	private void forwardChunk(ChunkWriteRequest request) {
		ChunkUtil chunkUtil = request.getLocations().pollFirst();
		try {
			TCPSender sender = new TCPSender(new Socket(chunkUtil.getHostname(), chunkUtil.getPort()));
			sender.sendData(new ChunkWriteRequest(request.getLocations(),request.getFilename(),request.getChunkData()).getBytes());
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if(args.length < 3) {
			System.err.println("ChunkServer: Error must specify at least 3 arguments");
			System.exit(1);
		}
		try {
			String hostname = args[0];
			int port = Integer.parseInt(args[1]);
			String dataDir = args[2];
			ChunkServer chunkServer = new ChunkServer(hostname, port, dataDir);
			chunkServer.init();
		}catch(NumberFormatException nfe) {
			System.err.println("ChunkServer: Error invalid port, must be number in range 1024-65535");
		}
	}
}
