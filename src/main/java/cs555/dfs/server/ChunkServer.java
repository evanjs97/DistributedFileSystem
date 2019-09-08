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
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ChunkServer implements Server{

	private final String hostname;
	private final int hostPort;
	private int port;
//	private final List<String> files = new LinkedList<>();
	private final HashMap<String, List<String>> fileChecksums = new HashMap<>();
	private final HashMap<String, List<Integer>> fileToCorruptions = new HashMap<>();
	private final Set<String> newFiles = new HashSet<>();

	private static final String BASE_DIR = "/tmp";

	public ChunkServer(String hostname, int port) {
		this.hostname = hostname;
		this.hostPort = port;
	}

	public Set<String> getRecentFiles() {
		return this.newFiles;
	}

	public Set<String> getAllFiles() {
//		return new LinkedList<>(filepathToName.values());
		return this.fileChecksums.keySet();
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
			synchronized (newFiles) {
				newFiles.add(filename);
			}
			synchronized (fileChecksums) {
//				filepathToName.put(filename, shortName);
				fileChecksums.put(filename, ChunkUtil.getChecksums(chunk));
				System.out.println("Writing Checksums");
				for(String checksum : fileChecksums.get(filename)) {
					System.out.println("Checksum: " + checksum);
				}
			}
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void handleFileReadRequest(ChunkReadRequest request, Socket socket) {
		System.out.println("Reading file: " + request.getFilename());
		FileRead chunkRead= readFile(request.getFilename());
		if(!chunkRead.corruptions.isEmpty()) {
			synchronized (fileToCorruptions) {
				fileToCorruptions.put(request.getFilename(), chunkRead.corruptions);
			}
		}
		try {
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getCanonicalHostName(), request.getPort()));
			sender.sendData(new ChunkReadResponse(chunkRead.bytes, request.getFilename(), chunkRead.corruptions).getBytes());
			sender.flush();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void repairCorruptFile(String filename, List<Integer> corruptSections) {
		try {
			TCPSender sender = new TCPSender(new Socket(hostname, hostPort));
			sender.sendData(new ChunkLocationRequest(filename, port).getBytes());
			sender.flush();
		}catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	class FileRead {
		final byte[] bytes;
		final List<Integer> corruptions;

		FileRead(byte[] bytes, List<Integer> corruptions) {
			this.bytes = bytes;
			this.corruptions = Collections.unmodifiableList(corruptions);
		}
	}

	private FileRead readFile(String filename) {
		byte[] bytes = null;
		List<Integer> corruptions = new LinkedList<>();
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			bytes = new byte[(int)raFile.length()];
			List<String> checksums = fileChecksums.get(filename);

			int offset = 0;
			int bytesRemaining = (int)raFile.length();

			for(int i = 0; i < checksums.size(); i++) {
				int checksumLength = 8 * 1024;
				if(checksumLength > bytesRemaining) checksumLength = bytesRemaining;
				raFile.read(bytes, offset, checksumLength);

				if(isCorrupt(checksums.get(i), bytes, offset, checksumLength)) corruptions.add(i);
				offset += checksumLength;
				bytesRemaining -= checksumLength;
			}
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return new FileRead(bytes, corruptions);
	}

	private boolean isCorrupt(String origHash, byte[] newBytes, int offset, int checksumLength) {
		try {
			if (!origHash.equals(ChunkUtil.SHAChecksum(newBytes, offset, checksumLength))) {
				return true;
			}
		}catch(NoSuchAlgorithmException nfae) {
			nfae.printStackTrace();
		}catch(DigestException de) {
			de.printStackTrace();
		}
		return false;
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
