package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPHeartbeat;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.Heartbeat;

import java.io.*;
import java.net.Socket;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ChunkServer implements Server{

	private final String hostname;
	private final int hostPort;
	private int port;
	private final HashMap<String, List<String>> fileChecksums = new HashMap<>();
	private final HashMap<String, List<Integer>> fileToCorruptions = new HashMap<>();
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
				handleChunkLocationResponse((ChunkLocationResponse) event);
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
//			if(filename.contains("159")) {
//				System.out.println(Arrays.toString(chunk));
//			}
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "rw");
			List<String> checkSums = ChunkUtil.getChecksums(chunk);
			synchronized (fileChecksums) {
				fileChecksums.put(filename, checkSums);
			}
			raFile.write(chunk);
			if(filename.contains("159")) System.out.println("WROTE File of length: " + raFile.length() + " chunk length: " + chunk.length + " Checksums: " + checkSums.size());
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
		System.out.println("Reading file: " + request.getFilename());
		FileRead chunkRead;
		if(request.getChunkSlices().isEmpty()) {
			chunkRead = readFile(request.getFilename());
		}else chunkRead = readFile(request.getFilename(), request.getChunkSlices());

		try {
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getCanonicalHostName(), request.getPort()));
			System.out.println("Reading File: " + request.getFilename());
			sender.sendData(new ChunkReadResponse(chunkRead.bytes, request.getFilename(), chunkRead.corruptions, chunkRead.fileLength).getBytes());
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		if(!chunkRead.corruptions.isEmpty()) {
			//repairCorruptFile(request.getFilename(), chunkRead.corruptions);
		}
	}

	private void handleChunkReadResponse(ChunkReadResponse response) {
		if(response.getCorruptions().isEmpty()) {
			System.out.println("Chunk Server: Repairing file corruption: " + response.getFilename());
			byte[] validChunk = response.getChunk();
			List<Integer> slices;
			synchronized (fileToCorruptions) {
				slices = fileToCorruptions.get(response.getFilename());
			}
//			List<String> checkSums = fileChecksums.get(response.getFilename());
			try {
				RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + response.getFilename(), "rw");
				int remainingBytes = validChunk.length;
				int offset = 0;
				for(int i = 0; i < slices.size(); i++) {
//					raFile.seek(slices.get(i));
					int length = 8 * 1024;
					if(length > remainingBytes) length = remainingBytes;
					byte[] temp = Arrays.copyOfRange(validChunk, offset, offset+length);
					raFile.write(temp, slices.get(i), temp.length);
					offset += length;
				}
				raFile.setLength(response.getChunkSize());
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

	private void handleChunkLocationResponse(ChunkLocationResponse response) {
		try {
			TCPSender sender = new TCPSender(new Socket(response.getHostname(), response.getPort()));
			List<Integer> corruptions;
			synchronized (fileToCorruptions) {
				corruptions = fileToCorruptions.get(response.getFilename());
			}
			sender.sendData(new ChunkReadRequest(response.getFilename(),
					port, corruptions).getBytes());
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void repairCorruptFile(String filename, List<Integer> corruptSections) {
		try {
			synchronized (fileToCorruptions) {
				fileToCorruptions.put(filename, corruptSections);
			}
			TCPSender sender = new TCPSender(new Socket(hostname, hostPort));
			sender.sendData(new ChunkLocationRequest(filename, port).getBytes());
			sender.close();
		}catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	class FileRead {
		final byte[] bytes;
		final List<Integer> corruptions;
		final int fileLength;

		FileRead(byte[] bytes, List<Integer> corruptions, int length) {
			this.bytes = bytes;
			this.corruptions = Collections.unmodifiableList(corruptions);
			this.fileLength = length;
		}
	}

	private FileRead readFile(String filename, List<Integer> slices) {
		byte[] bytes = null;
		List<Integer> corruptions = new LinkedList<>();
		int length = 0;
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			length = (int) raFile.length();

			List<String> checkSums;
			synchronized (fileChecksums) {
				checkSums = fileChecksums.get(filename);
			}
			bytes = new byte[length];
			raFile.readFully(bytes);
			List<String> newChecks = ChunkUtil.getChecksums(bytes);
			corruptions = ChunkUtil.getCorruptionsFromChecksums(checkSums, newChecks);

		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return new FileRead(bytes, corruptions, length);
	}

	private FileRead readFile(String filename) {
		byte[] bytes = null;
		List<Integer> corruptions = new LinkedList<>();
		int length = 0;
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			length = (int) raFile.length();

			List<String> checkSums;
			synchronized (fileChecksums) {
				checkSums = fileChecksums.get(filename);
			}
			if(filename.contains("159")) System.out.println("LENGTH: " + length + " Num Checksums: " + checkSums.size());

			bytes = new byte[length];
			raFile.readFully(bytes);
			List<String> newChecks = ChunkUtil.getChecksums(bytes);
			corruptions = ChunkUtil.getCorruptionsFromChecksums(checkSums, newChecks);

		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return new FileRead(bytes, corruptions, length);
	}

	private boolean isCorrupt(String origHash, byte[] newBytes) {
		try {
			String checksum = ChunkUtil.SHAChecksum(newBytes);
			if (!origHash.equals(checksum)) {
				System.out.println(origHash + "\n" + checksum);
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
