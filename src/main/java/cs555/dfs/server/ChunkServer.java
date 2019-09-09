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
		FileRead chunkRead = null;
		if(request.getChunkSlices().isEmpty()) {
			chunkRead = readFile(request.getFilename());
		}else chunkRead = readFile(request.getFilename(), request.getChunkSlices());

		try {
			TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getCanonicalHostName(), request.getPort()));
			sender.sendData(new ChunkReadResponse(chunkRead.bytes, request.getFilename(), chunkRead.corruptions).getBytes());
			sender.flush();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		if(!chunkRead.corruptions.isEmpty()) {
			repairCorruptFile(request.getFilename(), chunkRead.corruptions);
		}
	}

	private void handleChunkReadResponse(ChunkReadResponse response) {
		if(response.getCorruptions().isEmpty()) {
			System.out.println("Chunk Server: Repairing file corruption: " + response.getFilename());
			byte[] validChunk = response.getChunk();
			List<Integer> slices = fileToCorruptions.get(response.getFilename());
			try {
				RandomAccessFile raFile = new RandomAccessFile(response.getFilename(), "rw");
				for(Integer slice : slices) {
					raFile.seek(slice);
				}
			}catch(FileNotFoundException fnfe) {
				fnfe.printStackTrace();
			}
		}else {
			System.out.println("Chunk Server: Unable to repair corruption");
		}
	}

	private void handleChunkLocationResponse(ChunkLocationResponse response) {
		try {
			TCPSender sender = new TCPSender(new Socket(response.getHostname(), response.getPort()));
			sender.sendData(new ChunkReadRequest(response.getFilename(),
					port, fileToCorruptions.get(response.getFilename())).getBytes());
			sender.flush();
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

	private FileRead readFile(String filename, List<Integer> slices) {
		byte[] bytes = null;
		List<Integer> corruptions = new LinkedList<>();
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			bytes = new byte[(int)raFile.length()];
			ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

			List<String> checkSums = fileChecksums.get(filename);

			for(int i = 0; i < slices.size(); i++) {
				int offset = slices.get(i);
				int offsetLength = Math.min(8 * 1024, (int) raFile.length() - offset);
				byte[] arr = new byte[offsetLength];
				raFile.read(arr, offset, offsetLength);

				int checkIndex = (offset / (8 * 1024)) -1;
				if(isCorrupt(checkSums.get(checkIndex), bytes, offset, offsetLength)) corruptions.add(offset);

				dout.write(arr);
			}
			dout.flush();
			bytes = baOutStream.toByteArray();
			baOutStream.close();
			dout.close();
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return new FileRead(bytes, corruptions);
	}

	private FileRead readFile(String filename) {
		byte[] bytes = null;
		List<Integer> corruptions = new LinkedList<>();
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			bytes = new byte[(int)raFile.length()];
			List<String> checkSums = fileChecksums.get(filename);

			int offset = 0;
			int bytesRemaining = (int)raFile.length();

			for(int i = 0; i < checkSums.size(); i++) {
				int checksumLength = 8 * 1024;
				if(checksumLength > bytesRemaining) checksumLength = bytesRemaining;
				raFile.read(bytes, offset, checksumLength);

				if(isCorrupt(checkSums.get(i), bytes, offset, checksumLength)) corruptions.add(offset);
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
