package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPHeartbeat;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.ChunkMetadata;
import cs555.dfs.util.Heartbeat;

import java.io.*;
import java.net.Socket;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ChunkServer implements Server{

	private final String hostname;
	private final int hostPort;
	private int port;
	private final ConcurrentHashMap<String, List<Integer>> corruptFiles = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, Integer> fileToSize = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentSkipListSet<Integer>>> files = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentSkipListSet<Integer>>> newFiles = new ConcurrentHashMap<>();
	private static final int CHECKSUM_SLICE = 8 * 1024;
	private static final int CHECKSUM = 40;

	private final String BASE_DIR;

	public ChunkServer(String hostname, int port, String dataDir) {
		this.hostname = hostname;
		this.hostPort = port;
		this.BASE_DIR = dataDir;
	}

	public final int getPort() {
		return port;
	}

	public void clearRecentFiles() {
		synchronized (newFiles) {
			newFiles.clear();
		}
	}

	/**
	 * Initializes chunk server, starts server on random port
	 * Registers itself with the controller server
	 * Sets up heartbeat server to send heartbeats to the controller
	 */
	private void init() {
		TCPServer tcpServer = new TCPServer(0, this);
		System.out.println("ChunkServer: Starting on " + tcpServer.getInetAddress().getCanonicalHostName()+":"+tcpServer.getLocalPort());
		this.port = tcpServer.getLocalPort();
		register(tcpServer.getInetAddress().getCanonicalHostName(), tcpServer.getLocalPort());
		Thread server = new Thread(tcpServer);
		server.start();

		List<Heartbeat> heartbeatList = new LinkedList<>();
		heartbeatList.add(new Heartbeat(30, new ChunkServerHeartbeatTask(hostname, hostPort, this, newFiles, BASE_DIR)));
		heartbeatList.add(new Heartbeat(5 * 60, new ChunkServerHeartbeatTask(hostname, hostPort, this, files, BASE_DIR)));
		TCPHeartbeat heartbeat = new TCPHeartbeat(heartbeatList);
		Thread heartbeatThread = new Thread(heartbeat);
		heartbeatThread.start();
	}

	/**
	 * Registers the chunk server with the Controller
	 * @param hostname the hostname of the controller
	 * @param port the port of the controller server
	 */
	private void register(String hostname, int port) {
		try {
			TCPSender sender = new TCPSender(new Socket(this.hostname, this.hostPort));
			long freeSpace = ChunkMetadata.getAvailableDiskSpace(BASE_DIR);
			sender.sendData(new RegisterRequest(hostname, port, freeSpace).getBytes());
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
				System.out.println("RECIEVED READ RESPONSE: " + socket.getInetAddress().getCanonicalHostName());
				handleChunkReadResponse((ChunkReadResponse) event);
				break;
			case CHUNK_LOCATION_RESPONSE:
				handleLocationResponse((ChunkLocationResponse) event);
//				MessagingUtil.handleChunkLocationResponse((ChunkLocationResponse) event, port, false);
				break;
			case CHUNK_FORWARD_REQUEST:
				System.out.println("Received Chunk Forwarding Request");
				handleChunkForwardRequest((ChunkForwardRequest) event);
				break;
			default:
				System.err.println("ChunkServer: No supported event of given type");
				break;
		}
	}

	private void handleLocationResponse(ChunkLocationResponse response) {
		List<Integer> corruptChunks = corruptFiles.getOrDefault(response.getFilename(), null);
		if (corruptChunks == null) {
			System.out.println("Failed to repair file, cannot locate corrupt slices");
			return;
		}
		try {
			TCPSender sender = new TCPSender(new Socket(response.getHostname(), response.getPort()));
			sender.sendData(new ChunkReadRequest(response.getFilename(), port, corruptChunks).getBytes());
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void handleChunkForwardRequest(ChunkForwardRequest request) {
		FileRead chunkRead = readFile(request.getFilename(), true);

		try {
			TCPSender sender = new TCPSender(new Socket(request.getDestHost(), request.getDestPort()));
			Instant lastModified = ChunkMetadata.getLastModifiedTime(BASE_DIR+request.getFilename());
			ChunkWriteRequest cwr = new ChunkWriteRequest(new LinkedList<>(), request.getFilename(), chunkRead.bytes, lastModified);
			sender.sendData(cwr.getBytes());
			sender.close();
		}catch(IOException ioe) {
			System.out.println("Failed to send to: " + request.getDestHost() + ":"+request.getDestPort());
		}
	}


	/**
	 * handle chunk write request from client
	 * initializes chunk write to file
	 * @param request the chunk write request
	 */
	private void writeChunk(ChunkWriteRequest request) {

		if(!request.getLocations().isEmpty() && request.getReplication()) {
			forwardChunk(request);
		}
		writeFile(request.getChunkData(), request.getFilename(), request.getReplication(), request.getActualSize());
		ChunkMetadata.setLastModifiedTime(BASE_DIR+request.getFilename(), request.getLastModified());
	}

	/**
	 * write file handles writing of file to disk
	 * the location of the write will be in BASE_DIR/filaname
	 * @param chunk the chunk to write
	 * @param filename the filename for that chunk
	 */
	private void writeFile(byte[] chunk, String filename, boolean replication, int actualSize) {
		try {
			String dir = BASE_DIR + filename.substring(0, filename.lastIndexOf("/"));
			File file = new File(dir);
			file.mkdirs();

			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "rw");
			raFile.seek(0);

			if(replication) {
				int numChecksums = chunk.length / (8 * 1024);
				if (chunk.length % (8 * 1024) > 0) {
					numChecksums++;
				}
				int remainingBytes = chunk.length;
				int offset = 0;
				int length = CHECKSUM_SLICE;
				for (int i = 0; i < numChecksums; i++) {

					byte[] range = Arrays.copyOfRange(chunk, offset, length);
					raFile.write(range);

					raFile.write(ChunkUtil.SHAChecksum(range).getBytes());

					remainingBytes -= CHECKSUM_SLICE;
					offset = length;
					if (CHECKSUM_SLICE > remainingBytes) length += remainingBytes;
					else length += CHECKSUM_SLICE;
				}
				raFile.setLength(chunk.length + (numChecksums * CHECKSUM));
			}else {
				raFile.write(chunk);
				raFile.setLength(chunk.length);
			}
			fileToSize.put(filename, actualSize);
			String actualFile = filename.substring(0, filename.lastIndexOf("_chunk_"));
			int chunkNum ;
			int shardNum = -1;
			if(!replication) {
				chunkNum = Integer.parseInt(filename.substring(filename.lastIndexOf("_chunk_")+7,
						filename.lastIndexOf('_')));
				shardNum = Integer.parseInt(filename.substring(filename.lastIndexOf('_')+1));
			}else {
				chunkNum = Integer.parseInt(filename.substring(filename.lastIndexOf('_')+1));
			}
			newFiles.putIfAbsent(actualFile, new ConcurrentHashMap<>());
			files.putIfAbsent(actualFile, new ConcurrentHashMap<>());
			newFiles.get(actualFile).putIfAbsent(chunkNum, new ConcurrentSkipListSet<>());
			files.get(actualFile).putIfAbsent(chunkNum, new ConcurrentSkipListSet<>());


			if(shardNum != -1) {
				ConcurrentSkipListSet<Integer> newShards = newFiles.get(actualFile).remove(chunkNum);
				ConcurrentSkipListSet<Integer> oldShards = files.get(actualFile).remove(chunkNum);
				newShards.add(shardNum);
				oldShards.add(shardNum);
				newFiles.get(actualFile).put(chunkNum, newShards);
				files.get(actualFile).put(chunkNum, newShards);
			}

			raFile.close();
			ChunkMetadata.incrementVersion(BASE_DIR+filename+".metadata");
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}catch(NoSuchAlgorithmException nsae) {
			nsae.printStackTrace();
		}catch(DigestException de) {
			de.printStackTrace();
		}
	}

	private void writeFile(byte[] chunk, String filename, int fileSize) {
		List<Integer> corruptions = corruptFiles.get(filename);
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "rw");

			int remaining = chunk.length;
			int startPosition = 0;
			for(int rep : corruptions) {
				raFile.seek(rep*(CHECKSUM_SLICE+CHECKSUM));
				int size = CHECKSUM_SLICE+CHECKSUM;
				if(size > remaining) size = remaining;

				byte[] copy = Arrays.copyOfRange(chunk, startPosition, startPosition+size);
				raFile.write(copy);

				remaining-=size;
				startPosition+=size;
			}
			raFile.setLength(fileSize);
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * handles incoming file read request from client
	 * retrieves file from disk and sends to client
	 * In case of file corruption, the client is informed in the message, then starts file repair task
	 * @param request the read request
	 * @param socket the socket that received the request
	 */
	private void handleFileReadRequest(ChunkReadRequest request, Socket socket) {
		FileRead chunkRead;
		if(request.getCorruptions().isEmpty()) {
			chunkRead = readFile(request.getFilename(), request.getReplication());
			try {
				TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getCanonicalHostName(), request.getPort()));
				sender.sendData(new ChunkReadResponse(chunkRead.bytes, request.getFilename(), chunkRead.corrupted.isEmpty(), fileToSize.get(request.getFilename())).getBytes());
				sender.close();
			}catch(IOException ioe) {
				ioe.printStackTrace();
			}
		}
		else {
			chunkRead = readFile(request.getFilename(), request.getCorruptions());
			try {
				TCPSender sender = new TCPSender(new Socket(socket.getInetAddress().getCanonicalHostName(), request.getPort()));
				sender.sendData(new ChunkReadResponse(chunkRead.bytes, request.getFilename(), chunkRead.corrupted.isEmpty(), chunkRead.length).getBytes());
				sender.close();
			}catch(IOException ioe) {
				ioe.printStackTrace();
			}
		}

		if(!chunkRead.corrupted.isEmpty() && request.getReplication()) {
			System.out.println("Repairing Corrupt File");
			repairCorruptFile(request.getFilename(), chunkRead.corrupted);
		}
	}

	/**
	 * handles response from another chunk server that contains the requested file
	 * the received chunk, overwrites the original chunk, unless the new one is also corrupt
	 * @param response the response received from the chunk server
	 */
	private void handleChunkReadResponse(ChunkReadResponse response) {
		if(response.isSuccess()) {
			System.out.println("Chunk Server: Repairing file corruption: " + response.getFilename());
			writeFile(response.getChunk(), response.getFilename(), response.getFileSize());
		}else {
			System.out.println("Chunk Server: Unable to repair corruption");
		}
	}

	/**
	 * repairCorruptFile initializes chunk repair request
	 * @param filename the filename associated with the corrupt file
	 */
	private void repairCorruptFile(String filename, List<Integer> corruptChunks) {
		try {
			corruptFiles.put(filename, corruptChunks);
			TCPSender sender = new TCPSender(new Socket(hostname, hostPort));
			MessagingUtil.handleChunkLocationRequest(sender, filename, port);
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * Util pair class for use with file reading
	 */
	class FileRead {
		final byte[] bytes;
		final List<Integer> corrupted;
		final int length;

		FileRead(byte[] bytes, List<Integer> corrupted, int length) {
			this.bytes = bytes;
			this.corrupted = corrupted;
			this.length = length;
		}
	}

	private FileRead readFile(String filename, List<Integer> repairs) {
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			ByteArrayOutputStream baout = new ByteArrayOutputStream();
			int length = (int)  raFile.length();
			for (int rep : repairs) {
				int position = rep*(CHECKSUM_SLICE+CHECKSUM);
				raFile.seek(position);

				int size = CHECKSUM_SLICE+CHECKSUM;
				int pointer = (int) raFile.getFilePointer();
				if(pointer + size > length) {
					size = length - pointer;
				}
				byte[] bytes = new byte[size];
				raFile.readFully(bytes);
				baout.write(bytes);
			}
			byte[] out = baout.toByteArray();
			System.out.println("Chunk Server: Retrieved chunks for file repair");
			return new FileRead(out, new LinkedList<>(), out.length);
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return new FileRead(new byte[0], new LinkedList<>(), 0);
	}


	/**
	 * readFile reads the specified file from disk, and checks it for corruptions
	 * @param filename the filename to read from
	 * @return a FileRead object that contains chunk and corruption information
	 */
	private FileRead readFile(String filename, boolean replication) {
		byte[] bytes = null;
		List<Integer> corruptChunks = new ArrayList<>();
		int chunkNum = 0;
		int length = 0;
		try {
			RandomAccessFile raFile = new RandomAccessFile(BASE_DIR + filename, "r");
			raFile.seek(0);
			length = (int) raFile.length();

			ByteArrayOutputStream baout = new ByteArrayOutputStream();
			if(replication) {
				for (int i = 0; i < length; i += (CHECKSUM_SLICE + CHECKSUM)) {
					int numBytes = CHECKSUM_SLICE;
					if (length - (i + CHECKSUM) < CHECKSUM_SLICE) {
						numBytes = length - (i + CHECKSUM);
					}
					if(numBytes < 0) {
						System.out.println("Chunk Server: Detected corrupted file: " + filename + " at slice: " + chunkNum);
						corruptChunks.add(chunkNum);
						break;
					}
					byte[] arr = new byte[numBytes];
					byte[] checksum = new byte[CHECKSUM];
					raFile.readFully(arr);
					raFile.readFully(checksum);
					String check1 = ChunkUtil.SHAChecksum(arr);
					String check2 = new String(checksum);
					if (!check1.equals(check2)) {
						System.out.println("Chunk Server: Detected corrupted file: " + filename + " at slice: " + chunkNum);
						corruptChunks.add(chunkNum);

					}
					chunkNum++;
					baout.write(arr);
				}
			}else {
				byte[] arr = new byte[(int)raFile.length()];
				raFile.readFully(arr);
				baout.write(arr);
			}
			bytes = baout.toByteArray();
			raFile.close();
		}catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}catch(NoSuchAlgorithmException nsae) {
			nsae.printStackTrace();
		}catch(DigestException de) {
			de.printStackTrace();
		}
		return new FileRead(bytes, corruptChunks, length);
	}

	/**
	 * Forwards a chunk to the next location from the list
	 * @param request the chunk write request to use for forwarding.
	 */
	private void forwardChunk(ChunkWriteRequest request) {
		ChunkUtil chunkUtil = request.getLocations().pollFirst();
		try {
			TCPSender sender = new TCPSender(new Socket(chunkUtil.getHostname(), chunkUtil.getPort()));
			sender.sendData(request.getBytes());
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
