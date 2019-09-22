package cs555.dfs.server;

import cs555.dfs.erasure.SolomonErasure;
import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPFileReader;
import cs555.dfs.transport.TCPFileSender;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;
import cs555.dfs.util.ChunkUtil;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class ClientServer implements Server{

	private final String controllerHostname;
	private final int controllerPort;
	private int port;
	private TCPFileSender uploader = null;
	private ConcurrentHashMap<String, Long> filenameToChunks = new ConcurrentHashMap<>();
	private TCPFileReader reader = null;
	private boolean replication;
	private int NUM_FILE_DESTINATIONS;
	private static final int BATCH_SIZE = 1000;
	private HashMap<String, TCPSender> senders = new HashMap<>();


	@Override
	public void onEvent(Event event, Socket socket) {
		switch (event.getType()) {
			case CHUNK_DESTINATION_RESPONSE:
				handleChunkDestinationResponse((ChunkDestinationResponse) event);
				break;
			case CHUNK_LOCATION_RESPONSE:
				handleChunkLocationResponse((ChunkLocationResponse) event);
				break;
			case CHUNK_READ_RESPONSE:
				handleChunkReadResponse((ChunkReadResponse) event);
				break;
			default:
				System.err.println("Client: No event found for request");
				return;
		}
	}

	private void handleChunkLocationResponse(ChunkLocationResponse response) {
		List<ChunkUtil> locations = response.getLocations();
		int serverIndex = 0;
		String base = response.getFilename();

		for(int i = response.getStartChunk(); i < response.getEndChunk(); i++) {
			try {
				ChunkUtil server;
				if(response.getNumShards() != 0) {

					for(int j = 0; j < response.getNumShards(); j++) {
						server = locations.get(serverIndex);
						if(server.getPort() == 0 || server.getHostname().equals("")) {
							serverIndex++;
							continue;
						}
						senders.putIfAbsent(server.toString(), new TCPSender(new Socket(server.getHostname(), server.getPort())));
						serverIndex++;
						TCPSender sender = senders.get(server.toString());
						sender.sendData(new ChunkReadRequest(base+i+"_"+j, port, replication).getBytes());
						sender.flush();
					}
				}else {
					server = locations.get(serverIndex);
					if(server.getPort() == 0 || server.getHostname().equals("")) {
						serverIndex++;
						continue;
					}
					senders.putIfAbsent(server.toString(), new TCPSender(new Socket(server.getHostname(), server.getPort())));
					TCPSender sender = senders.get(server.toString());
					sender.sendData(new ChunkReadRequest(base+i, port, replication).getBytes());
					sender.flush();
					serverIndex++;
				}

			}catch(IOException ioe) {
				ioe.printStackTrace();
			}
		}

	}

	/**
	 * Handles destination response from the controller
	 * the locations list from the response is sent to the file uploader
	 * @param response the response received from the controller
	 */
	private void handleChunkDestinationResponse(ChunkDestinationResponse response) {
		if(uploader != null) {
			int index = Integer.parseInt(response.getFilename().substring(response.getFilename().lastIndexOf('_')+1));
			uploader.addLocationList(response.getLocations(), index);
		}else {
			System.err.println("Client: Unable to handle chunk location response file sender not running");
		}
	}

	/**
	 * handleChunkReadResponse is responsible for dealing with incoming chunk data
	 * 	this method will send the chunk data to the file reader which will write it to file
	 * In the case of a file corruption on the chunk it will inform the client
	 * @param response the response received from the chunk server
	 */
	private void handleChunkReadResponse(ChunkReadResponse response) {

		if(!response.isSuccess()) {
			System.out.println("Chunk Server detected corruption for file: " + response.getFilename() + " trying to read again.");
			try {
				int index = response.getFilename().lastIndexOf("_chunk_");
				int chunk = Integer.parseInt(response.getFilename().substring(index+7));
//				System.out.println("Chunk to repair" + chunk);
				String fileRepair = response.getFilename().substring(0, index+7);
//				System.out.println("File to repair" + fileRepair);
				Socket socket = new Socket(controllerHostname, controllerPort);
				TCPSender sender = new TCPSender(socket);
				MessagingUtil.handleChunkLocationRequest(sender, fileRepair, port, chunk, chunk+1);
				socket.close();
			}catch(IOException ioe) {
				ioe.printStackTrace();
			}
		} else if(reader != null){
			if(replication) {
				int index = Integer.parseInt(response.getFilename().substring(response.getFilename().lastIndexOf('_') + 1));
				synchronized (reader) {
					reader.addFileBytes(response.getChunk(), index);
				}
			}else {
				int shard = Integer.parseInt(response.getFilename().substring(response.getFilename().lastIndexOf('_') + 1));
				String filename = response.getFilename().substring(0, response.getFilename().lastIndexOf('_'));
				int index = Integer.parseInt(filename.substring(filename.lastIndexOf('_') + 1));
				reader.addShardBytes(response.getChunk(), shard, index, response.getFileSize());
			}
		}
	}

	private void handleUserInput() {
		Scanner scan = new Scanner(System.in);
		while(true) {
			while(scan.hasNextLine()) {
				String[] input = scan.nextLine().split("\\s+");
				switch(input[0]) {
					case "put":
						if(input.length < 3) {
							System.err.println("Error: upload must specify filename and destination path");
							break;
						}
						else {
							requestChunkDestinations(input[1], input[2]);
						}
						break;
					case "get":
						if(input.length < 3) {
							System.err.println("Error: get must specify filename and destination");
							break;
						}
						else {
							requestChunkLocations(input[1], input[2]);
						}
						break;
					case "set":
						switch(input[1]) {
							case "mode":
								replication = !input[2].equals("erasure");
								NUM_FILE_DESTINATIONS = (input[2].equals("erasure") ? SolomonErasure.TOTAL_SHARDS : 3);
								System.out.println("Mode changed to: " + input[2]);
								break;
							default:
								break;
						}
						break;
					default:
						System.out.println("Invalid Argument: Use 'help' to learn more");
						break;
				}
			}
		}
	}

	/**
	 * Requests chunk destinations for input file from the controller server
	 * Starts up file uploader to upload the chunks to chunk servers
	 * @param filename the file to be uploaded
	 * @param destination the file destination (on the chunk servers)
	 */
	private void requestChunkDestinations(String filename, String destination) {
		try {
			Socket socket = new Socket(controllerHostname, controllerPort);
			TCPSender sender = new TCPSender(socket);

			if(filename.contains("/")) {
				destination += filename.substring(filename.lastIndexOf('/')+1);
			}

			TCPFileSender fileSender = new TCPFileSender(filename, destination, replication);
			this.uploader = fileSender;

			long numChunks = fileSender.getNumChunks();

			for(long i = 0; i < numChunks; i++) {
				this.filenameToChunks.put(destination, numChunks);
				sender.sendData(new ChunkDestinationRequest(this.port, destination+"_chunk_"+i,
						NUM_FILE_DESTINATIONS).getBytes());
				sender.flush();
			}
			socket.close();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * Requests chunk locations for already uploaded file from the controller
	 * @param filename the location of the file on the chunk server
	 * @param destination the destination for the file on the client
	 */
	private void requestChunkLocations(String filename, String destination) {
		if(this.filenameToChunks.get(filename) == null) {
			System.out.println("File not found");
			return;
		}
		int chunks = this.filenameToChunks.get(filename).intValue();
		try {
			Socket socket = new Socket(controllerHostname, controllerPort);
			TCPSender sender = new TCPSender(socket);
			reader = new TCPFileReader(filename, chunks, destination, replication);
			String name = filename + "_chunk_";
			for (int i = 0; i < chunks; i+=BATCH_SIZE) {
				int start = i;
				int end = Math.min(i+BATCH_SIZE, chunks);
				if(!replication) {
					MessagingUtil.handleChunkLocationRequest(sender, name, port, start, end, SolomonErasure.TOTAL_SHARDS);
				}else {
					MessagingUtil.handleChunkLocationRequest(sender, name, port, start, end);
				}
			}
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}


	public ClientServer(String controllerHostname, int controllerPort, int clientPort, boolean replication) {
		this.controllerHostname = controllerHostname;
		this.controllerPort = controllerPort;
		this.port = clientPort;
		this.replication = replication;
		if(replication) NUM_FILE_DESTINATIONS = 3;
		else NUM_FILE_DESTINATIONS = SolomonErasure.TOTAL_SHARDS;
		System.out.println("Creating client with default mode : " + (replication ? "replication" : "erasure"));
	}

	/**
	 * Initializes client server on specified port
	 */
	private void init() {
		TCPServer tcpServer = new TCPServer(port, this);
		this.port = tcpServer.getLocalPort();
		Thread server = new Thread(tcpServer);
		server.start();
	}

	public static void main(String[] args) {
		boolean replication = true;
		if(args.length < 2) {
			System.err.println("Error: Must specify at least 3 arguments");
			System.exit(1);
		}
		try {
			String hostname = args[0];
			int hostPort = Integer.parseInt(args[1]);
			int clientPort = 0;
			if(hostPort < 1024 || hostPort > 65535) {
				throw new NumberFormatException();
			}
			if(args.length > 2) {
				if(args[2].equals("erasure")) {
					replication = false;
				}
				if(args.length > 3) {
					clientPort = Integer.parseInt(args[3]);
					if(clientPort < 1024 || clientPort > 65535 || hostPort == clientPort) {
						throw new NumberFormatException();
					}


				}
			}

			ClientServer client = new ClientServer(hostname, hostPort, clientPort, replication);
			client.init();
			client.handleUserInput();
		}catch(NumberFormatException nfe) {
			System.err.println("Error: Must specify valid port");
		}

	}
}
