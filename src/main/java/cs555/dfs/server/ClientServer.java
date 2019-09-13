package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPFileReader;
import cs555.dfs.transport.TCPFileSender;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;

public class ClientServer implements Server{

	private final String controllerHostname;
	private final int controllerPort;
	private int port;
	private TCPFileSender uploader = null;
	private HashMap<String, Long> filenameToChunks = new HashMap<>();
	private TCPFileReader reader = null;


	@Override
	public void onEvent(Event event, Socket socket) {
		switch (event.getType()) {
			case CHUNK_DESTINATION_RESPONSE:
				handleChunkDestinationResponse((ChunkDestinationResponse) event);
				break;
			case CHUNK_LOCATION_RESPONSE:
				MessagingUtil.handleChunkLocationResponse((ChunkLocationResponse) event, port);
				break;
			case CHUNK_READ_RESPONSE:
				handleChunkReadResponse((ChunkReadResponse) event);
				break;
			default:
				System.err.println("Client: No event found for request");
				return;
		}
	}

	/**
	 * Handles destination response from the controller
	 * the locations list from the response is sent to the file uploader
	 * @param response the response received from the controller
	 */
	private void handleChunkDestinationResponse(ChunkDestinationResponse response) {
		if(uploader != null) {
			uploader.addLocationList(response.getLocations());
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
			System.out.println("Failed to retrieve file from chunk server\n" +
					"Chunk Server detected corruption for file: " + response.getFilename());
		}
		reader.addFileBytes(response.getChunk());
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

			TCPFileSender fileSender = new TCPFileSender(filename, destination);
			this.uploader = fileSender;

			long numChunks = fileSender.getNumChunks();
			Thread thread = new Thread(fileSender);
			thread.start();

			for(long i = 0; i < numChunks; i++) {
				synchronized (filenameToChunks) {
					this.filenameToChunks.put(destination, numChunks);
				}
				sender.sendData(new ChunkDestinationRequest(this.port).getBytes());
				sender.flush();
			}
			socket.close();
			thread.join();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}catch(InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	/**
	 * Requests chunk locations for already uploaded file from the controller
	 * @param filename the location of the file on the chunk server
	 * @param destination the destination for the file on the client
	 */
	private void requestChunkLocations(String filename, String destination) {
		Long chunks;
		synchronized (filenameToChunks) {
			chunks = this.filenameToChunks.get(filename);
		}
		try {
			Socket socket = new Socket(controllerHostname, controllerPort);
			TCPSender sender = new TCPSender(socket);
			reader = new TCPFileReader(filename, chunks, destination);
			for (long i = 0; i < chunks; i++) {
				MessagingUtil.handleChunkLocationRequest(sender, filename+"_chunk_"+i, port);
			}
			reader.readFile();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}


	public ClientServer(String controllerHostname, int controllerPort, int clientPort) {
		this.controllerHostname = controllerHostname;
		this.controllerPort = controllerPort;
		this.port = clientPort;
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
		if(args.length < 2) {
			System.err.println("Error: Must specify at least 2 arguments");
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
				clientPort = Integer.parseInt(args[2]);
				if(clientPort < 1024 || clientPort > 65535 || hostPort == clientPort) {
					throw new NumberFormatException();
				}
			}

			ClientServer client = new ClientServer(hostname, hostPort, clientPort);
			client.init();
			client.handleUserInput();
		}catch(NumberFormatException nfe) {
			System.err.println("Error: Must specify valid port");
		}

	}
}
