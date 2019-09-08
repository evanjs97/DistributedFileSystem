package cs555.dfs.server;

import cs555.dfs.messaging.*;
import cs555.dfs.transport.TCPFileReader;
import cs555.dfs.transport.TCPFileSender;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Path;
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

	private void handleChunkDestinationResponse(ChunkDestinationResponse response) {
		System.out.println("Client: Received chunk destination response");
		if(uploader != null) {
			uploader.addLocationList(response.getLocations());
		}else {
			System.err.println("Client: Unable to handle chunk location response file sender not running");
		}
	}

	private void handleChunkLocationResponse(ChunkLocationResponse response) {
		System.out.println("Client: Received chunk location: " + response.getHostname() + ":" + response.getPort());
		System.out.println("Filename is: " + response.getFilename());
		try {
			TCPSender sender = new TCPSender(new Socket(response.getHostname(), response.getPort()));
			sender.sendData(new ChunkReadRequest(response.getFilename(), port).getBytes());
			sender.flush();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	private void handleChunkReadResponse(ChunkReadResponse response) {
		if(response.isSuccess()) {
			System.out.println("Received bytes from chunk server of length: " + response.getChunk().length);
		}else {
			System.out.println("Failed to retrieve file from chunk server\n" +
					"Chunk Server detected corruption for file: " + response.getFilename()
					+ " in chunks: " + response.getCorruptions().toString());
		}
		reader.addFileBytes(response.getChunk());
	}

	private void handleUserInput() {
		Scanner scan = new Scanner(System.in);
		while(true) {
			while(scan.hasNextLine()) {
				String[] input = scan.nextLine().split("\\s+");
				switch(input[0]) {
					case "upload":
						if(input.length < 2) {
							System.err.println("Error: upload must specify filename");
							break;
						}
						else {
							requestChunkDestinations(input[1]);
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
						return;
				}
			}
		}
	}

	private void requestChunkDestinations(String filename) {
		try {
			Socket socket = new Socket(controllerHostname, controllerPort);
			TCPSender sender = new TCPSender(socket);

			TCPFileSender fileSender = new TCPFileSender(filename);
			this.uploader = fileSender;

			long numChunks = fileSender.getNumChunks();
			Thread thread = new Thread(fileSender);
			thread.start();

			File f = new File(filename);

			for(long i = 0; i < numChunks; i++) {
				this.filenameToChunks.put(f.getAbsolutePath(), numChunks);
				sender.sendData(new ChunkDestinationRequest(this.port).getBytes());
				sender.flush();
			}

			thread.join();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}catch(InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	private void requestChunkLocations(String filename, String destination) {
		Long chunks = this.filenameToChunks.get(filename);
		try {
			Socket socket = new Socket(controllerHostname, controllerPort);
			TCPSender sender = new TCPSender(socket);
			for (long i = 0; i < chunks; i++) {
				sender.sendData(new ChunkLocationRequest(filename+"_chunk_"+i, port).getBytes());
				sender.flush();
			}
			reader = new TCPFileReader(filename, chunks, destination);
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
