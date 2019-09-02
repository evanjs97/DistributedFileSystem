package cs555.dfs.server;

import cs555.dfs.messaging.ChunkLocationRequest;
import cs555.dfs.messaging.ChunkLocationResponse;
import cs555.dfs.messaging.Event;
import cs555.dfs.transport.TCPFileSender;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.transport.TCPServer;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

public class ClientServer implements Server{

	private final String controllerHostname;
	private final int controllerPort;
	private int port;
	private TCPFileSender uploader = null;


	@Override
	public void onEvent(Event event, Socket socket) {
		switch (event.getType()) {
			case CHUNK_LOCATION_RESPONSE:
				handleChunkLocationResponse((ChunkLocationResponse) event);
				break;
			default:
				System.err.println("Client: No event found for request");
				return;
		}
	}

	private void handleChunkLocationResponse(ChunkLocationResponse response) {
		System.out.println("Client: Received chunk location response");
		uploader.addLocationList(response.getLocations());
	}

	private void handleUserInput() {
		Scanner scan = new Scanner(System.in);
		while(true) {
			while(scan.hasNextLine()) {
				String[] input = scan.nextLine().split("\\s+");
				switch(input[0]) {
					case "upload":
						if(input.length < 2) {
							System.err.println("Error: upload must specify filepath");
							return;
						}
						else {
							requestChunkLocations(input[1]);
						}

						break;
//					case "test":
//						requestChunkLocations();
//						break;
					default:
						System.out.println("Invalid Argument: Use 'help' to learn more");
						return;
				}
			}
		}
	}

	private void requestChunkLocations(String filename) {
		try {
			Socket socket = new Socket(controllerHostname, controllerPort);
			TCPSender sender = new TCPSender(socket);

			TCPFileSender fileSender = new TCPFileSender(filename);
			this.uploader = fileSender;

			long numChunks = fileSender.getNumChunks();
			Thread thread = new Thread(fileSender);
			thread.start();

			for(long i = 0; i < numChunks; i++) {
				sender.sendData(new ChunkLocationRequest(this.port).getBytes());
				sender.flush();
			}

			thread.join();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}catch(InterruptedException ie) {
			ie.printStackTrace();
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
