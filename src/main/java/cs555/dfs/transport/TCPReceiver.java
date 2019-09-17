package cs555.dfs.transport;

import cs555.dfs.messaging.Event;
import cs555.dfs.messaging.EventFactory;
import cs555.dfs.server.Server;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

public class TCPReceiver implements Runnable {

	private Socket socket;
	private DataInputStream din;
	private Server server;

	/**
	 * TCPReceiverThread creates new receiver thread instance
	 * @param socket the socket to receive messages over
	 * @throws IOException
	 */
	public TCPReceiver(Socket socket, Server server) throws IOException {
		this.server = server;
		this.socket = socket;
		this.din = new DataInputStream(socket.getInputStream());
	}

	/**
	 * run method
	 * reads from socket while not null
	 * send event to EventFactory after reading
	 */
	@Override
	public void run() {
		int dataLength = 0;
		while (socket != null && socket.isConnected()) {
			try {
				dataLength = din.readInt();
				byte[] data = new byte[dataLength];
				din.readFully(data, 0, dataLength);
				Event event = EventFactory.getInstance().getEvent(data);
				server.onEvent(event, socket);
			} catch(EOFException eofe) {
				break;
			} catch(IOException ioe) {
				System.err.println("TCPReceiver: Error while reading from socket");
				ioe.printStackTrace();
			} catch(NegativeArraySizeException ne) {
				System.err.println("TCPReceiver: INVALID SIZE: " + dataLength);
				ne.printStackTrace();
			}
		}
		try {
			socket.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}