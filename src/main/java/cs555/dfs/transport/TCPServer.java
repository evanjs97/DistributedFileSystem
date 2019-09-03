package cs555.dfs.transport;

import cs555.dfs.server.Server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer implements Runnable{
	private ServerSocket serverSocket;
	private int port;
	private Server server;

	/**
	 * TCPServerThread constructor creates new Server thread
	 * @param port to open server socket for, use '0' for automatic allocation
	 */
	public TCPServer(int port, Server server) {
		this.server = server;
		openServerSocket(port);
		this.port = this.serverSocket.getLocalPort();
	}

	/**
	 * openServerSocket opens ServerSocket over port
	 * @param port to open ServerSocket over, pass '0' to automatically allocate
	 */
	private void openServerSocket(int port) {
		try{
			this.serverSocket = new ServerSocket(port, 0, java.net.InetAddress.getLocalHost());
			return;
		}catch(IOException ioe) {
			System.out.println(ioe.getMessage());
		}
	}

	public InetAddress getInetAddress() {
		return serverSocket.getInetAddress();
	}

	public String getAddress() {
		return serverSocket.getInetAddress().getHostAddress();
	}

	public int getLocalPort() {
		return serverSocket.getLocalPort();
	}

	/**
	 * run method for thread
	 * blocks till connection made, then open TCPReceiverThread over that socket
	 */
	@Override
	public void run() {
		while (true) {
			try {
				Socket socket = serverSocket.accept();
				new Thread(new TCPReceiver(socket, server)).start();
			} catch (IOException ioe) {
				System.out.println(ioe.getMessage());
			}
		}
	}
}