package cs555.dfs.messaging;

import cs555.dfs.transport.TCPSender;

import java.io.IOException;
import java.net.Socket;

public class MessagingUtil {
	public static void handleChunkLocationResponse(ChunkLocationResponse response, int port, boolean replication) {
		try {
			TCPSender sender = new TCPSender(new Socket(response.getHostname(), response.getPort()));
			sender.sendData(new ChunkReadRequest(response.getFilename(), port, replication).getBytes());
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	public static void handleChunkLocationRequest(TCPSender sender, String filename, int localPort) {
		try {
			sender.sendData(new ChunkLocationRequest(filename, localPort).getBytes());
			sender.flush();
		}catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
