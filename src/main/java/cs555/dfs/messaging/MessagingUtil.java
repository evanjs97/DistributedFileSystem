package cs555.dfs.messaging;

import cs555.dfs.transport.TCPSender;

import java.io.IOException;
import java.net.Socket;

public class MessagingUtil {
	public static void handleChunkLocationResponse(ChunkLocationResponse response, int port) {
		try {
			TCPSender sender = new TCPSender(new Socket(response.getHostname(), response.getPort()));
			sender.sendData(new ChunkReadRequest(response.getFilename(), port).getBytes());
			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
