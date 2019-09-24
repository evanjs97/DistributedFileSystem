package cs555.dfs.messaging;

import cs555.dfs.transport.TCPSender;

import java.io.IOException;
import java.net.Socket;

public class MessagingUtil {

	public static void handleChunkLocationRequest(TCPSender sender, String filename, int localPort) {
		try {
			sender.sendData(new ChunkLocationRequest(filename, localPort).getBytes());
			sender.flush();
		}catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public static void handleChunkLocationRequest(TCPSender sender, String filename, int localPort, int startChunk, int endChunk) {
		try {
			sender.sendData(new ChunkLocationRequest(filename, startChunk, endChunk, localPort).getBytes());
			sender.flush();
		}catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public static void handleChunkLocationRequest(TCPSender sender, String filename, int localPort, int startChunk, int endChunk, int numShards) {
		try {
			sender.sendData(new ChunkLocationRequest(filename, startChunk, endChunk, localPort, numShards).getBytes());
			sender.flush();
		}catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
