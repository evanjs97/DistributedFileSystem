package cs555.dfs.messaging;

import cs555.dfs.server.ChunkServer;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.util.FileMetadata;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

public class HeartbeatTask{
	private final String destHost;
	private final int destPort;
	private final List<String> files;
	private final ChunkServer server;
	private final Event.Type type;

	public HeartbeatTask(String destHost, int destPort, ChunkServer server, Event.Type type) {
		this.destHost = destHost;
		this.destPort = destPort;
		this.files = type == Event.Type.CHUNK_SERVER_MINOR_HEARTBEAT ? server.getRecentFiles() : server.getAllFiles();
		this.server = server;
		this.type = type;
	}

	private List<FileMetadata> getFileMetadata() {
		List<FileMetadata> metadata = new LinkedList<>();
		for(String filename : files) {
			metadata.add(FileMetadata.getFileMetadata(filename));
		}
		return metadata;
	}

	public void execute() {
		try {
			TCPSender sender = new TCPSender(new Socket(destHost, destPort));
			ChunkServerHeartbeat request = new ChunkServerHeartbeat(getFileMetadata(), type);
			sender.sendData(request.getBytes());
			server.clearRecentFiles();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	};
}
