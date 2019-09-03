package cs555.dfs.messaging;

import cs555.dfs.transport.TCPSender;
import cs555.dfs.util.FileMetadata;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

public class ChunkServerMajorHeartbeat extends HeartbeatTask{

	private final String destHost;
	private final int destPort;
	private final List<String> files;

	public ChunkServerMajorHeartbeat(String destHost, int destPort, List<String> files) {
		this.destHost = destHost;
		this.destPort = destPort;
		this.files = files;
	}

	private List<FileMetadata> getFileMetadata() {
		List<FileMetadata> metadata = new LinkedList<>();
		for(String filename : files) {
			metadata.add(FileMetadata.getFileMetadata(filename));
		}
		return metadata;
	}

	@Override
	public void execute() {
		try {
			TCPSender sender = new TCPSender(new Socket(destHost, destPort));
			ChunkServerHeartbeat request = new ChunkServerHeartbeat(getFileMetadata(), Event.Type.CHUNK_SERVER_MAJOR_HEARTBEAT);
			sender.sendData(request.getBytes());
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
