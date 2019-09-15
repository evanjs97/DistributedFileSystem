package cs555.dfs.messaging;

import cs555.dfs.server.ChunkServer;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.util.FileMetadata;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

public class ChunkServerHeartbeatTask implements HeartbeatTask{
	private final String destHost;
	private final int destPort;
	private final ConcurrentSkipListSet<String> files;
	private final ChunkServer server;
	private final String BASE_DIR;

	public ChunkServerHeartbeatTask(String destHost, int destPort, ChunkServer server, ConcurrentSkipListSet<String> files, String baseDir) {
		this.destHost = destHost;
		this.destPort = destPort;
		this.files = files;
		this.server = server;
		this.BASE_DIR = baseDir;
	}

	private List<FileMetadata> getFileMetadata() {
		List<FileMetadata> metadata = new LinkedList<>();
		for(String filename : files) {
			metadata.add(FileMetadata.getFileMetadata(BASE_DIR,filename));
		}
		return metadata;
	}

	@Override
	public void execute() {
		try {
			TCPSender sender = new TCPSender(new Socket(destHost, destPort));
			ChunkServerHeartbeat request = new ChunkServerHeartbeat(getFileMetadata(), server.getPort(), FileMetadata.getAvailableDiskSpace(BASE_DIR));
			sender.sendData(request.getBytes());
			server.clearRecentFiles();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	};
}
