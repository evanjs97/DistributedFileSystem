package cs555.dfs.messaging;

import cs555.dfs.server.ChunkServer;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.util.ChunkMetadata;
import cs555.dfs.util.FileMetadata;
import cs555.dfs.util.Format;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ChunkServerHeartbeatTask implements HeartbeatTask{
	private final String destHost;
	private final int destPort;
	private final ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> files;
	private final ChunkServer server;
	private final String BASE_DIR;

	public ChunkServerHeartbeatTask(String destHost, int destPort, ChunkServer server,
									ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> files, String baseDir) {
		this.destHost = destHost;
		this.destPort = destPort;
		this.files = files;
		this.server = server;
		this.BASE_DIR = baseDir;
	}


	private List<FileMetadata> getFileMetadata() {
		List<FileMetadata> metadata = new LinkedList<>();
		for(Map.Entry<String, ConcurrentSkipListSet<Integer>> entry: files.entrySet()) {
			System.out.println("Getting metadata for: " + entry.getKey());
			FileMetadata fileMetadata = new FileMetadata(entry.getKey());
			for(Integer chunk : entry.getValue()) {
				fileMetadata.addChunk(ChunkMetadata.getFileMetadata(BASE_DIR,
						entry.getKey()+"_chunk_"+chunk, chunk));
			}
			metadata.add(fileMetadata);
		}
		return metadata;
	}

	@Override
	public void execute() {
		try {
			TCPSender sender = new TCPSender(new Socket(destHost, destPort));
			long space = ChunkMetadata.getAvailableDiskSpace(BASE_DIR);
			double freeSpace = Format.formatBytes(space, 2, "GB");
			ChunkServerHeartbeat request = new ChunkServerHeartbeat(getFileMetadata(), server.getPort(), freeSpace);
			sender.sendData(request.getBytes());
			sender.flush();
			server.clearRecentFiles();
//			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	};
}
