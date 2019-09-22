package cs555.dfs.messaging;

import cs555.dfs.erasure.SolomonErasure;
import cs555.dfs.server.ChunkServer;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.util.ChunkMetadata;
import cs555.dfs.util.FileMetadata;
import cs555.dfs.util.Format;
import cs555.dfs.util.ShardMetadata;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ChunkServerHeartbeatTask implements HeartbeatTask{
	private final String destHost;
	private final int destPort;
	private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, HashSet<Integer>>> files;
	private final ChunkServer server;
	private final String BASE_DIR;
	private boolean replication = true;

	public ChunkServerHeartbeatTask(String destHost, int destPort, ChunkServer server,
									ConcurrentHashMap<String, ConcurrentHashMap<Integer, HashSet<Integer>>> files, String baseDir) {
		this.destHost = destHost;
		this.destPort = destPort;
		this.files = files;
		this.server = server;
		this.BASE_DIR = baseDir;
	}




	private List<FileMetadata> getFileMetadata() {
		List<FileMetadata> metadata = new LinkedList<>();
		for(Map.Entry<String, ConcurrentHashMap<Integer, HashSet<Integer>>> entry: files.entrySet()) {
			System.out.println("Getting metadata for: " + entry.getKey());
			if(entry.getKey().charAt(entry.getKey().length()-3) != 'k') {
				replication = false;
			}
			FileMetadata fileMetadata = new FileMetadata(entry.getKey(), replication);
			for(Map.Entry<Integer, HashSet<Integer>> pair : entry.getValue().entrySet()) {
				ChunkMetadata chunkMetadata;
				if(pair.getValue().isEmpty()) {
					 chunkMetadata = ChunkMetadata.getFileMetadata(BASE_DIR,
							entry.getKey() + "_chunk_" + pair.getKey(), pair.getKey());
				}else {
					chunkMetadata = new ChunkMetadata(pair.getKey());
					for(Integer shard : pair.getValue()) {
						chunkMetadata.addShard(ShardMetadata.getFileMetadata(BASE_DIR,
								entry.getKey() + "_chunk_" + pair.getKey() + "_" + shard, shard));
					}
				}
				fileMetadata.addChunk(chunkMetadata);
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
			List<FileMetadata> metadata = getFileMetadata();
			ChunkServerHeartbeat request = new ChunkServerHeartbeat(metadata, server.getPort(), freeSpace, replication);
			sender.sendData(request.getBytes());
			sender.flush();
			server.clearRecentFiles();
//			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	};
}
