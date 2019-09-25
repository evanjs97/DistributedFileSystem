package cs555.dfs.messaging;

import cs555.dfs.erasure.SolomonErasure;
import cs555.dfs.server.ChunkServer;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.util.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ChunkServerHeartbeatTask implements HeartbeatTask{
	private final String destHost;
	private final int destPort;
	private final ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<ChunkServer.ChunkSlice, Boolean>> files;
	private final ChunkServer server;
	private final String BASE_DIR;
	private boolean replication = true;

	public ChunkServerHeartbeatTask(String destHost, int destPort, ChunkServer server,
									ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<ChunkServer.ChunkSlice, Boolean>> files, String baseDir) {
		this.destHost = destHost;
		this.destPort = destPort;
		this.files = files;
		this.server = server;
		this.BASE_DIR = baseDir;
	}




	private List<FileMetadata> getFileMetadata() {
		List<FileMetadata> metadata = new LinkedList<>();
		for(Map.Entry<String, ConcurrentHashMap.KeySetView<ChunkServer.ChunkSlice, Boolean>> entry: files.entrySet()) {
			FileMetadata fileMetadata = new FileMetadata(entry.getKey(), false);
			Iterator<ChunkServer.ChunkSlice> iter = entry.getValue().iterator();
			if(!iter.hasNext())break;
			while(iter.hasNext()) {
				ChunkServer.ChunkSlice pair = iter.next();
				ChunkMetadata chunkMetadata;
				if (pair.getShard() == -1) {
					fileMetadata.setReplication(true);
					chunkMetadata = ChunkMetadata.getFileMetadata(BASE_DIR,
							entry.getKey() + "_chunk_" + pair.getChunk(), pair.getChunk());
				} else {
					chunkMetadata = new ChunkMetadata(pair.getChunk());
					chunkMetadata.addShard(ShardMetadata.getFileMetadata(BASE_DIR,
							entry.getKey() + "_chunk_" + pair.getChunk() + "_" + pair.getShard(), pair.getShard()));
				}

				fileMetadata.addChunk(chunkMetadata);
				iter.remove();
			}
			metadata.add(fileMetadata);

		}
		return metadata;
	}

	@Override
	public void execute() {
		try {
			System.out.println("Sending Heartbeat");
			TCPSender sender = new TCPSender(new Socket(destHost, destPort));
			long space = ChunkMetadata.getAvailableDiskSpace(BASE_DIR);
			double freeSpace = Format.formatBytes(space, 2, "MB");
			List<FileMetadata> metadata = getFileMetadata();
			ChunkServerHeartbeat request = new ChunkServerHeartbeat(metadata, server.getPort(), freeSpace, replication);
			sender.sendData(request.getBytes());
			sender.flush();
//			sender.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	};
}
