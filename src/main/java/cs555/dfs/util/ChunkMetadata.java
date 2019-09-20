package cs555.dfs.util;


import cs555.dfs.messaging.MessageMarshaller;
import cs555.dfs.messaging.MessageReader;
import sun.security.provider.SHA;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ChunkMetadata {

	private Instant lastModified;
	private int version;
	private final int chunkNum;
	private List<ShardMetadata> shardMetadata = new ArrayList<>();

	private ChunkMetadata(int chunkNum, Instant lastModified, int version) {
		this.lastModified = lastModified;
		this.version = version;
		this.chunkNum = chunkNum;
	}

	public ChunkMetadata(int chunkNum) {
		this.chunkNum = chunkNum;
	}

	public void addShard(ShardMetadata metadata) {
		shardMetadata.add(metadata);
		if(lastModified == null || metadata.getLastModified().isAfter(lastModified)) {
			lastModified = metadata.getLastModified();
		}
		version = Math.max(version, metadata.getVersion());
	}

	public int getVersion() { return this.version; }

	public Instant getLastModified() {
		return this.lastModified;
	}

	public int getChunkNum() { return this.chunkNum; }

	public List<ShardMetadata> getShardMetadata() { return this.shardMetadata; }

	public ChunkMetadata(MessageReader messageReader) {
		Instant lastModified = null;
		int version = 0;
		int chunkNum = 0;
		try {
			chunkNum = messageReader.readInt();
			lastModified = messageReader.readInstant();
			version = messageReader.readInt();
			messageReader.readShardMetadataList(shardMetadata);
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
//		this.filename = filename;
		this.lastModified = lastModified;
		this.version = version;
		this.chunkNum = chunkNum;
	}

	public static ChunkMetadata getFileMetadata(String baseDir, String filename, int chunkNum) {
		try {
			Instant lastModified = Files.getLastModifiedTime(FileSystems.getDefault()
					.getPath(baseDir+filename), LinkOption.NOFOLLOW_LINKS).toInstant();
			return new ChunkMetadata(chunkNum, lastModified,
					getFileVersion(new RandomAccessFile(baseDir+filename+".metadata", "r")));
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	public static int getFileVersion(RandomAccessFile raFile) {
		int version = 1;
		try {
			raFile.seek(0);
			if(raFile.length() > 0) {
				return raFile.readInt();
			}
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return version;
	}

	public static void incrementVersion(String path) {
		try {
			RandomAccessFile raFile = new RandomAccessFile(path, "rw");
			int version = getFileVersion(raFile) +1;
			raFile.seek(0);
			raFile.writeInt(version);
			raFile.setLength(4);
			raFile.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public static long getAvailableDiskSpace(String path) {
		return new File(path).getFreeSpace();
	}

	public static void setLastModifiedTime(String path, Instant time) {
		try {
			Files.setLastModifiedTime(FileSystems.getDefault()
					.getPath(path), FileTime.from(time));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Instant getLastModifiedTime(String path) {
		try {
			return Files.getLastModifiedTime(FileSystems.getDefault()
					.getPath(path), LinkOption.NOFOLLOW_LINKS).toInstant();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public String toString() {

		if(!shardMetadata.isEmpty()) return String.format("Chunk: %d  Shards: %d  version: %d   last modified: %s",
				this.chunkNum, shardMetadata.size(), version, lastModified);
		else return String.format("Chunk: %d    version: %d   last modified: %s",this.chunkNum, version, lastModified);
	}

	public void writeToStream(MessageMarshaller messageMarshaller) throws IOException{
//		messageMarshaller.writeString(filename);
		messageMarshaller.writeInt(chunkNum);
		messageMarshaller.writeInstant(lastModified);
		messageMarshaller.writeInt(version);
		messageMarshaller.writeShardMetadataList(shardMetadata);
	}
}
