package cs555.dfs.util;

import cs555.dfs.messaging.MessageMarshaller;
import cs555.dfs.messaging.MessageReader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.time.Instant;

import static cs555.dfs.util.ChunkMetadata.getFileVersion;

public class ShardMetadata {
	private Instant lastModified;
	private int version;
	private final int shardNum;

	public Instant getLastModified() {
		return lastModified;
	}

	public int getVersion() {
		return version;
	}

	public int getShardNum() {
		return shardNum;
	}

	private ShardMetadata(int shardNum, Instant lastModified, int version) {
//		this.filename = filename;
		this.lastModified = lastModified;
		this.version = version;
		this.shardNum = shardNum;
	}

	public ShardMetadata(MessageReader messageReader) {
//		String filename = "";
		Instant lastModified = null;
		int version = 0;
		int shardNum = 0;
		try {
//			filename = messageReader.readString();
			shardNum = messageReader.readInt();
			lastModified = messageReader.readInstant();
			version = messageReader.readInt();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
//		this.filename = filename;
		this.lastModified = lastModified;
		this.version = version;
		this.shardNum = shardNum;
	}

	public static ShardMetadata getFileMetadata(String baseDir, String filename, int shardNum) {
		try {
			Instant lastModified = Files.getLastModifiedTime(FileSystems.getDefault()
					.getPath(baseDir+filename), LinkOption.NOFOLLOW_LINKS).toInstant();
			return new ShardMetadata(shardNum, lastModified,
					getFileVersion(new RandomAccessFile(baseDir+filename+".metadata", "r")));
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	public void writeToStream(MessageMarshaller messageMarshaller) throws IOException{
//		messageMarshaller.writeString(filename);
		messageMarshaller.writeInt(shardNum);
		messageMarshaller.writeInstant(lastModified);
		messageMarshaller.writeInt(version);
	}
}
