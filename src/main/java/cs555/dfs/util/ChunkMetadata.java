package cs555.dfs.util;


import cs555.dfs.messaging.MessageMarshaller;
import cs555.dfs.messaging.MessageReader;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

public class ChunkMetadata {

//	private final String filename;
	private Instant lastModified;
	private int version;
	private final int chunkNum;

	private ChunkMetadata(int chunkNum, Instant lastModified, int version) {
//		this.filename = filename;
		this.lastModified = lastModified;
		this.version = version;
		this.chunkNum = chunkNum;
	}

//	public String getFilename() {
//		return this.filename;
//	}

	public int getVersion() { return this.version; }

	public Instant getLastModified() {
		return this.lastModified;
	}

	public int getChunkNum() { return this.chunkNum; }

	public ChunkMetadata(MessageReader messageReader) {
//		String filename = "";
		Instant lastModified = null;
		int version = 0;
		int chunkNum = 0;
		try {
//			filename = messageReader.readString();
			chunkNum = messageReader.readInt();
			lastModified = messageReader.readInstant();
			version = messageReader.readInt();

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
		return ""+this.chunkNum;
	}

	public void writeToStream(MessageMarshaller messageMarshaller) throws IOException{
//		messageMarshaller.writeString(filename);
		messageMarshaller.writeInt(chunkNum);
		messageMarshaller.writeInstant(lastModified);
		messageMarshaller.writeInt(version);
	}
}
