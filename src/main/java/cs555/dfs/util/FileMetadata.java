package cs555.dfs.util;


import cs555.dfs.messaging.MessageMarshaller;
import cs555.dfs.messaging.MessageReader;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

public class FileMetadata {

	private final String filename;
	private Instant lastModified;
	private final int version;

	private FileMetadata(String filename, Instant lastModified, int version) {
		this.filename = filename;
		this.lastModified = lastModified;
		this.version = version;
	}

	public String getFilename() {
		return this.filename;
	}

	public int getVersion() { return this.version; }

	public Instant getLasModified() {
		return this.lastModified;
	}

	public FileMetadata(MessageReader messageReader) {
		String filename = "";
		Instant lastModified = null;
		int version = 0;
		try {
			filename = messageReader.readString();
			lastModified = messageReader.readInstant();
			version = messageReader.readInt();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
		this.lastModified = lastModified;
		this.version = version;
	}

	public static FileMetadata getFileMetadata(String baseDir, String filename) {
		try {
			Instant lastModified = Files.getLastModifiedTime(FileSystems.getDefault()
					.getPath(baseDir+filename), LinkOption.NOFOLLOW_LINKS).toInstant();
			return new FileMetadata(filename, lastModified, getFileVersion(new RandomAccessFile(baseDir+filename+".metadata", "r")));
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
//
//	public static FileMetadata getFileMetadata(String path) {
//
//		Instant lastModified = getLastModifiedTime(path);
//		int version = 0;
//		try {
//			version = getFileVersion(new RandomAccessFile(path+".metadata", "r"));
//		} catch(FileNotFoundException fnfe) {
//			fnfe.printStackTrace();
//		}
//		return new FileMetadata(path.substring(path.lastIndexOf("/")+1), lastModified, version);
//	}

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
		return this.filename + "version: " + version + " last modified at " + lastModified;
	}

	public void writeToStream(MessageMarshaller messageMarshaller) throws IOException{
		messageMarshaller.writeString(filename);
		messageMarshaller.writeInstant(lastModified);
		messageMarshaller.writeInt(version);
	}
}
