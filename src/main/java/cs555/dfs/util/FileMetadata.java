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

	private FileMetadata(String filename, Instant lastModified) {
		this.filename = filename;
		this.lastModified = lastModified;
	}

	public String getFilename() {
		return this.filename;
	}

	public Instant getLasModified() {
		return this.lastModified;
	}

	public FileMetadata(MessageReader messageReader) {
		String filename = "";
		Instant lastModified = null;
		try {
//			int nameLength = din.readInt();
//			byte[] nameBytes = new byte[nameLength];
//			din.readFully(nameBytes);
//			filename = new String(nameBytes);
			filename = messageReader.readString();

//			int instantLength = din.readInt();
//			byte[] instant = new byte[instantLength];
//			din.readFully(instant);
//			lastModified = Instant.parse(new String(instant));
			lastModified = messageReader.readInstant();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
		this.lastModified = lastModified;
	}

	public static FileMetadata getFileMetadata(String baseDir, String filename) {
		try {
			Instant lastModified = Files.getLastModifiedTime(FileSystems.getDefault()
					.getPath(baseDir+filename), LinkOption.NOFOLLOW_LINKS).toInstant();
			return new FileMetadata(filename, lastModified);
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	public static FileMetadata getFileMetadata(String path) {
		Instant lastModified = getLastModifiedTime(path);
		return new FileMetadata(path.substring(path.lastIndexOf("/")+1), lastModified);
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
		return this.filename + " last modified at " + lastModified;
	}

	public byte[] getBytes() throws IOException{
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeString(filename);
		messageMarshaller.writeInstant(lastModified);
		return messageMarshaller.getMarshalledData();
//		byte[] marshalledData;
//		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
//		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));
//		byte[] nameBytes = filename.getBytes();
//		dout.writeInt(nameBytes.length);
//		dout.write(nameBytes);

//		byte[] instant = lastModified.toString().getBytes();
//		dout.writeInt(instant.length);
//		dout.write(instant);

//		dout.flush();
//		marshalledData = baOutStream.toByteArray();
//		baOutStream.close();
//		dout.close();
//
//		return marshalledData;

	}
}
