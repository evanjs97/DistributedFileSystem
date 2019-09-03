package cs555.dfs.util;


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

	public FileMetadata(DataInputStream din) {
		String filename = "";
		Instant lastModified = null;
		try {
			int nameLength = din.readInt();
			byte[] nameBytes = new byte[nameLength];
			din.readFully(nameBytes);
			filename = new String(nameBytes);

			int instantLength = din.readInt();
			byte[] instant = new byte[instantLength];
			din.readFully(instant);
			lastModified = Instant.parse(new String(instant));

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
		this.lastModified = lastModified;
	}

	public static FileMetadata getFileMetadata(String filename) {
		try {
			Instant lastModified = Files.getLastModifiedTime(FileSystems.getDefault()
					.getPath(filename), LinkOption.NOFOLLOW_LINKS).toInstant();
			return new FileMetadata(filename, lastModified);
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	public String toString() {
		return this.filename + " last modified at " + lastModified;
	}

	public byte[] getBytes() throws IOException{
		byte[] marshalledData;
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));
		byte[] nameBytes = filename.getBytes();
		dout.writeInt(nameBytes.length);
		dout.write(nameBytes);

		byte[] instant = lastModified.toString().getBytes();
		dout.writeInt(instant.length);
		dout.write(instant);

		dout.flush();
		marshalledData = baOutStream.toByteArray();
		baOutStream.close();
		dout.close();

		return marshalledData;

	}
}
