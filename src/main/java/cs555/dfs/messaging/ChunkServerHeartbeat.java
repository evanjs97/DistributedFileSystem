package cs555.dfs.messaging;

import cs555.dfs.util.FileMetadata;

import java.io.*;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ChunkServerHeartbeat implements Event{

	private final Type type;
	private final List<FileMetadata> fileInfo;
	private final int port;

	public ChunkServerHeartbeat(List<FileMetadata> fileInfo, Type type, int port) {
		this.type = type;
		this.fileInfo = Collections.unmodifiableList(fileInfo);
		this.port = port;
	}

	public List<FileMetadata> getFileInfo() {
		return fileInfo;
	}

	public int getPort() {
		return port;
	}

	public ChunkServerHeartbeat(DataInputStream din, Type type) {
		List<FileMetadata> fileInfo = new LinkedList<>();
		int port = 0;
		try {
			port = din.readInt();
			int listSize = din.readInt();
			for(int i = 0; i < listSize; i++) {
				fileInfo.add(new FileMetadata(din));
			}

			din.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.fileInfo = fileInfo;
		this.type = type;
		this.port = port;
	}

	@Override
	public Type getType() {
		return this.type;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("Heartbeat " + Instant.now() + ":\n");
		for(FileMetadata metadata : fileInfo) {
			output.append(metadata.toString());
		}
		return output.toString();
	}



	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledData;
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());
		dout.writeInt(port);

		dout.writeInt(fileInfo.size());
		for(FileMetadata metadata : fileInfo) {
			byte[] metaBytes = metadata.getBytes();
			dout.write(metaBytes);
		}

		dout.flush();
		marshalledData = baOutStream.toByteArray();

		baOutStream.close();
		dout.close();


		return marshalledData;

	}

}
