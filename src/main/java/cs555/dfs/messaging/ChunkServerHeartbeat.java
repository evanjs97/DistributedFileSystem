package cs555.dfs.messaging;

import cs555.dfs.util.FileMetadata;

import java.io.*;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ChunkServerHeartbeat implements Event{

//	private final Type type;
	private final List<FileMetadata> fileInfo;
	private final int port;
	private final double freeDiskSpace;
	private final boolean replication;

	public ChunkServerHeartbeat(List<FileMetadata> fileInfo, int port, double freeDiskSpace, boolean replication) {
		this.fileInfo = Collections.unmodifiableList(fileInfo);
		this.port = port;
		this.freeDiskSpace = freeDiskSpace;
		this.replication = replication;
	}

	public List<FileMetadata> getFileInfo() {
		return fileInfo;
	}

	public int getPort() {
		return port;
	}

	public double getFreeDiskSpace() { return this.freeDiskSpace; }

	public boolean getReplication() { return this.replication; }

	public ChunkServerHeartbeat(DataInputStream din) {
		List<FileMetadata> fileInfo = new LinkedList<>();
		int port = 0;
		double freeSpace = 0;
		boolean replication = true;
		try {
			MessageReader messageReader = new MessageReader(din);
			port = messageReader.readInt();
			freeSpace = messageReader.readDouble();
			messageReader.readFileMetadataList(fileInfo);
			replication = messageReader.readBoolean();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.fileInfo = fileInfo;
//		this.type = type;
		this.freeDiskSpace = freeSpace;
		this.port = port;
		this.replication = replication;
	}

	@Override
	public Type getType() {
		return Type.CHUNK_SERVER_HEARTBEAT;
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
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeInt(port);
		messageMarshaller.writeDouble(freeDiskSpace);
		messageMarshaller.writeFileMetadataList(fileInfo);
		messageMarshaller.writeBoolean(replication);
		return messageMarshaller.getMarshalledData();
	}

}
