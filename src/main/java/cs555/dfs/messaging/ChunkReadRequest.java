package cs555.dfs.messaging;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ChunkReadRequest implements Event{

	private final int port;
	private final String filename;
	private final boolean replication;
	private final List<Integer> corruptions;

	public int getPort() {
		return port;
	}

	public String getFilename() {
		return filename;
	}

	@Override
	public Type getType() {
		return Type.CHUNK_READ_REQUEST;
	}

	public boolean getReplication() { return this.replication; }

	public final List<Integer> getCorruptions() { return this.corruptions; }

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), filename, port);
		messageMarshaller.writeBoolean(replication);
		messageMarshaller.writeIntList(corruptions);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkReadRequest(DataInputStream din) {
		int port = 0;
		String filename = null;
		boolean replication = true;
		List<Integer> corruptions = new ArrayList<>();
		try {
			MessageReader messageReader = new MessageReader(din);
			filename = messageReader.readString();
			port = messageReader.readInt();
			replication = messageReader.readBoolean();
			messageReader.readIntUtilList(corruptions);
			messageReader.close();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.port = port;
		this.filename = filename;
		this.replication = replication;
		this.corruptions = corruptions;
	}

	public ChunkReadRequest(String filename, int port, List<Integer> corruptions) {
		this.filename = filename;
		this.port = port;
		this.replication = true;
		this.corruptions = corruptions;
	}

	public ChunkReadRequest(String filename, int port, boolean replication) {
		this.filename = filename;
		this.port = port;
		this.replication = replication;
		this.corruptions = new ArrayList<>();
	}

}
