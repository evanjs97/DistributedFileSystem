package cs555.dfs.messaging;

import java.io.*;

public class ChunkReadRequest implements Event{

	private final int port;
	private final String filename;
	private final boolean replication;

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

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), filename, port);
		messageMarshaller.writeBoolean(replication);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkReadRequest(DataInputStream din) {
		int port = 0;
		String filename = null;
		boolean replication = true;
		try {
			MessageReader messageReader = new MessageReader(din);
			filename = messageReader.readString();
			port = messageReader.readInt();
			replication = messageReader.readBoolean();
			messageReader.close();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.port = port;
		this.filename = filename;
		this.replication = replication;
	}

	public ChunkReadRequest(String filename, int port) {
		this.filename = filename;
		this.port = port;
		this.replication = true;
	}

	public ChunkReadRequest(String filename, int port, boolean replication) {
		this.filename = filename;
		this.port = port;
		this.replication = replication;
	}

}
