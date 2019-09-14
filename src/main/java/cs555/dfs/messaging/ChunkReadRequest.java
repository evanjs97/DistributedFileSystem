package cs555.dfs.messaging;

import java.io.*;

public class ChunkReadRequest implements Event{

	private final int port;
	private final String filename;

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

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), filename, port);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkReadRequest(DataInputStream din) {
		int port = 0;
		String filename = null;
		try {
			MessageReader messageReader = new MessageReader(din);
			filename = messageReader.readString();
			port = messageReader.readInt();
			messageReader.close();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.port = port;
		this.filename = filename;
	}

	public ChunkReadRequest(String filename, int port) {
		this.filename = filename;
		this.port = port;
	}
}
