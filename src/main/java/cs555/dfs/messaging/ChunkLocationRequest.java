package cs555.dfs.messaging;

import java.io.*;

public class ChunkLocationRequest implements Event{

	private final String filename;
	private final int port;


	@Override
	public Type getType() {
		return Type.CHUNK_LOCATION_REQUEST;
	}

	public String getFilename() {
		return this.filename;
	}

	public int getPort() {
		return this.port;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), filename, port);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkLocationRequest(String filename, int port) {
		this.port = port;
		this.filename = filename;
	}

	public ChunkLocationRequest(DataInputStream din) {
		String filename = "";
		int port = 0;
		try {
			MessageReader messageReader = new MessageReader(din);
			filename = messageReader.readString();
			port = messageReader.readInt();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
		this.port = port;

	}
}
