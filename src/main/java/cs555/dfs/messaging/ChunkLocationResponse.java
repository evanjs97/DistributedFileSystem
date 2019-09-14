package cs555.dfs.messaging;

import java.io.*;

public class ChunkLocationResponse implements Event{

	private final String hostname;
	private final int port;
	private final boolean success;
	private final String filename;
	@Override
	public Type getType() {
		return Type.CHUNK_LOCATION_RESPONSE;
	}

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}

	public boolean isSuccess() {
		return success;
	}

	public String getFilename() {
		return this.filename;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), hostname, port);
		messageMarshaller.writeBoolean(success);
		messageMarshaller.writeString(filename);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkLocationResponse(String hostname, int port, boolean success, String filename) {
		this.hostname = hostname;
		this.port = port;
		this.success = success;
		this.filename = filename;
	}

	public ChunkLocationResponse(DataInputStream din) {
		String hostname = "";
		int port = 0;
		boolean success = false;
		String filename = "";
		try {
			MessageReader messageReader = new MessageReader(din);
			hostname = messageReader.readString();
			port = messageReader.readInt();
			success = messageReader.readBoolean();
			filename = messageReader.readString();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.hostname = hostname;
		this.port = port;
		this.success = success;
		this.filename = filename;
	}
}
