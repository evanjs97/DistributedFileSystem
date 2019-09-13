package cs555.dfs.messaging;

import java.io.*;
import java.time.Instant;

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
//		messageMarshaller.writeInt(getType().getValue());
//		messageMarshaller.writeString(filename);
//		messageMarshaller.writeInt(port);

	}

	public ChunkLocationRequest(String filename, int port) {
		this.port = port;
		this.filename = filename;
	}

	public ChunkLocationRequest(DataInputStream din) {
		String filename = "";
		int port = 0;
		try {
			int nameLength = din.readInt();
			byte[] nameBytes = new byte[nameLength];
			din.readFully(nameBytes);
			filename = new String(nameBytes);

			port = din.readInt();

			din.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
		this.port = port;

	}
}
