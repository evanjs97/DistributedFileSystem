package cs555.dfs.messaging;

import java.io.DataInputStream;
import java.io.IOException;

public class ChunkForwardRequest implements Event{

	private final String filename;
	private final int destPort;
	private final String destHost;

	public String getFilename() {
		return filename;
	}

	public int getDestPort() {
		return destPort;
	}

	public String getDestHost() {
		return destHost;
	}

	@Override
	public Type getType() {
		return Type.CHUNK_FORWARD_REQUEST;
	}

	public ChunkForwardRequest(String filename, String destHost, int destPort) {
		this.filename = filename;
		this.destHost = destHost;
		this.destPort = destPort;
	}

	public ChunkForwardRequest(DataInputStream din) {
		MessageReader messageReader = new MessageReader(din);
		String filename = "";
		int destPort = 0;
		String destHost = "";
		try {
			destHost = messageReader.readString();
			destPort = messageReader.readInt();
			filename = messageReader.readString();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
		this.destPort = destPort;
		this.destHost = destHost;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), destHost, destPort);
		messageMarshaller.writeString(filename);

		return messageMarshaller.getMarshalledData();
	}
}
