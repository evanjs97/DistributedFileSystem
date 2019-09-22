package cs555.dfs.messaging;

import java.io.DataInputStream;
import java.io.IOException;

public class FileListRequest implements Event{
	@Override
	public Type getType() {
		return Type.FILE_LIST_REQUEST;
	}

	private final int port;

	public FileListRequest(int port) {
		this.port = port;
	}

	public FileListRequest(DataInputStream din) {
		MessageReader messageReader = new MessageReader(din);
		int port = 0;
		try {
			port = messageReader.readInt();
			messageReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.port = port;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeInt(port);
		return messageMarshaller.getMarshalledData();
	}
}
