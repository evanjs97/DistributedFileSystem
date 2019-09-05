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
		byte[] marshalledData;

		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());
		dout.writeInt(port);

		byte[] nameBytes = filename.getBytes();
		dout.writeInt(nameBytes.length);
		dout.write(nameBytes);

		dout.flush();
		marshalledData = baOutStream.toByteArray();
		baOutStream.close();
		dout.close();

		return marshalledData;
	}

	public ChunkReadRequest(DataInputStream din) {
		int port = 0;
		String filename = null;
		try {
			port = din.readInt();

			int nameLength = din.readInt();
			byte[] nameBytes = new byte[nameLength];
			din.readFully(nameBytes);
			filename = new String(nameBytes);

			din.close();
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
