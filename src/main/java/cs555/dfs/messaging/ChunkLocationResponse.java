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
		byte[] marshalledData;
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());
		dout.writeBoolean(success);

		byte[] hostBytes = hostname.getBytes();
		dout.writeInt(hostBytes.length);
		dout.write(hostBytes);

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
			success = din.readBoolean();
			int hostLength = din.readInt();
			byte[] hostBytes = new byte[hostLength];
			din.readFully(hostBytes);
			hostname = new String(hostBytes);

			port = din.readInt();

			int nameLength = din.readInt();
			byte[] nameBytes = new byte[nameLength];
			din.readFully(nameBytes);
			filename = new String(nameBytes);

			din.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.hostname = hostname;
		this.port = port;
		this.success = success;
		this.filename = filename;
	}
}
