package cs555.dfs.messaging;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ChunkReadResponse implements Event{

	private final byte[] chunk;
	private final String filename;
	private final boolean success;

	@Override
	public final Type getType() {
		return Type.CHUNK_READ_RESPONSE;
	}

	public final byte[] getChunk() {
		return chunk;
	}

	public final String getFilename() {
		return filename;
	}

	public final boolean isSuccess() { return success; }


	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledData;

		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());

		dout.writeBoolean(success);

		if(success) {
			dout.writeInt(chunk.length);
			dout.write(chunk);
		}

		byte[] nameBytes = filename.getBytes();
		dout.writeInt(nameBytes.length);
		dout.write(nameBytes);

		dout.flush();
		marshalledData = baOutStream.toByteArray();
		baOutStream.close();
		dout.close();

		return marshalledData;
	}

	public ChunkReadResponse(byte[] bytes, String filename, boolean success) {
		this.chunk = bytes;
		this.filename = filename;
		this.success = success;
	}

	public ChunkReadResponse(DataInputStream din) {
		byte[] chunk = null;
		String filename = "";
		boolean success = true;

		try{
			success = din.readBoolean();

			if(success) {
				int chunkSize = din.readInt();
				chunk = new byte[chunkSize];
				din.readFully(chunk);
			}
			int nameLength = din.readInt();
			byte[] nameBytes = new byte[nameLength];
			din.readFully(nameBytes);
			filename = new String(nameBytes);

			din.close();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.chunk = chunk;
		this.filename = filename;
		this.success = success;
	}


}
