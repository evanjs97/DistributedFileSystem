package cs555.dfs.messaging;

import java.io.*;

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
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeBoolean(success);
		if(success) {
			messageMarshaller.writeByteArr(chunk);
		}
		messageMarshaller.writeString(filename);
		return messageMarshaller.getMarshalledData();
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
			MessageReader messageReader = new MessageReader(din);
			success = messageReader.readBoolean();
			if(success) {
				chunk = messageReader.readByteArr();
			}
			filename = messageReader.readString();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.chunk = chunk;
		this.filename = filename;
		this.success = success;
	}
}
