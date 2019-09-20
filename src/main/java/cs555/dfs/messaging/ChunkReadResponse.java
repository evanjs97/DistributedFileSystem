package cs555.dfs.messaging;

import java.io.*;

public class ChunkReadResponse implements Event{

	private final byte[] chunk;
	private final String filename;
	private final boolean success;
	private final int fileSize;

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

	public final int getFileSize() { return this.fileSize; }

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeBoolean(success);
		if(success) {
			messageMarshaller.writeByteArr(chunk);
		}
		messageMarshaller.writeString(filename);
		messageMarshaller.writeInt(fileSize);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkReadResponse(byte[] bytes, String filename, boolean success, int fileSize) {
		this.chunk = bytes;
		this.filename = filename;
		this.success = success;
		this.fileSize = fileSize;
	}

	public ChunkReadResponse(DataInputStream din) {
		byte[] chunk = null;
		String filename = "";
		boolean success = true;
		int fileSize = 0;
		try{
			MessageReader messageReader = new MessageReader(din);
			success = messageReader.readBoolean();
			if(success) {
				chunk = messageReader.readByteArr();
			}
			filename = messageReader.readString();
			fileSize = messageReader.readInt();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.chunk = chunk;
		this.filename = filename;
		this.success = success;
		this.fileSize = fileSize;
	}
}
