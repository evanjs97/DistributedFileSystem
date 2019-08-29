package cs555.dfs.messaging;

import java.io.IOException;

public class ChunkWriteRequest implements Event{
	@Override
	public Type getType() {
		return Type.CHUNK_WRITE_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		return new byte[0];
	}
}
