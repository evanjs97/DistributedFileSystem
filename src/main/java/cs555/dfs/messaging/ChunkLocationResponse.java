package cs555.dfs.messaging;

import java.io.IOException;

public class ChunkLocationResponse implements Event{
	@Override
	public Type getType() {
		return Type.CHUNK_LOCATION_RESPONSE;
	}

	@Override
	public byte[] getBytes() throws IOException {
		return new byte[0];
	}
}
