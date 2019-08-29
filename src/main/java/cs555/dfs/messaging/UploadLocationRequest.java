package cs555.dfs.messaging;

import java.io.IOException;

public class UploadLocationRequest implements Event{
	@Override
	public Type getType() {
		return Type.UPLOAD_LOCATION_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		return new byte[0];
	}
}
