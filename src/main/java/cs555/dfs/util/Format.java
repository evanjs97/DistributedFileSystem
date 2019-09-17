package cs555.dfs.util;

public class Format {
	public static String format(String s, int newLength) {
		StringBuilder builder = new StringBuilder(s);
		while(builder.length() < newLength) {
			builder.append(' ');
		}
		return builder.toString();
	}

	public static double formatBytes(double bytes, int digits, String size) {
		String[] byteFormats = { "bytes", "KB", "MB", "GB" };
		int index = 0;
		while(index < byteFormats.length) {
			if (byteFormats[index].equals(size)) break;
			bytes = bytes / 1024;
			index++;
		}

		return bytes;
	}
}
