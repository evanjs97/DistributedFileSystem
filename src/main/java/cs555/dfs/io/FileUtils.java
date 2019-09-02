package cs555.dfs.io;

import cs555.dfs.transport.TCPSender;
import cs555.dfs.util.ChunkUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class FileUtils {

	public static void uploadFile(TCPSender sender, RandomAccessFile file, LinkedList<ChunkUtil> locations, int bufferSize, int chunkSize) throws IOException{
		long fileSize = file.length();
		long numSplits = fileSize / bufferSize;
		if(fileSize % bufferSize > 0) numSplits++;


		for(int i = 0; i < numSplits; i++) {

//			sendFile(sender, file, chunkSize, bufferSize);
		}

	}

	public static void uploadFile(TCPSender sender, RandomAccessFile file, LinkedList<ChunkUtil> locations) throws IOException{
		int bufferSize = 64 * 1024;
		int chunkSize = 64 * 1024;
	}

//	public static void sendFile(TCPSender sender, RandomAccessFile file,int chunkSize, int bufferSize) throws IOException{
//		for(int j = 0; j < chunkSize / bufferSize; j++) {
//
//
//		}
//		sender.flush();
//	}

}
