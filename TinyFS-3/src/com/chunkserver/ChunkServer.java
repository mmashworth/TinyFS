package com.chunkserver;

import java.io.File;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.io.FileNotFoundException;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;


import com.client.Client;
import com.interfaces.ChunkServerInterface;
import com.master.Master;
import com.client.RID;
import com.client.FileHandle;
import com.client.ClientFS.FSReturnVals;
import com.client.TinyRec;


/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String rootFilePath = "TinyFS-3/csci485/";	//or C:\\newfile.txt
	public final static String ClientConfigFile = "TinyFS-3/ClientConfig.txt";
	
	//Used for the file system
	public static long counter;
	
	public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer  
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
	
	public static final int AppendRecordCMD = 104;
	public static final int DeleteRecordCMD = 105;
	public static final int ReadFirstRecordCMD = 106;
	public static final int ReadLastRecordCMD = 107;
	public static final int ReadNextRecordCMD = 108;
	public static final int ReadPrevRecordCMD = 109;
	
	public static final int ReadFirstRecordOfChunkCMD = 110;
	
	//Replies provided by the server
	public static final int TRUE = 1;
	public static final int FALSE = 0;
	
	//map from rootFilePath to its chunk handles
	//chunk handle = filename + slot number/index
	Map<String, List<String>> fileToChunks;
	
	//maps from a file's chunk to a linked list of its record
	//KEY: filepath concatenated with the chunk number
	Map<String, LinkedList<RID>> chunkToRecs;
	
	public static final int FileHeaderLength = 4;
	public static final int CHUNK_SIZE = 4096;
	
	
	/*
	 * Sent from ClientRec,
	 * Append payload to the File with fh
	 * 
	 * Returns BadHandle if ofh is invalid 
	 * Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 */
	public FSReturnVals chunkServerAppendRecord(FileHandle fh, byte[] payload, RID recordID) {	
		if(!recordID.invalid()) return FSReturnVals.BadRecID;
		if(payload.length > CHUNK_SIZE - 8) return FSReturnVals.RecordTooLong;
				
		String totalFilepath = rootFilePath + removeFirstSlash(fh.getFileDir());
		String filepath = fh.getFileDir() + fh.getFileName();
		String filename = fh.getFileName();
		
		//if the file has no chunks, add it to the list
		if(!fileToChunks.containsKey(filepath)) {
			fileToChunks.put(filepath, new ArrayList<>());		
		}
		
		RandomAccessFile currFile = getFile(totalFilepath, filename);
		List<String> fileChunks = fileToChunks.get(filepath);
		int numChunks = fileChunks.size();

		if(numChunks == 0) { //this file has no chunks yet
			return appendRecordToEmptyChunk(currFile, payload, recordID, filepath, 1);
		}
		
		//if the last chunk has space append record to that, otherwise add it to a new chunk
		String lastChunk = fileChunks.get(fileChunks.size()-1);
		try {
			int chunkOffset = (numChunks-1)*CHUNK_SIZE;
			//System.out.println("chunk offset: " + chunkOffset);
			currFile.seek(4 + chunkOffset); //get location of first free record
			
		
			int nextFreeRecordOffset = readIntAtOffset(currFile, 4+chunkOffset);
			//System.out.println("First free record is at: " + nextFreeRecordOffset);
			
			//read value at firstFreeRecord+4
				//if this value is >= payload.length+8, insert the record here at offset
			int prevPointerOffset = 4+chunkOffset;
			while(nextFreeRecordOffset != -1) { //112
				//System.out.println("next free record offset -->" + nextFreeRecordOffset);
				int freeRecordSpace = readIntAtOffset(currFile, nextFreeRecordOffset+4);
				//System.out.println("Still have " + freeRecordSpace + " bytes of free space");
				
				
				if(freeRecordSpace >= payload.length + 8) {
					int nextOpenSpace = readIntAtOffset(currFile, nextFreeRecordOffset);

					writeIntAtOffset(currFile, prevPointerOffset, nextFreeRecordOffset + payload.length);
					currFile.seek(nextFreeRecordOffset);
					currFile.write(payload);
					writeIntAtOffset(currFile, nextFreeRecordOffset+payload.length, nextOpenSpace);
					writeIntAtOffset(currFile, nextFreeRecordOffset+payload.length+4, freeRecordSpace-payload.length);
					
					recordID.setFilepath(filepath);
					recordID.setChunk(Integer.toString(numChunks));
					recordID.setOffset(nextFreeRecordOffset);
					recordID.setLength(payload.length);
					
					int numRecords = readIntAtOffset(currFile, chunkOffset) + 1;
					writeIntAtOffset(currFile, chunkOffset, numRecords);
					
					System.out.println("adding another record in at offset" + nextFreeRecordOffset);
					System.out.println("adding it to chunk: " + Integer.toString(fileChunks.size()));
					insertRecord(filepath, Integer.toString(fileChunks.size()), recordID);
					
					return FSReturnVals.Success;
				}
				
				prevPointerOffset = nextFreeRecordOffset;
				nextFreeRecordOffset = readIntAtOffset(currFile, nextFreeRecordOffset);

			}
		} catch(IOException ioe) {}
		
		return appendRecordToEmptyChunk(currFile, payload, recordID, filepath, fileChunks.size()+1);
	}
	
	public FSReturnVals appendRecordToEmptyChunk(RandomAccessFile currFile, byte[] payload, RID recordID, String filepath, int chunkNum) {
		System.out.println("------CREATING NEW CHUNK------");
		try {
			int chunkOffset = CHUNK_SIZE*(chunkNum-1);
			//allocate a space of size CHUNK_SIZE in the file
			currFile.setLength(CHUNK_SIZE*chunkNum);
			//get the starting location of that chunk
			currFile.seek(chunkOffset);
			//write 1 to the first byte (num records in the chunk)
			byte[] numRecords = ByteBuffer.allocate(4).putInt(1).array();
			currFile.write(numRecords);
			//write 4 + 4 + payload.length to the next four bytes (points to next free record)
			currFile.seek(4 + chunkOffset);
			byte[] nextFreeRecord = ByteBuffer.allocate(4).putInt(4+4+payload.length+chunkOffset).array();
			currFile.write(nextFreeRecord);
			//write payload to the next payload.length bytes
			currFile.seek(8 + chunkOffset);
			currFile.write(payload);
			//write null/-1 at 4 + 4 + payload.length (last free record, doesn't point to anything)
			currFile.seek(4+4+payload.length + chunkOffset);
			byte[] lastRecord = ByteBuffer.allocate(4).putInt(-1).array();
			currFile.write(lastRecord);
			//write CHUNK_SIZE-4-4-payload.length (free space remaining)
			currFile.seek(4+4+4+payload.length + chunkOffset);
			byte[] remainingSpace = ByteBuffer.allocate(4).putInt(CHUNK_SIZE-payload.length-4-4).array();
			currFile.write(remainingSpace);
		} catch(IOException ioe) { 
			System.out.println("append record ioe: " + ioe.getMessage()); 
			return FSReturnVals.Fail;
		}
		fileToChunks.get(filepath).add(Integer.toString(chunkNum)); //first chunk for this file
		recordID.setFilepath(filepath);
		recordID.setChunk(Integer.toString(chunkNum));
		recordID.setOffset(8 + (chunkNum-1)*CHUNK_SIZE);
		recordID.setLength(payload.length);
		
		LinkedList<RID> records = new LinkedList<>();
		records.add(recordID);
		System.out.println("added first record to this new chunk at offset: " + (CHUNK_SIZE*(chunkNum-1)+8));
		chunkToRecs.put(filepath + Integer.toString(chunkNum), records);
		
		return FSReturnVals.Success;
	}
	
	/*
	 * Returns BadHandle if ofh is invalid 
	 * Returns BadRecID if the specified RID is not valid 
	 * Returns RecDoesNotExist if the record specified by RecordID does not exist.
	 */
	public FSReturnVals chunkServerDeleteRecord(FileHandle fh, RID recordID) {
		String filepath = fh.getFileDir() + fh.getFileName();
		int chunkNum = Integer.parseInt( recordID.getChunk() );
		String chunkHandle = filepath + Integer.toString(chunkNum);
		//check that filepath exists in fileToChunks, if not return BadHandle
		if(!fileToChunks.containsKey(filepath)) return FSReturnVals.BadHandle;
		
		//check rid is valid by seeing if it exists with chunkToRecs
		if(!chunkToRecs.containsKey(chunkHandle)) return FSReturnVals.BadRecID;
		
		
		//need to remove the record id from the chunkToRecs mapping
		//need to free up that record in the file
			//call method insert free record
				//input: chunkHandle, offset of new free record & length of it
		
		System.out.println("old size: " + chunkToRecs.get(chunkHandle).size());
		chunkToRecs.get(chunkHandle).remove(recordID);
		System.out.println("new size: " + chunkToRecs.get(chunkHandle).size());

		freeRecord(filepath, chunkNum, recordID);
		return FSReturnVals.Success;
	}
	
	public void freeRecord(String filepath, int chunkNum, RID rid) {
		System.out.println("offset --> " + rid.getOffset());
		int chunkOffset = (chunkNum-1) * CHUNK_SIZE;
		long offset = rid.getOffset();
		int length = (int) rid.getLength();
		
		//things to change:
			//pointer with the largest offset less than rid's offset (r1) now points to rid's offset
			//value at rid's offset now points to whatever r1's value was
			//value at rid's offset + 4 is now length
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(rootFilePath + filepath, "rw");
		} catch(FileNotFoundException fnfe) { System.out.println("free record fnfe: " + fnfe.getMessage());}
		
		int prevPointer = 4 + chunkOffset;
		int nextPointer = 4 + chunkOffset;
		while(nextPointer < offset) {
			prevPointer = nextPointer;
			System.out.println("free record at --> " + prevPointer);
			nextPointer = readIntAtOffset(raf, prevPointer);
		}
		
//		System.out.println("prevPointer: " + prevPointer);
//		System.out.println("nextPointer: " + nextPointer);
		
		int tmp = readIntAtOffset(raf, prevPointer);
		writeIntAtOffset(raf, prevPointer, (int) offset);
		writeIntAtOffset(raf, (int) offset, tmp);
		writeIntAtOffset(raf, (int) (offset+4), length);
	}
	
	public FSReturnVals chunkServerReadFirstRecord(FileHandle fh, TinyRec rec, int chunkNum) {
		System.out.println("----READING FIRST RECORD OF CHUNK " + chunkNum + "----");
		String filepath = fh.getFileDir() + fh.getFileName();
		String chunkHandle = filepath + chunkNum;
		
		//file has no chunks yet
		if(!chunkToRecs.containsKey(chunkHandle)) return FSReturnVals.RecDoesNotExist;
		//file had chunks but they were all deleted
		if(chunkToRecs.get(chunkHandle).size() == 0) return FSReturnVals.RecDoesNotExist;
				
		//fill rec with the first record of the first chunk
		RID recordID = chunkToRecs.get(chunkHandle).getFirst();
		
		byte[] payload = new byte[(int) recordID.getLength()];
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(rootFilePath + filepath, "r");
		} catch (FileNotFoundException fnfe) { System.out.println("CS read first record fnfe: " + fnfe.getMessage()); }
		try {
			raf.seek(recordID.getOffset());
			raf.read(payload, 0, (int) recordID.getLength());
		} catch(IOException ioe) { System.out.println("CS read first record ioe: " + ioe.getMessage()); }
		
		rec.setRID(recordID);
		rec.setPayload(payload);
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals chunkServerReadLastRecord(FileHandle fh, TinyRec rec) {
		System.out.println("----READING LAST RECORD OF LAST CHUNK----");
		String filepath = fh.getFileDir() + fh.getFileName();
		String numChunks = Integer.toString(  fileToChunks.get(filepath).size()  );
		
		String chunkHandle = filepath + numChunks;
		
		//file has no chunks yet
		if(!chunkToRecs.containsKey(chunkHandle)) return FSReturnVals.RecDoesNotExist;
		//chunk had records but they were all deleted
		if(chunkToRecs.get(chunkHandle).size() == 0) return FSReturnVals.RecDoesNotExist;
				
		//fill rec with the first record of the first chunk
		RID recordID = chunkToRecs.get(chunkHandle).getLast();
		
		byte[] payload = new byte[(int) recordID.getLength()];
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(rootFilePath + filepath, "r");
		} catch (FileNotFoundException fnfe) { System.out.println("CS read first record fnfe: " + fnfe.getMessage()); }
		try {
			raf.seek(recordID.getOffset());
			raf.read(payload, 0, (int) recordID.getLength());
		} catch(IOException ioe) { System.out.println("CS read first record ioe: " + ioe.getMessage()); }
		
		rec.setRID(recordID);
		rec.setPayload(payload);
		
		return FSReturnVals.Success;
	}
	public FSReturnVals chunkServerReadLastRecord(FileHandle fh, TinyRec rec, int chunkNum) {
		System.out.println("----READING LAST RECORD OF SOME CHUNK----");
		String filepath = fh.getFileDir() + fh.getFileName();
		String chunkHandle = filepath + chunkNum;
		
		//file has no chunks yet
		if(!chunkToRecs.containsKey(chunkHandle)) return FSReturnVals.RecDoesNotExist;
		//chunk had records but they were all deleted
		if(chunkToRecs.get(chunkHandle).size() == 0) return FSReturnVals.RecDoesNotExist;
				
		//fill rec with the first record of the first chunk
		RID recordID = chunkToRecs.get(chunkHandle).getLast();
		
		byte[] payload = new byte[(int) recordID.getLength()];
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(rootFilePath + filepath, "r");
		} catch (FileNotFoundException fnfe) { System.out.println("CS read first record fnfe: " + fnfe.getMessage()); }
		try {
			raf.seek(recordID.getOffset());
			raf.read(payload, 0, (int) recordID.getLength());
		} catch(IOException ioe) { System.out.println("CS read first record ioe: " + ioe.getMessage()); }
		
		rec.setRID(recordID);
		rec.setPayload(payload);
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals chunkServerReadPrevRecord(FileHandle fh, RID pivot, TinyRec rec) {
		//System.out.println("\tpivot offset: " + pivot.getOffset());
		//System.out.println("\tpivot chunk: " + pivot.getChunk());
		String filepath = fh.getFileDir() + fh.getFileName();
		String chunkHandle = filepath + pivot.getChunk();
		
		//file has no chunks yet
		if(!chunkToRecs.containsKey(chunkHandle)) return FSReturnVals.RecDoesNotExist;
		//chunk had records but they were all deleted
		if(chunkToRecs.get(chunkHandle).size() == 0) return FSReturnVals.RecDoesNotExist;
						
		//find the next record from the chunkToRecs LinkedList
		System.out.println("\tChunkHandle: " + chunkHandle);
		LinkedList<RID> records = chunkToRecs.get(chunkHandle);
		RID prevRecord = null;
		
		
		for(int i=records.size()-1; i>=0; i--) {
			if(pivot.getOffset() == records.get(i).getOffset()) {
				if(i != 0) prevRecord = records.get(i-1);
				break;
			}
		}
		

		
		if(prevRecord == null) { //try the next chunk
			int prevChunk = Integer.parseInt(pivot.getChunk()) - 1;
			System.out.println("Moving to last record of the prev chunk: " + prevChunk );
			return chunkServerReadLastRecord(fh, rec, prevChunk);
		}
		
		
		//at this point we know if next record exists or not
		
		if(prevRecord == null) return FSReturnVals.RecDoesNotExist;
		
		byte[] payload = new byte[(int) prevRecord.getLength()];
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(rootFilePath + filepath, "r");
		} catch (FileNotFoundException fnfe) { System.out.println("CS read first record fnfe: " + fnfe.getMessage()); }
		try {
			raf.seek(prevRecord.getOffset());
			raf.read(payload, 0, (int) prevRecord.getLength());
		} catch(IOException ioe) { System.out.println("CS read first record ioe: " + ioe.getMessage()); }
		
		rec.setRID(prevRecord);
		rec.setPayload(payload);
		
		return FSReturnVals.Success;
	}
	//TODO: will have to be able to hop from the end of a chunk to the beginning of another
	public FSReturnVals chunkServerReadNextRecord(FileHandle fh, RID pivot, TinyRec rec) {
		//System.out.println("\tpivot offset: " + pivot.getOffset());
		//System.out.println("\tpivot chunk: " + pivot.getChunk());
		String filepath = fh.getFileDir() + fh.getFileName();
		String chunkHandle = filepath + pivot.getChunk();
		
		//file has no chunks yet
		if(!chunkToRecs.containsKey(chunkHandle)) return FSReturnVals.RecDoesNotExist;
		//chunk had records but they were all deleted
		if(chunkToRecs.get(chunkHandle).size() == 0) return FSReturnVals.RecDoesNotExist;
						
		//find the next record from the chunkToRecs LinkedList
		System.out.println("\tChunkHandle: " + chunkHandle);
		LinkedList<RID> records = chunkToRecs.get(chunkHandle);
		RID nextRecord = null;
		for(int i=0; i<records.size(); i++) {
			if(pivot.getOffset() == records.get(i).getOffset()) {
				if(i != records.size()-1) nextRecord = records.get(i+1);
				break;
			}
		}
		
		if(nextRecord == null) { //try the next chunk
			int nextChunk = Integer.parseInt(pivot.getChunk()) + 1;
			System.out.println("Moving to first record of the next chunk: " + nextChunk );
			return chunkServerReadFirstRecord(fh, rec, nextChunk);
		}
		
		
		//at this point we know if next record exists or not
		
		if(nextRecord == null) return FSReturnVals.RecDoesNotExist;
		
		byte[] payload = new byte[(int) nextRecord.getLength()];
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(rootFilePath + filepath, "r");
		} catch (FileNotFoundException fnfe) { System.out.println("CS read first record fnfe: " + fnfe.getMessage()); }
		try {
			raf.seek(nextRecord.getOffset());
			raf.read(payload, 0, (int) nextRecord.getLength());
		} catch(IOException ioe) { System.out.println("CS read first record ioe: " + ioe.getMessage()); }
		
		rec.setRID(nextRecord);
		rec.setPayload(payload);
		
		return FSReturnVals.Success;
	}
	

	/* Returns a file with the filepath and filename
	 * 
	 * If the file exists, return the existing file
	 * Else, return a new file with a 0 as the first 4 bytes since
	 * it has no chunks yet
	 */
	public RandomAccessFile getFile(String filepath, String filename) {
		File dirPath = new File(filepath);
		dirPath.mkdirs();
		
		File f = new File(filepath+filename);
		try { f.createNewFile(); } 
		catch(IOException ioe) { System.out.println("getFile ioe: " + ioe.getMessage());}
		
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(filepath+filename, "rw");
			return file;	
		} catch (FileNotFoundException fnfe) {
			System.out.println("getFile fnfe: " + fnfe.getMessage());
		}
		return null;

	}
		
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		File dir = new File(rootFilePath);
		File[] fs = dir.listFiles();

		fileToChunks = new HashMap<String, List<String>>();
		chunkToRecs = new HashMap<String, LinkedList<RID>>() ;

		
		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				try {
					cntrs[j] = Long.valueOf( fs[j].getName() ); 
				} catch(NumberFormatException nfe) {
					
				}
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String createChunk() {
		counter++;
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(rootFilePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(rootFilePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(rootFilePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	
	
	public void ReadAndProcessRequests()
	{
		System.out.println("Starting ReadAndProcessRequests");
		ChunkServer cs = new ChunkServer();
		
		//Used for communication with the Client via the network
		int ServerPort = 0; //Set to 0 to cause ServerSocket to allocate the port 
		ServerSocket commChanel = null;
		ObjectOutputStream WriteOutput = null;
		ObjectInputStream ReadInput = null;
		
		try {
			//Allocate a port and write it to the config file for the Client to consume
			commChanel = new ServerSocket(ServerPort);
			ServerPort=commChanel.getLocalPort();
			PrintWriter outWrite=new PrintWriter(new FileOutputStream(ClientConfigFile));
			outWrite.println("port : "+ServerPort);
			outWrite.close();
		} catch (IOException ex) {
			System.out.println("Error, failed to open a new socket to listen on.");
			ex.printStackTrace();
		}
		
		boolean done = false;
		Socket ClientConnection = null;  //A client's connection to the server

		while (!done){
			try {				
				System.out.println("Waiting for connections on port: " + ServerPort);

				ClientConnection = commChanel.accept();
				ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
				WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
				System.out.println("Chunkserver got connection");
				//Use the existing input and output stream as long as the client is connected
				while (!ClientConnection.isClosed()) {
					
					int CMD = Master.getPayloadInt(ReadInput);
					
					if(CMD != -1) System.out.println("Received command: " + CMD);
					
					switch (CMD){
					case AppendRecordCMD:
						String fileDir = Master.readString(ReadInput);
						String fileName = Master.readString(ReadInput);
						int size = Master.getPayloadInt(ReadInput);
						byte[] payload = Client.RecvPayload("g", ReadInput, size);
						System.out.println(fileDir+fileName + ", "+ size);
						
						RID recordID = new RID();
						FSReturnVals result = chunkServerAppendRecord(new FileHandle(fileDir, fileName), payload, recordID);
						Master.sendString(WriteOutput, recordID.getFilepath());
						Master.sendString(WriteOutput, recordID.getChunk());
						WriteOutput.writeInt((int) recordID.getOffset());
						WriteOutput.writeInt((int) recordID.getLength());
						Master.sendResultToClient(WriteOutput, result);
						break;
						
					case ReadFirstRecordCMD:
						fileDir = Master.readString(ReadInput);
						fileName = Master.readString(ReadInput);
						FileHandle ofh = new FileHandle(fileDir, fileName);
						TinyRec rec = new TinyRec();
						result = chunkServerReadFirstRecord(ofh, rec, 1);
						
						WriteOutput.writeInt(rec.getPayload().length);
						WriteOutput.write(rec.getPayload());
						
						recordID = rec.getRID();
						Master.sendString(WriteOutput, recordID.getFilepath());
						Master.sendString(WriteOutput, recordID.getChunk());
						WriteOutput.writeInt((int) recordID.getOffset());
						WriteOutput.writeInt((int) recordID.getLength());
						Master.sendResultToClient(WriteOutput, result);
						break;
						
					case ReadFirstRecordOfChunkCMD:
						fileDir = Master.readString(ReadInput);
						fileName = Master.readString(ReadInput);
						int chunkNum = Master.getPayloadInt(ReadInput);
						ofh = new FileHandle(fileDir, fileName);
						rec = new TinyRec();
						result = chunkServerReadFirstRecord(ofh, rec, chunkNum);
						
						WriteOutput.writeInt(rec.getPayload().length);
						WriteOutput.write(rec.getPayload());
						
						recordID = rec.getRID();
						Master.sendString(WriteOutput, recordID.getFilepath());
						Master.sendString(WriteOutput, recordID.getChunk());
						WriteOutput.writeInt((int) recordID.getOffset());
						WriteOutput.writeInt((int) recordID.getLength());
						Master.sendResultToClient(WriteOutput, result);
						break;
						
					case ReadNextRecordCMD:
						try {
							FileHandle fh = (FileHandle) ReadInput.readObject();
							RID pivot = (RID) ReadInput.readObject();
							rec = (TinyRec) ReadInput.readObject();
							result = chunkServerReadNextRecord(fh, pivot, rec);
							
							WriteOutput.writeObject(rec);
							Master.sendResultToClient(WriteOutput, result);
						} catch(ClassNotFoundException cnfe) {}
						
					/*
					public static final int AppendRecordCMD = 104;
					public static final int DeleteRecordCMD = 105;
					public static final int ReadFirstRecordCMD = 106;
					public static final int ReadLastRecordCMD = 107;
					public static final int ReadNextRecordCMD = 108;
					public static final int ReadPrevRecordCMD = 109;
					*/
						/*
					case CreateChunkCMD:
						String chunkhandle = cs.createChunk();
						byte[] CHinbytes = chunkhandle.getBytes();
						WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
						WriteOutput.write(CHinbytes);
						WriteOutput.flush();
						break;

					case ReadChunkCMD:
						int offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						int payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
						byte[] CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						String ChunkHandle = (new String(CHinBytes)).toString();
						
						byte[] res = cs.readChunk(ChunkHandle, offset, payloadlength);
						
						if (res == null)
							WriteOutput.writeInt(ChunkServer.PayloadSZ);
						else {
							WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
							WriteOutput.write(res);
						}
						WriteOutput.flush();
						break;

					case WriteChunkCMD:
						offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						byte[] payload = Client.RecvPayload("ChunkServer", ReadInput, payloadlength);
						chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
						CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();

						//Call the writeChunk command
						if (cs.writeChunk(ChunkHandle, payload, offset))
							WriteOutput.writeInt(ChunkServer.TRUE);
						else WriteOutput.writeInt(ChunkServer.FALSE);
						
						WriteOutput.flush();
						break;
						*/
					default:
						//System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
						break;
					}
				}
			} catch (IOException ex){
				System.out.println("Client Disconnected");
			} /*finally {
				try {
					if (ClientConnection != null)
						ClientConnection.close();
					if (ReadInput != null)
						ReadInput.close();
					if (WriteOutput != null) WriteOutput.close();
				} catch (IOException fex){
					System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
					fex.printStackTrace();
				}
			}*/
		}
	}

	public static void main(String args[])
	{
		ChunkServer cs = new ChunkServer();
		cs.ReadAndProcessRequests();
	}
	
	
	
	/*
	 * HELPERS
	 */
	
	//method to insert a record into list
	public void insertRecord(String filepath, String chunk, RID recordID) {
		LinkedList<RID> records = chunkToRecs.get(filepath+chunk);
		if(records.size() == 0) records.add(recordID);
		
		for(int i=0; i<records.size(); i++) {
			//record being inserted has a smaller offset
			if(!recordID.greaterThan(records.get(i))) { 
				records.add(i, recordID);
				return;
			}
		}
		records.addLast(recordID);
		//printRecords(filepath, chunk);
	}
	
	public void printRecords(String filepath, String chunk) {
		System.out.println("-----PRINTING RECORDS FOR " + filepath+chunk + "-----");
		System.out.println(chunkToRecs.get(filepath+chunk).size());
		for(RID rid : chunkToRecs.get(filepath+chunk)) {
			System.out.println("\tOffset: " + rid.getOffset());
			//System.out.println("\t\tChunk: " + rid.getChunk());
		}
		System.out.println("-----DONE PRINTING RECORDS FOR " + filepath+chunk + "-----");
	}
	
	public void writeIntAtOffset(RandomAccessFile file, int offset, int num) {
		try {
			file.seek(offset);
			byte[] numBytes = ByteBuffer.allocate(4).putInt(num).array();
			file.write(numBytes);
		}catch(IOException ioe) { 
			System.out.println("writeIntAtOffset ioe: " + ioe.getMessage());
			return;
		}
	}
	
	public int readIntAtOffset(RandomAccessFile file, int offset) {
		try {
			file.seek(offset); //get length of the free space
			byte[] offsetBytes = new byte[4];
			file.read(offsetBytes);
			return ByteBuffer.wrap(offsetBytes).getInt();
		} catch(IOException ioe) { 
			System.out.println("readIntAtOffset ioe: " + ioe.getMessage());
			ioe.printStackTrace();
			System.exit(0);
			return Integer.MIN_VALUE;

		}
	}
	
	public String removeLastSlash(String filepath) {
		if(filepath.charAt(filepath.length()-1) == '/')
			return filepath.substring(0, filepath.length()-1);
		return filepath;
	}
	
	public String removeFirstSlash(String filepath) {
		if(filepath.charAt(0) == '/')
			return filepath.substring(1);
		return filepath;
	}
	
	public String cutOffLastDir(String path) {
		path = path.substring(0, path.length()-1);
		while(path.charAt(path.length()-1) != '/') {
			path = path.substring(0, path.length()-1);	
		}
		return path;
	}
}
