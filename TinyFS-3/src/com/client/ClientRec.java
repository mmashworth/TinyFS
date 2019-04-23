package com.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.client.ClientFS.FSReturnVals;
import com.master.Master;
import com.chunkserver.ChunkServer;

public class ClientRec {
	
	ChunkServer cs = new ChunkServer();
	
	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	private static Socket s;
	
	private int chunkServerPort;
	
	public ClientRec() {
		System.out.println("in clientRec constructor");
		//establish connection with a chunk server
		if (s != null) return;
		try {
			FileReader fr = new FileReader(new File("TinyFS-3/ClientConfig.txt/"));
			
			
			BufferedReader bufferedReader = new BufferedReader(fr);
			String line;
			String lastEntry = "";
			while ((line = bufferedReader.readLine()) != null) {
				lastEntry = line;
			}
			fr.close();
			chunkServerPort = Integer.parseInt(lastEntry.substring(lastEntry.indexOf(':')+2));

			
			s = new Socket("127.0.0.1", chunkServerPort);
			oos = new ObjectOutputStream(s.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(s.getInputStream());

		} catch(IOException ioe) {
			System.out.println("ioe in Client constructor: " + ioe.getMessage());
		}
	}

	/**
	 * Appends a record to the open file as specified by ofh 
	 * 
	 * Returns BadHandle if ofh is invalid 
	 * Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		
		System.out.println("----APPENDING RECORD----");
		try {
			oos.writeInt(ChunkServer.AppendRecordCMD);
			Master.sendString(oos, ofh.getFileDir());
			Master.sendString(oos, ofh.getFileName());
			oos.writeInt(payload.length);
			oos.write(payload);
			oos.flush();
			
			String filepath = Master.readString(ois);
			String chunk = Master.readString(ois);
			int offset = Master.getPayloadInt(ois);
			int length = Master.getPayloadInt(ois);
			
			System.out.println("\t" + offset + chunk);
			
			String result = Master.readString(ois);
			return FSReturnVals.valueOf(result);
		} catch(IOException ioe) {
			System.out.println("clientrec appendrecord ioe: " + ioe.getMessage());
			return FSReturnVals.Fail;
		}
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh 
	 * 
	 * Returns BadHandle if ofh is invalid 
	 * Returns BadRecID if the specified RID is not valid 
	 * Returns RecDoesNotExist if the record specified by RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		System.out.println("----DELETING RECORD----");
		FSReturnVals result = cs.chunkServerDeleteRecord(ofh, RecordID);
		System.out.println("delete record result: " + result);
		return result;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload 
	 * 
	 * Returns BadHandle if ofh is invalid 
	 * Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		System.out.println("----FETCHING FIRST RECORD----");
		
		
		try {
			oos.writeInt(ChunkServer.ReadFirstRecordCMD);
			Master.sendString(oos, ofh.getFileDir());
			Master.sendString(oos, ofh.getFileName());
			oos.flush();
			
			
			int payloadLength = Master.getPayloadInt(ois);
			byte[] payload = Client.RecvPayload("client", ois, payloadLength);
			
			
			String filepath = Master.readString(ois);
			String chunk = Master.readString(ois);
			int offset = Master.getPayloadInt(ois);
			int length = Master.getPayloadInt(ois);
			RID recordID = new RID(filepath, chunk, offset, length);
			
			rec.setPayload(payload);
			rec.setRID(recordID);
			
			System.out.println("\t" + "done with readfirstrecord");
			
			String result = Master.readString(ois);
			return FSReturnVals.valueOf(result);
		} catch(IOException ioe) {
			System.out.println("clientrec readfirstrecord ioe: " + ioe.getMessage());
			return FSReturnVals.Fail;
		}
		
		
		//FSReturnVals result = cs.chunkServerReadFirstRecord(ofh, rec, 1);
		//return result;
	}
	
	/**
	* Generalize above method so we can read first record of any chunk.
	* 
	* Useful for traversing from last record in a chunk to the first record in
	* the next chunk
	**/
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec, int chunkNum){
		//System.out.println("----FETCHING FIRST RECORD----");
		try {
			oos.writeInt(ChunkServer.ReadFirstRecordCMD);
			Master.sendString(oos, ofh.getFileDir());
			Master.sendString(oos, ofh.getFileName());
			oos.writeInt(chunkNum);
			oos.flush();
			
			
			int payloadLength = Master.getPayloadInt(ois);
			byte[] payload = Client.RecvPayload("client", ois, payloadLength);
			
			
			String filepath = Master.readString(ois);
			String chunk = Master.readString(ois);
			int offset = Master.getPayloadInt(ois);
			int length = Master.getPayloadInt(ois);
			RID recordID = new RID(filepath, chunk, offset, length);
			
			rec.setPayload(payload);
			rec.setRID(recordID);
			
			System.out.println("\t" + "done with readfirstrecord");
			
			String result = Master.readString(ois);
			return FSReturnVals.valueOf(result);
		} catch(IOException ioe) {
			System.out.println("clientrec readfirstrecord ioe: " + ioe.getMessage());
			return FSReturnVals.Fail;
		}
	}
	
	

	/**
	 * Reads the last record of the file specified by ofh into payload 
	 * 
	 * Returns BadHandle if ofh is invalid 
	 * Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		System.out.println("----FETCHING LAST RECORD----");
		FSReturnVals result = cs.chunkServerReadLastRecord(ofh, rec);
		System.out.println("Read last record result: " + result);
		return result;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload 
	 * 
	 * Returns BadHandle if ofh is invalid 
	 * Returns RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		System.out.println("----FETCHING NEXT RECORD----");
		
		
		
		try {
			oos.writeInt(ChunkServer.ReadNextRecordCMD);
			oos.writeObject(ofh);
			oos.writeObject(pivot);
			oos.writeObject(rec);
			oos.flush();
			
			
			int payloadLength = Master.getPayloadInt(ois);
			byte[] payload = Client.RecvPayload("client", ois, payloadLength);
			
			TinyRec rc = null;
			try {
				rc = (TinyRec) ois.readObject();
			} catch(ClassNotFoundException cnfe) {}
			
			rec.setPayload(rc.getPayload());
			rec.setRID(rc.getRID());
			System.out.println("\t" + "done with readnextrecord");
			
			String result = Master.readString(ois);
			return FSReturnVals.valueOf(result);
		} catch(IOException ioe) {
			ioe.printStackTrace(System.out);
			return FSReturnVals.Fail;
		}
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		System.out.println("----FETCHING PREV RECORD----");
		FSReturnVals result = cs.chunkServerReadPrevRecord(ofh, pivot, rec);
		System.out.println("result: " + result);
		return result;
	}

}
