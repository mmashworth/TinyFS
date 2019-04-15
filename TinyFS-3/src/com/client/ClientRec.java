package com.client;

import java.nio.ByteBuffer;

import com.client.ClientFS.FSReturnVals;
import com.chunkserver.ChunkServer;

public class ClientRec {
	
	ChunkServer cs = new ChunkServer();

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
		//System.out.println("----APPENDING RECORD----");
		FSReturnVals result = cs.chunkServerAppendRecord(ofh, payload, RecordID);
		//System.out.println("append result: " + result);
		return result;
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
		//System.out.println("----FETCHING FIRST RECORD----");
		FSReturnVals result = cs.chunkServerReadFirstRecord(ofh, rec, 1);
		return result;
	}
	
	/**
	* Generalize above method so we can read first record of any chunk.
	* 
	* Useful for traversing from last record in a chunk to the first record in
	* the next chunk
	**/
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec, int chunkNum){
		//System.out.println("----FETCHING FIRST RECORD----");
		FSReturnVals result = cs.chunkServerReadFirstRecord(ofh, rec, chunkNum);
		return result;
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
		//System.out.println("----FETCHING LAST RECORD----");
		FSReturnVals result = cs.chunkServerReadLastRecord(ofh, rec);
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
		FSReturnVals result = cs.chunkServerReadNextRecord(ofh, pivot, rec);
		System.out.println("result: " + result);
		return result;
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
		return null;
	}

}
