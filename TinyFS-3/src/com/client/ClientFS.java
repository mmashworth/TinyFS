package com.client;

import java.io.File;
import com.master.Master;
import java.util.List;
import java.util.ArrayList;

public class ClientFS {
	
	private Master m = new Master();

	public static final String root = "csci485";
	
	public enum FSReturnVals {
		DirExists,         // Returned by CreateDir when directory exists
		DirNotEmpty,       // Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists,     // Returned when a destination directory exists
		FileExists,        // Returned when a file exists
		FileDoesNotExist,  // Returns when a file does not exist
		BadHandle,         // Returned when the handle for an open file is not valid
		RecordTooLong,     // Returned when a record size is larger than chunk size
		BadRecID,          // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist,   // The specified record does not exist, used by DeleteRecord
		NotImplemented,    // Specific to CSCI 485 and its unit tests
		Success,           // Returned when a method succeeds
		Fail               // Returned when a method fails
	}

	/**
	 * Creates the specified dirname in the src directory 
	 * 
	 * Returns SrcDirNotExistent if the src directory does not exist 
	 * Returns DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		return m.masterCreateDir(src, dirname);
	}

	/**
	 * Deletes the specified dirname in the src directory 
	 * 
	 * Returns SrcDirNotExistent if the src directory does not exist 
	 * Returns DestDirExists if the specified dirname exists
	 * -- Returns DirNotEmpty when a non empty directory is deleted (attempted)
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		return m.masterDeleteDir(src, dirname);	
	}
	
	//unnecessary, thought we deleted a dir even if it had files in it
	public void emptyDir(File dir, String path, int numDir) {
		if(dir.list() == null) {
			if(numDir != 0) dir.delete();
			return;
		}
		for(String currFileStr : dir.list()) {
			File currFile = new File(path + "/" + currFileStr);
			boolean del = currFile.delete();
			if(!del) emptyDir(currFile, path+"/"+currFileStr, numDir+1);
			currFile.delete();
		}
		if(numDir != 0) dir.delete();
		
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * 
	 * Returns SrcDirNotExistent if the src directory does not exist 
	 * Returns DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String newName) {
		return m.masterRenameDir(src, newName);
	}

	/**
	 * Lists the content of the target directory
	 * 
	 * Returns SrcDirNotExistent if the target directory does not exist 
	 * Returns null if the target directory is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		return m.masterListDir(tgt);
	}

	/**
	 * Creates the specified filename in the target directory 
	 * 
	 * Returns SrcDirNotExistent if the target directory does not exist 
	 * Returns FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
//		System.out.println("---CREATING FILE---");
//		System.out.println("\ttgtdir: " + tgtdir);
//		System.out.println("\tfilename: " + filename);
		
		
		return m.masterCreateFile(tgtdir, filename);
	}

	/**
	 * Deletes the specified filename from the tgtdir 
	 * 
	 * Returns SrcDirNotExistent if the target directory does not exist 
	 * Returns FileDoesNotExist if the specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
//		System.out.println("---DELETING FILE----");
//		System.out.println("\ttgtdir: " + tgtdir);
//		System.out.println("\tfilename: " + filename);
		
		FSReturnVals result =  m.masterDeleteFile(tgtdir, filename);
		System.out.println("deletion result: " + result);
		return result;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * 
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		return null;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		return null;
	}

}
