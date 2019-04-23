package com.client;

public class FileHandle implements java.io.Serializable {
	//A file is uniquely identified by its directory path and its name (no duplicates).
	//might have to add in which chunk(s) it belongs to as well
	private String dir;
	private String filename;
	
	public FileHandle() {}
	
	public FileHandle(String dir, String filename) {
		this.dir = dir;
		this.filename = filename;
	}
	
	public String getFileDir() { return dir; }
	public String getFileName() { return filename; }
	
	
	public void setFileDir(String dir) { this.dir = dir;}
	public void setFileName(String name) { filename = name; }
	
	public boolean equals(FileHandle other) {
		if(this.dir.equals(other.getFileDir()))
			if(this.filename.equals(other.getFileName()))
				return true;
		return false;
	}
	
}
