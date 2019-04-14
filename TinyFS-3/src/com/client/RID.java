package com.client;

public class RID {
	private String filepath;
	private String chunk;
	private long offset;
	private long length;
	
	public RID() {
	}
	
	public boolean invalid() {
		if(filepath == null || chunk == null) {
			return true;
		}
		return false;
	}
	
	public RID(String filepath, String chunk, long offset, long length) {
		this.filepath = filepath;
		this.chunk = chunk;
		this.offset = offset;
		this.length = length;
	}

	public String getFilepath() { return filepath; }
	public String getChunk() { return chunk; }
	public long getOffset() { return offset; }
	public long getLength() { return length; }


	public void setFilepath(String filepath) { this.filepath = filepath; }
	public void setChunk(String chunk) { this.chunk = chunk; }
	public void setOffset(long offset) { this.offset = offset; }
	public void setLength(long length) { this.length = length; }
	
	
}
