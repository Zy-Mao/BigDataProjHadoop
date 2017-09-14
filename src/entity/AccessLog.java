package entity;

import java.util.Date;

public class AccessLog {
	private int AccessId;
	private int byWho;
	private int whatPage;
	private String typeOfAccess;
	private int accessTime;

	public AccessLog() {

	}

	public AccessLog(int accessId, int byWho, int whatPage, String typeOfAccess, int accessTime) {
		AccessId = accessId;
		this.byWho = byWho;
		this.whatPage = whatPage;
		this.typeOfAccess = typeOfAccess;
		this.accessTime = accessTime;
	}

	@Override
	public String toString() {
		return AccessId + "," + byWho + "," + whatPage + "," + typeOfAccess + "," + accessTime;
	}

	public int getAccessId() {
		return AccessId;
	}

	public void setAccessId(int accessId) {
		AccessId = accessId;
	}

	public int getByWho() {
		return byWho;
	}

	public void setByWho(int byWho) {
		this.byWho = byWho;
	}

	public int getWhatPage() {
		return whatPage;
	}

	public void setWhatPage(int whatPage) {
		this.whatPage = whatPage;
	}

	public String getTypeOfAccess() {
		return typeOfAccess;
	}

	public void setTypeOfAccess(String typeOfAccess) {
		this.typeOfAccess = typeOfAccess;
	}

	public int getAccessTime() {
		return accessTime;
	}

	public void setAccessTime(int accessTime) {
		this.accessTime = accessTime;
	}
}
