package entity;

public class AccessLog {
	private int accessId;
	private int byWho;
	private int whatPage;
	private String typeOfAccess;
	private int accessTime;

	public AccessLog() {

	}

	// Construct the instance according to input string.
	public AccessLog(String string) {
		String[] strings= string.split(",");
		if (strings.length == 5) {
			this.accessId = Integer.parseInt(strings[0]);
			this.byWho = Integer.parseInt(strings[1]);
			this.whatPage = Integer.parseInt(strings[2]);
			this.typeOfAccess = strings[3];
			this.accessTime = Integer.parseInt(strings[4]);
		}
	}

	public AccessLog(int accessId, int byWho, int whatPage, String typeOfAccess, int accessTime) {
		this.accessId = accessId;
		this.byWho = byWho;
		this.whatPage = whatPage;
		this.typeOfAccess = typeOfAccess;
		this.accessTime = accessTime;
	}

	@Override
	public String toString() {
		return accessId + "," + byWho + "," + whatPage + "," + typeOfAccess + "," + accessTime;
	}

	public int getAccessId() {
		return accessId;
	}

	public void setAccessId(int accessId) {
		accessId = accessId;
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
