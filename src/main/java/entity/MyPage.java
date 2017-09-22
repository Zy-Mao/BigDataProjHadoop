package entity;

public class MyPage {
	private int id;
	private String name;
	private String nationality;
	private int countryCode;
	private String hobby;

	public MyPage() {
	}

	// Construct the instance according to input string.
	public MyPage(String string) {
		String[] strings= string.split(",");
		if (strings.length == 5) {
			this.id = Integer.parseInt(strings[0]);
			this.name = strings[1];
			this.nationality = strings[2];
			this.countryCode = Integer.parseInt(strings[3]);
			this.hobby = strings[4];
		}
	}

	public MyPage(int id, String name, String nationality, int countryCode, String hobby) {
		this.id = id;
		this.name = name;
		this.nationality = nationality;
		this.countryCode = countryCode;
		this.hobby = hobby;
	}

	@Override
	public String toString() {
		return id + "," + name + "," + nationality + "," + countryCode + "," + hobby;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public int getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(int countryCode) {
		this.countryCode = countryCode;
	}

	public String getHobby() {
		return hobby;
	}

	public void setHobby(String hobby) {
		this.hobby = hobby;
	}
}
