package dao;

import entity.AccessLog;
import entity.Friends;
import entity.MyPage;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

public class DataOperator {
	// The nationality of MyPage entities would generated from this array.
	private static String[] COUNTRY_POOL =
			{"Argentina", "Austria", "Australia", "Canada", "Switzerland", "China", "Germany",
					"Spain", "Finland", "France", "Iceland", "India", "Italy", "Japan", "Monaco",
					"Russia", "Singapore", "Thailand", "Turkey", "Ukraine", "United_States",
					"Venezuela", "Vietnam", "South Africa", "Zambia", "Zimbabwe"};

	public ArrayList<MyPage> generateMyPageList() {
		ArrayList<MyPage> myPageList = new ArrayList<>();
		for (int i = 1; i <= 100000; i++) {
			MyPage myPage = new MyPage();
			myPage.setId(i);
			myPage.setName(generateRandomString(generateRandomInt(10, 20)));
			myPage.setNationality(COUNTRY_POOL[generateRandomInt(0, COUNTRY_POOL.length - 1)]);
			myPage.setCountryCode(generateRandomInt(1, 10));
			myPage.setHobby(generateRandomString(generateRandomInt(10, 20)));
			myPageList.add(myPage);
		}
		outputDataIntoFiles(myPageList, "MyPage.csv");
		System.out.println("Finished: generated and output MyPage records.");
		return myPageList;
	}

	public ArrayList<Friends> generateFriendsList() {
		ArrayList<Friends> friendsList = new ArrayList<>();
		for (int i = 1; i <= 50000000; i++) {
			Friends friends = new Friends();
			friends.setFriendRel(i);
			friends.setPersonId(generateRandomInt(1, 100000));
			friends.setMyFriend(generateRandomInt(1, 100000));
			friends.setDateOfFriendship(generateRandomInt(1, 1000000));
			friends.setDesc(generateRandomString(generateRandomInt(20, 50)));
			friendsList.add(friends);
			//Write the data into the file part by part.
			if (i % 1000000 == 0) {
				outputDataIntoFiles(friendsList, "Friends.csv");
				friendsList = new ArrayList<>();
				System.out.println("Generated and output " + i + " Friends records.");
			}
		}
		outputDataIntoFiles(friendsList, "Friends.csv");
		System.out.println("Finished: generate and output Friends records.");
		return friendsList;
	}

	public ArrayList<AccessLog> generateAccessLogList() {
		ArrayList<AccessLog> accessLogList = new ArrayList<>();
		for (int i = 1; i <= 10000000; i++) {
			AccessLog accessLog = new AccessLog();
			accessLog.setAccessId(i);
			accessLog.setByWho(generateRandomInt(1, 100000));
			accessLog.setWhatPage(generateRandomInt(1, 100000));
			accessLog.setTypeOfAccess(generateRandomString(generateRandomInt(20, 50)));
			accessLog.setAccessTime(generateRandomInt(1, 1000000));
			accessLogList.add(accessLog);
			//Write the data into the file part by part.
			if (i % 1000000 == 0) {
				outputDataIntoFiles(accessLogList, "AccessLog.csv");
				accessLogList = new ArrayList<>();
				System.out.println("Generated and output " + i + " AccessLog records.");
			}
		}
		outputDataIntoFiles(accessLogList, "AccessLog.csv");
		System.out.println("Finished: generate and output AccessLog records.");
		return accessLogList;
	}

	private int generateRandomInt(int min, int max) {
		return new Random().nextInt(max - min + 1) + min;
	}

	// generate random string in specific length.
	private String generateRandomString(int length) {
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < length; i++) {
			strBuilder.append((char) ('a' + generateRandomInt(0, 25)));
		}
		return strBuilder.toString();
	}

	// Output the data in the input ArrayList into the file, line by line.
	private void outputDataIntoFiles(ArrayList arrayList, String fileName) {
		FileWriter fileWriter;
		BufferedWriter bufferedWriter;
		PrintWriter printWriter = null;
		StringBuilder strBuilder = new StringBuilder();

		try {
			fileWriter = new FileWriter(fileName, true);
			bufferedWriter = new BufferedWriter(fileWriter);
			printWriter = new PrintWriter(bufferedWriter);
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (Object object : arrayList) {
			strBuilder.append(object.toString());
			strBuilder.append(System.lineSeparator());
		}
		try {
			printWriter.write(strBuilder.toString());
			printWriter.close();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
	}
}