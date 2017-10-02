package proj01;

import proj01.dao.DataOperator;

public class Main {

    public static void main(String[] args) {
		DataOperator dataOperator = new DataOperator();
		dataOperator.generateMyPageList();
		dataOperator.generateFriendsList();
		dataOperator.generateAccessLogList();
    }
}