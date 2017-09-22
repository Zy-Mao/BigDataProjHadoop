import dao.DataOperator;
import entity.AccessLog;
import entity.Friends;
import entity.MyPage;

import java.util.ArrayList;
import java.util.Locale;

public class Main {

    public static void main(String[] args) {
		DataOperator dataOperator = new DataOperator();
		dataOperator.generateMyPageList();
		dataOperator.generateFriendsList();
		dataOperator.generateAccessLogList();
    }
}