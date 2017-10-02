package proj02.task01;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;

public class DataOperator {
	private static String POINT_FILE_NAME = "Point.csv";
	private static String RECTANGLE_FILE_NAME = "Rectangle.csv";

	private void generatePoint() {
		int count = 0;
		ArrayList<Point> pointArrayList = new ArrayList<>();
		while (true) {
			count += 1;
			Point point = new Point();
			point.setX(generateRandomInt(1, 10000));
			point.setY(generateRandomInt(1, 10000));
			pointArrayList.add(point);

			if (count % 100000 == 0) {
				outputDataIntoFiles(pointArrayList, POINT_FILE_NAME);
				pointArrayList = new ArrayList<>();
				System.out.println("Generated and output " + count + " Point records.");
				if (getFileSize(POINT_FILE_NAME) >= 100*1024*1024) {
					break;
				}
			}

		}
		System.out.println("Finished: generated and output Point records.");
	}

	// generate a random number in range [min, max]
	private int generateRandomInt(int min, int max) {
		return new Random().nextInt(max - min + 1) + min;
	}

	private long getFileSize(String fileName) {
		return 0;
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
