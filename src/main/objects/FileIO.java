package main.objects;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileIO {

	private File file;
	private BufferedWriter writer;
	private BufferedReader reader;
	private FileInputStream input;
	private FileOutputStream output;
	private int lsn;
	
	public FileIO(String filePath) {
		file = new File(filePath);

		init();
	}

	public FileIO(File file) {
		this.file = file;

		init();
	}

	public int lastLsn() {
		return lsn;
	}
	
	public void addLine(String line) throws IOException {
		writer.append(line + '\n').flush();
	}
	
	public void add(String line) throws IOException {
		writer.append(line).flush();
	}
	

	public void addTransaction(String transaction) throws IOException {
		writer.append("<lsn>" + (++lsn) + "</lsn>" + transaction + '\n').flush();
	}

	public String findLastTransaction() throws IOException {
		
		String[] transactions = readAll().split("\n");

		if (transactions[transactions.length - 1].startsWith("<lsn>"))
			return transactions[transactions.length - 1];

		return null;
	}

	public String findTransaction(int sequenceNumber) throws IOException {

		String lsn;
		String[] transactions = readAll().split("\n");

		if (transactions[0].length() > 0) {
			for (String transaction : transactions) {
				lsn = transaction.split("</?lsn>")[1];
				if (lsn == null) {
					Logerr("LSN split is null in " + file.getName());
				} else if (lsn.equals("" + sequenceNumber)) {
					return transaction;
				}
			}
		}
		return null;
	}

	public void deleteLastTransaction() throws IOException {

		String[] transactions = readAll().split("\n");
		File file = this.file;

		PrintWriter pw = new PrintWriter(file.getAbsolutePath());
		pw.print("");
		pw.close();

		for (int i = 0; i < transactions.length - 1; i++) {
			writer.append(transactions[i] + '\n');
		}

		lsn = (lsn > 0) ? lsn - 1 : 0;

		writer.flush();
	}

	public String readAll() throws IOException {

		// reset position for the next operation
		input.getChannel().position(0);

		String buffer = "";
		StringBuilder res = new StringBuilder();

		while ((buffer = reader.readLine()) != null) {
			res.append(buffer + '\n');
		}

		return res.toString();
	}
	
	public String findTagInfo(String tag, boolean getLine) throws IOException {
		String[] options = readAll().split("\n");

		if (options[0].length() > 0) {
			for (String option : options) {
				if(option.startsWith("<" + tag + ">")) {
					return (getLine) ? option : option.split("</?" + tag + ">")[1];
				}
			}
		}
		
		return null;
	}

	public static void deleteFile(String filePath) throws IOException {
		Files.deleteIfExists(Paths.get(filePath));
	}
	
	public static String extract(String parentTag, String line, String option) {
		
		int index = 1;
		
		if("inner".equals(option))
			index = 1;
		else if ("outer".equals(option))
			index = 2;
		
		if(line.startsWith("<" + parentTag + ">")) {
			return line.split("</?" + parentTag + ">")[index];
		}
		return null;
	}
	
	public static boolean exists(String filePath) {
		return new File(filePath).exists();
	}

	
	public void close() throws IOException {
		writer.close();
		reader.close();
		input.close();
		output.close();
	}

	private void init() {
		try {
			output = new FileOutputStream(file, true);
			input = new FileInputStream(file);

			writer = new BufferedWriter(new OutputStreamWriter(output));
			reader = new BufferedReader(new InputStreamReader(input));

			// read last LSN from the file

			String lastTransaction = findLastTransaction();

			if (lastTransaction != null) {
				lsn = Integer.parseInt(lastTransaction.split("</?lsn>")[1]);
			}
		} catch (IOException e) {
			// e.printStackTrace();
			Logerr("***Cannot open file " + file.getName() + " for read and write operations!***");
		}
	}

	private void Log(String message) {
		System.out.println(message);
	}

	private void Logerr(String message) {
		System.err.println(message);
	}

}
