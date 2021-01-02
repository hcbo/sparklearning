import java.io.*;

public class ExtractLog {
    public static void main(String[] args) throws IOException {
//        generateWriteKafka();
//        generateWriteZk();
//        generateReadKafka();
        generateReadZk();
    }

    private static void generateReadZk() throws IOException {
        String logFileName = "/Users/hcb/IdeaProjects/sparkDemo/run3_0.log";
//        String logFileName = "/Users/hcb/Desktop/1.log";
        File file = new File(logFileName);
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);

        String generFilename = "/Users/hcb/IdeaProjects/sparkDemo/run3_0_readZk.txt";
//        String generFilename = "/Users/hcb/Desktop/1_gener.log";
        File generFile = new File(generFilename);
        FileWriter fileWriter = new FileWriter(generFile);

        String lineContent = null;
        StringBuilder newLine = new StringBuilder();
        while((lineContent = br.readLine())!=null) {
            if(lineContent.contains("file metadata from zk time start")) {
                String path = lineContent.substring(lineContent.indexOf("/checkRoot"),
                        lineContent.lastIndexOf(" 160"));
                newLine.append(path + " ");
                String startTime = lineContent.substring(lineContent.lastIndexOf(" 160") + 1);
                newLine.append(startTime + " ");
            }
            if(lineContent.contains("file metadata from zk time stop")) {
                String endTime = lineContent.substring(lineContent.lastIndexOf(" 160") + 1);
                newLine.append(endTime);
                newLine.append("\n");
                fileWriter.write(newLine.toString());
                fileWriter.flush();
                newLine.delete(0,newLine.length());
            }
//            System.out.println(lineContent);
        }
    }

    private static void generateReadKafka() throws IOException {
        String logFileName = "/Users/hcb/IdeaProjects/sparkDemo/run3_0.log";
//        String logFileName = "/Users/hcb/Desktop/1.log";
        File file = new File(logFileName);
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);

        String generFilename = "/Users/hcb/IdeaProjects/sparkDemo/run3_0_readKafka.txt";
//        String generFilename = "/Users/hcb/Desktop/1_gener.log";
        File generFile = new File(generFilename);
        FileWriter fileWriter = new FileWriter(generFile);

        String lineContent = null;
        StringBuilder newLine = new StringBuilder();
        while((lineContent = br.readLine())!=null) {
            if(lineContent.contains("file data from kafka time start")) {
                String path = lineContent.substring(lineContent.indexOf("/checkRoot"),
                        lineContent.lastIndexOf(" 160"));
                newLine.append(path + " ");
                String startTime = lineContent.substring(lineContent.lastIndexOf(" 160") + 1);
                newLine.append(startTime + " ");
            }
            if(lineContent.contains("file data from kafka time stop")) {
                String endTime = lineContent.substring(lineContent.lastIndexOf(" 160") + 1);
                newLine.append(endTime);
                newLine.append("\n");
                fileWriter.write(newLine.toString());
                fileWriter.flush();
                newLine.delete(0,newLine.length());
            }
//            System.out.println(lineContent);
        }
    }

    private static void generateWriteZk() throws IOException {
        String logFileName = "/Users/hcb/IdeaProjects/sparkDemo/run3_0.log";
//        String logFileName = "/Users/hcb/Desktop/1.log";
        File file = new File(logFileName);
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);

        String generFilename = "/Users/hcb/IdeaProjects/sparkDemo/run3_0_writeZK.txt";
//        String generFilename = "/Users/hcb/Desktop/1_gener.log";
        File generFile = new File(generFilename);
        FileWriter fileWriter = new FileWriter(generFile);

        String lineContent = null;
        StringBuilder newLine = new StringBuilder();
        while((lineContent = br.readLine())!=null) {
            if(lineContent.contains("file metadata to zk time start")) {
                String path = lineContent.substring(lineContent.indexOf("/checkRoot"),
                        lineContent.lastIndexOf(" 160"));
                newLine.append(path + " ");
                String startTime = lineContent.substring(lineContent.lastIndexOf(" 160") + 1);
                newLine.append(startTime + " ");
            }
            if(lineContent.contains("file metadata to zk time stop")) {
                String endTime = lineContent.substring(lineContent.lastIndexOf(" 160") + 1);
                newLine.append(endTime);
                newLine.append("\n");
                fileWriter.write(newLine.toString());
                fileWriter.flush();
                newLine.delete(0,newLine.length());
            }
//            System.out.println(lineContent);
        }
    }

    private static void generateWriteKafka() throws IOException {
        String logFileName = "/Users/hcb/IdeaProjects/sparkDemo/run3_0.log";
//        String logFileName = "/Users/hcb/Desktop/1.log";
        File file = new File(logFileName);
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);

        String generFilename = "/Users/hcb/IdeaProjects/sparkDemo/run3_0_writeKafka.txt";
//        String generFilename = "/Users/hcb/Desktop/1_gener.log";
        File generFile = new File(generFilename);
        FileWriter fileWriter = new FileWriter(generFile);

        String lineContent = null;
        StringBuilder newLine = new StringBuilder();
        while((lineContent = br.readLine())!=null) {
            if(lineContent.contains("file data to kafka time start")) {
                String path = lineContent.substring(lineContent.indexOf("/checkRoot"),
                        lineContent.lastIndexOf(" 160"));
                newLine.append(path + " ");
                String startTime = lineContent.substring(lineContent.lastIndexOf(" 160") + 1);
                newLine.append(startTime + " ");
            }
            if(lineContent.contains("file data to kafka time stop")) {
                String endTime = lineContent.substring(lineContent.lastIndexOf(" 160") + 1);
                newLine.append(endTime);
                newLine.append("\n");
                fileWriter.write(newLine.toString());
                fileWriter.flush();
                newLine.delete(0,newLine.length());
            }
//            System.out.println(lineContent);
        }
    }
}
