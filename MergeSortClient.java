import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MergeSortClient 
{
    private String serverAddress;
    private int port;
    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;
    private int clientId;
    
    public MergeSortClient(String serverAddress, int port) 
    {
        this.serverAddress = serverAddress;
        this.port = port;
    }
    
    public void connect() throws IOException 
    {
        socket = new Socket(serverAddress, port);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);
        
        String line = in.readLine();
        if (line.startsWith("CLIENT_ID:")) 
        {
            clientId = Integer.parseInt(line.substring(10));
            System.out.println("Client id #" + clientId);
        }
    }
    private static int[] parseArray(String data) 
    {
        String[] strNumbers = data.split(",");
        int[] arr = new int[strNumbers.length];
        for (int i = 0; i < strNumbers.length; i++)
            arr[i] = Integer.parseInt(strNumbers[i]);
        return arr;
    }

    private static boolean isSorted(String data) 
    {
        int[] arr = parseArray(data);
        for (int i = 1; i < arr.length; i++) 
            if (arr[i - 1] > arr[i]) 
                return false;
        return true;
    }
    
    public void requestWork(String cmd, int size) throws IOException 
    {
        int[] arr = generateRandomArray(size);
        switch (cmd) {
            case "sort":
                out.println("SORT_REQUEST:" + formatArray(arr));
                break;
            case "test_sort":
                out.println("TEST_SPEEDUP:" + formatArray(arr));
                break;
            default:
                return;
        }

        String response = in.readLine();
        String[] parts = response.split(":");
        
        if (parts[0].equals("SORT_COMPLETE")) 
        {
            if (parseArray(parts[1]).length <= 20)
                System.out.println("Sorted array: " + parts[1]);
            else
                System.out.println("Sort result: " + (isSorted(parts[1]) ? "Sorted" : "Unsorted"));
        } 
        else if (parts[0].equals("TEST_SPEEDUP_COMPLETE")) 
        {
            String[] speedupParts = parts[1].split(";");
            System.out.println("Sort result: " + (isSorted(speedupParts[0]) ? "Sorted" : "Unsorted") +
                                "\n Sequential Time: " + speedupParts[1] + " ms" +
                                "\n Parallel Time: " + speedupParts[2] + " ms" +
                                "\n Speedup: x" + speedupParts[3]
            );
        }
        else 
        {
            System.out.println("Unexpected response: " + response);
        }
    }
    
    public void notifyDone() throws IOException 
    {
        out.println("DONE");
        String response = in.readLine();

        if (response.equals("ACK")) 
            System.out.println("Server acknowledged completion.");
        else 
            System.out.println("Unexpected response: " + response);
    }
    
    public void close() throws IOException 
    {
        if (socket != null) 
            socket.close();
    }

    private String formatArray(int[] arr) 
    {
        return String.join(",", 
                    Arrays.stream(arr)
                        .mapToObj(String::valueOf)
                        .toArray(String[]::new)
                    );
    }

    private static int[] generateRandomArray(int size)
    {
        int[] arr = new int[size];
        for (int i = 0; i < size; i++)
            arr[i] = (int) (Math.random() * 10000);
        return arr;
    }

    public static void main(String[] args) 
    {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        
        for (int i = 0; i < 1; i++) 
        {
            executor.submit(() -> 
            {
                MergeSortClient client = new MergeSortClient("localhost", 8888);
                try 
                {
                    client.connect();

                    Scanner sc = new Scanner(System.in);

                    System.out.print("Enter command (sort, test_sort, done): ");
                    String cmd = sc.nextLine().toLowerCase();
                    
                    System.out.print("Size of generated array: ");
                    String arrSizeStr = sc.nextLine();
                    sc.close();

                    client.requestWork(cmd, Integer.parseInt(arrSizeStr));
                    client.notifyDone();
                    
                    Thread.sleep(3000);
                    
                    client.close();
                } 
                catch (IOException | InterruptedException e) 
                {
                    e.printStackTrace();
                }
            });
        }
        
        executor.shutdown();
    }
}