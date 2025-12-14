import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MergeSortServer 
{    
    private static final int THRESHOLD = 1000;
    private static final int PORT = 8888;
    private static int nextClientId = 1;
    
    static class ClientHandler implements Runnable 
    {
        private Socket socket;
        private int clientId;
        private BufferedReader in;
        private PrintWriter out;
        
        public ClientHandler(Socket socket, int clientId) 
        {
            this.socket = socket;
            this.clientId = clientId;
        }
        
        @Override
        public void run() {
            try {
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out = new PrintWriter(socket.getOutputStream(), true);
                System.out.println("Assigned Client ID: " + clientId);
                out.println("CLIENT_ID:" + clientId);
                
                String request;
                while ((request = in.readLine()) != null) {
                    handleRequest(request);
                }
                
            } 
            catch (IOException e) 
            {
                System.out.println("Client " + clientId + " dropped connection.");
            }
            finally 
            {
                try 
                {
                    socket.close();
                } 
                catch (IOException e) 
                {
                    e.printStackTrace();
                }
            }
        }
        
        private void handleRequest(String request) 
        {
            String[] parts = request.split(":");
            String command = parts[0];
            switch (command) {
                case "SORT_REQUEST":
                    String[] sortReqParts = parts[1].split(";");
                    sort(Integer.parseInt(sortReqParts[0]), sortReqParts[1]);
                    break;
                case "TEST_SPEEDUP":
                    String[] testReqParts = parts[1].split(";");
                    test_speedup(Integer.parseInt(testReqParts[0]), testReqParts[1]);
                    break;
                case "DONE":
                    System.out.println("Client " + clientId + " finished work.");
                    out.println("ACK");
                    break;
            }
        }
        
        private synchronized void sort(int depth, String array_data) 
        {
            String[] strNumbers = array_data.split(",");
            int[] arr = new int[strNumbers.length];
            for (int i = 0; i < strNumbers.length; i++)
                arr[i] = Integer.parseInt(strNumbers[i]);

            if (depth == -1)
                depth = (int) (Math.log(Runtime.getRuntime().availableProcessors()) / Math.log(2));

            parallelMergeSort(arr, 0, arr.length - 1, depth);
            out.println("SORT_COMPLETE:" + formatArray(arr));
        }

        private synchronized void test_speedup(int depth, String data) 
        {
            String[] strNumbers = data.split(",");
            int[] arr = new int[strNumbers.length];
            for (int i = 0; i < strNumbers.length; i++)
                arr[i] = Integer.parseInt(strNumbers[i]);
            
            long seqTime = RunSequential(Arrays.copyOf(arr, arr.length));
            long parTime = RunParallel(arr, depth);
            out.println("TEST_SPEEDUP_COMPLETE:" + formatArray(arr) + 
                                ";" + seqTime + ";" + parTime + ";" + 
                                String.format("%.2f", (double)seqTime / parTime)
            );
        }
    }

    private static String formatArray(int[] arr) 
    {
        return String.join(",", 
                    Arrays.stream(arr)
                        .mapToObj(String::valueOf)
                        .toArray(String[]::new)
                    );
    }

    public static void sequentialMergeSort(int[] arr, int left, int right) 
    {
        if (left < right) 
        {
            int mid = (left + right) / 2;
            sequentialMergeSort(arr, left, mid);
            sequentialMergeSort(arr, mid + 1, right);
            merge(arr, left, mid, right);
        }
    }
    
    public static void parallelMergeSort(int[] arr, int left, int right, int depth) 
    {
        if (left < right) 
        {
            if (right - left < THRESHOLD || depth <= 0) 
            {
                sequentialMergeSort(arr, left, right);
                return;
            }
        
            int mid = (left + right) / 2;
    
            Thread leftThread = new Thread(() -> {
                parallelMergeSort(arr, left, mid, depth - 1);
            });
            
            Thread rightThread = new Thread(() -> {
                parallelMergeSort(arr, mid + 1, right, depth - 1);
            });
            
            leftThread.start();
            rightThread.start();
            
            try 
            {
                leftThread.join();
                rightThread.join();
            } 
            catch (InterruptedException e) 
            {
                Thread.currentThread().interrupt();
            }
            merge(arr, left, mid, right);
        }
    }
    
    private static void merge(int[] arr, int left, int mid, int right) 
    {
        int n1 = mid - left + 1;
        int n2 = right - mid;
        
        int[] leftArr = new int[n1];
        int[] rightArr = new int[n2];
        
        System.arraycopy(arr, left, leftArr, 0, n1);
        System.arraycopy(arr, mid + 1, rightArr, 0, n2);
        
        int i = 0, j = 0, k = left;
        
        while (i < n1 && j < n2) 
        {
            if (leftArr[i] <= rightArr[j])
                arr[k++] = leftArr[i++];
            else
                arr[k++] = rightArr[j++];
        }
        
        while (i < n1)
            arr[k++] = leftArr[i++];
        
        while (j < n2)
            arr[k++] = rightArr[j++];
    }

    private static long RunSequential(int[] arr) 
    {
        long start1 = System.currentTimeMillis();
        sequentialMergeSort(arr, 0, arr.length - 1);
        return System.currentTimeMillis() - start1;
    }

    private static long RunParallel(int[] arr, int depth)
    {
        long start2 = System.currentTimeMillis();
        if (depth == -1)
            depth = (int) (Math.log(Runtime.getRuntime().availableProcessors()) / Math.log(2));
        parallelMergeSort(arr, 0, arr.length - 1, depth);
        return System.currentTimeMillis() - start2;
    }
    
    public static void main(String[] args) 
    {
        System.out.println("Server started " + PORT);
        
        ExecutorService executor = Executors.newCachedThreadPool();
        
        try (ServerSocket serverSocket = new ServerSocket(PORT)) 
        {
            while (true) 
            {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client: " + clientSocket.getInetAddress());
                
                executor.submit(new ClientHandler(clientSocket, nextClientId++));
            }
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        } 
        finally 
        {
            executor.shutdown();
        }
    }
}