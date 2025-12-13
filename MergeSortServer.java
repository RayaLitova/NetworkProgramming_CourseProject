public class MergeSortServer 
{    
    private static final int THRESHOLD = 1000;
    
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

    private static long RunParallel(int[] arr)
    {
        long start2 = System.currentTimeMillis();
        int depth = (int) (Math.log(Runtime.getRuntime().availableProcessors()) / Math.log(2));
        parallelMergeSort(arr, 0, arr.length - 1, depth);
        return System.currentTimeMillis() - start2;
    }
    
    public static void main(String[] args) 
    {
        int[] sizes = { (int)Math.pow(10, 4), 
                        (int)Math.pow(10, 5), 
                        (int)Math.pow(10, 6), 
                        (int)Math.pow(10, 7)};
        
        for (int size : sizes) {
            System.out.println("\nSize: " + size);
            
            int[] arr = new int[size];
            for (int i = 0; i < size; i++)
                arr[i] = (int) (Math.random() * 10000);
            
            long timeSeq = RunSequential(arr.clone());
            System.out.println("Sequential: " + timeSeq + " ms");
            
            long timePar = RunParallel(arr.clone());
            System.out.println("Parallel: " + timePar + " ms");

            System.out.printf("Speedup: %.2fx\n", (double)timeSeq / timePar);
        }
    }
}