import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// MUST RUN BusinessSearch.java FIRST TO WRITE ALL 10,000 FILES TO FOLDER

public class ReadFromFilesTest {
	public static void main(String[] args) {
		readBusinessIdFromFile("0IYUdfag5M07C_darC8boA");
		System.out.println("------------------------------------------------------------------------------------------");
		readBusinessIdFromFile("bYjnX_J1bHZob10DoSFkqQ");
		System.out.println("------------------------------------------------------------------------------------------");
		readBusinessIdFromFile("wghnIlMb_i5U46HMBGx9ig");
		System.out.println("------------------------------------------------------------------------------------------");
		readBusinessIdFromFile("-W_xAFTRKKOg5PvulS0G8A");
		System.out.println("------------------------------------------------------------------------------------------");
		readBusinessIdFromFile("Je-tv7RquXMb53jKip48dg");
	}
	public static void readBusinessIdFromFile(String bId) {
		int totalByteSize;
		int bIdByteLength;
		int bNameByteLength;
		int bAddressByteLength;
		int bCategoriesByteLength;
		int revIdByteLength;
		int reviewByteLength;
		int revIdsCount;
		int reviewCount;
		int count;
		String bizId;
		String bName;
		String bAddress;
		String bCategories;
		double x;
		double y;
		int cluster;
		String revId;
		String reviewString;
		String fileName;
		ArrayList<String> revIds = new ArrayList<String>();
		ArrayList<String> review = new ArrayList<String>();
		ArrayList<ArrayList<String>> reviews = new ArrayList<ArrayList<String>>();
		ByteBuffer byteSizeBuffer = ByteBuffer.allocate(Integer.BYTES);
		PersistentHashtable hashtable = new PersistentHashtable();
        // load the hashTable from the file
        try {
            hashtable.load();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        fileName = hashtable.get(bId);

		// Try reading file input stream channel from file
		try (FileInputStream inputStream = new FileInputStream(fileName);
			 FileChannel inChannel = inputStream.getChannel()) {
			
			// Clear byteSizeBuffer
			byteSizeBuffer.clear();
			
			// Read inChannel and write to byteSizeBuffer
			inChannel.read(byteSizeBuffer);
			
			// Flip byteSizeBuffer
			byteSizeBuffer.flip();
			
			// Get totalByteSize from byteSizeBuffer
			totalByteSize = byteSizeBuffer.getInt();
			System.out.println("Total Byte Size: " + totalByteSize);
			
			// Allocate buffer of totalByteSize bytes
			ByteBuffer buffer = ByteBuffer.allocate(totalByteSize);
			
			// Read inChannel and write to buffer
			inChannel.read(buffer);
			
			// Flip buffer
			buffer.flip();
			
			// Read bIdByteLength from buffer
			bIdByteLength = buffer.getInt();
			
			// Read bizId from buffer
			byte[] bizIdBytes = new byte[bIdByteLength];
			buffer.get(bizIdBytes);
			bizId = new String(bizIdBytes);
			
			System.out.println("bIdByteLength: " + bIdByteLength);
			System.out.println("bizId: " + bizId);
			
			// Read bNameByteLength from buffer
			bNameByteLength = buffer.getInt();
			
			// Read bNameByteLength from buffer
			byte[] bNameBytes = new byte[bNameByteLength];
			buffer.get(bNameBytes);
			bName = new String(bNameBytes);
			
			System.out.println("bNameByteLength: " + bNameByteLength);
			System.out.println("bName: " + bName);
			
			// Read bAddressByteLength from buffer
			bAddressByteLength = buffer.getInt();
			
			// Read bAddress from buffer
			byte[] bAddressBytes = new byte[bAddressByteLength];
			buffer.get(bAddressBytes);
			bAddress = new String(bAddressBytes);
			
			System.out.println("bAddressByteLength: " + bAddressByteLength);
			System.out.println("bAddress: " + bAddress);
			
			// Read bCategoriesByteLength from buffer
			bCategoriesByteLength = buffer.getInt();
			
			// Read bCategories from buffer
			byte[] bCategoriesBytes = new byte[bCategoriesByteLength];
			buffer.get(bCategoriesBytes);
			bCategories = new String(bCategoriesBytes);
			
			System.out.println("bCategoriesByteLength: " + bCategoriesByteLength);
			System.out.println("bCategories: " + bCategories);
			
			// Read revIdsCount from buffer
			revIdsCount = buffer.getInt();
			System.out.println("reviewCount: " + revIdsCount);
			
			// Read revIds from buffer
			for (count = 0; count < revIdsCount; count++) {
				revIdByteLength = buffer.getInt();
				byte[] revIdBytes = new byte[revIdByteLength];
				buffer.get(revIdBytes);
				revId = new String(revIdBytes);
				revIds.add(revId);
			}
			
			System.out.println("revIds: ");
			for (String rev : revIds) {
				System.out.println(rev);
			}
			
			// Read reviewCount from buffer
			reviewCount = buffer.getInt();			
			System.out.println("reviewCount: " + reviewCount);
			
			// Read reviews from buffer
			for (count = 0; count < reviewCount; count++) {
				reviewByteLength = buffer.getInt();
				byte[] reviewBytes = new byte[reviewByteLength];
				buffer.get(reviewBytes);
				reviewString = new String(reviewBytes);
				
				// Remove "[" and "]" at beginning and end of review string
				reviewString = reviewString.substring(1);
				reviewString = reviewString.substring(0, reviewString.length() - 1);
				
				// Convert reviewString into review ArrayList<String>
				review = new ArrayList<String>(Arrays.asList(reviewString));
				
				// Add review ArrayList<String> to reviews ArrayList<ArrayList<String>>
				reviews.add(review);
			}
			
			System.out.println("reviews: ");
			for (ArrayList<String> rev : reviews) {
				System.out.println(rev);
			}
			
			// Get x-coordinate, y-coordinate, and cluster number from buffer
			x = buffer.getDouble();
			y = buffer.getDouble();
			cluster = buffer.getInt();
			
			System.out.println("X-Coordinate: " + x);
			System.out.println("Y-Coordinate: " + y);
			System.out.println("Cluster #: " + cluster);
			
		} catch (IOException e) {
			System.err.println("Error reading input from file: " + e.getMessage());
            return;
		}
	}
    private static class PersistentHashtable implements Serializable {
        private static final long serialVersionUID = 1L;
        private static String hashTableFilePath = "hashtable.ser";
        private Map<String, String> hashtable;
        private Map<String, String> cache;
        private List<Long> blockOffsets;

        public PersistentHashtable() {
            hashtable = new HashMap<String, String>();
            cache = new LinkedHashMap<>(16, 0.75f, true);
            blockOffsets = new ArrayList<>();
            }

        public void put(String businessName, String fileName) {
            hashtable.put(businessName, fileName);
            cache.put(businessName, fileName);
        }

        public String get(String businessName) {
            // check if the key is in the cache
            if (cache.containsKey(businessName)) {
                // update the position of the key as the most recently used
                String fileName = cache.remove(businessName);
                cache.put(businessName, fileName);
                return fileName;
            }

            // check if the key is in the hashtable
            if (hashtable.containsKey(businessName)) {
                // add the key-value pair to the cache and return the value
                String fileName = hashtable.get(businessName);
                cache.put(businessName, fileName);
                // check if the cache has exceeded its capacity and remove the least recently used key
                if (cache.size() > 16) {
                    Iterator<Map.Entry<String, String>> it = cache.entrySet().iterator();
                    it.next();
                    it.remove();
                }
                return fileName;
            }

            // key not found in hashtable
            return null;
        }
        public void save() throws IOException {
            try (RandomAccessFile file = new RandomAccessFile(hashTableFilePath, "rw")) {
                blockOffsets.clear();
                cache.forEach((key, value) -> {
                    try {
                        byte[] keyBytes = key.getBytes();
                        byte[] valueBytes = value.getBytes();
                        int keyLength = keyBytes.length;
                        int valueLength = valueBytes.length;
                        int totalLength = keyLength + valueLength + 8;
                        long blockOffset = file.getFilePointer();
                        blockOffsets.add(blockOffset);
                        file.writeInt(totalLength);
                        file.writeInt(keyLength);
                        file.write(keyBytes);
                        file.writeInt(valueLength);
                        file.write(valueBytes);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(hashTableFilePath + ".index"))) {
                    out.writeObject(blockOffsets);
                }
            }
        }
        public void load() throws IOException, ClassNotFoundException {
            try (RandomAccessFile file = new RandomAccessFile(hashTableFilePath, "r")) {
                cache.clear();
                blockOffsets.clear();
                try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(hashTableFilePath + ".index"))) {
                    blockOffsets = (List<Long>) in.readObject();
                }
                for (Long offset : blockOffsets) {
    	            file.seek(offset);
    	            int totalLength = file.readInt();
    	            int keyLength = file.readInt();
    	            byte[] keyBytes = new byte[keyLength];
    	            file.read(keyBytes);
    	            int valueLength = file.readInt();
    	            byte[] valueBytes = new byte[valueLength];
    	            file.read(valueBytes);
    	            String key = new String(keyBytes);
    	            String value = new String(valueBytes);
    	            cache.put(key, value);
    	            hashtable.put(key, value);
                }
            }
        }
    }
}
