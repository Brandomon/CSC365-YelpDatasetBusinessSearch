import java.io.*;
import java.util.*;

public class PersistentHashtable implements Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String, String> hashtable;
    private Map<String, String> cache;
    private List<Long> blockOffsets;
    private static String hashTableFilePath = "hashtable.ser";
    
    public static void main (String[] args) {
        PersistentHashtable hashtable = new PersistentHashtable();

        // add entries to the hashtable
        hashtable.put("Google", "google.txt");
        hashtable.put("Microsoft", "microsoft.txt");
        hashtable.put("Apple", "apple.txt");
        hashtable.put("Amazon", "amazon.txt");
        hashtable.put("Facebook", "facebook.txt");
        hashtable.put("Netflix", "netflix.txt");
        hashtable.put("Tesla", "tesla.txt");
        hashtable.put("Alphabet", "alphabet.txt");
        hashtable.put("Oracle", "oracle.txt");
        hashtable.put("IBM", "ibm.txt");
        hashtable.put("Intel", "intel.txt");
        hashtable.put("Cisco", "cisco.txt");
        hashtable.put("Salesforce", "salesforce.txt");
        hashtable.put("Adobe", "adobe.txt");
        hashtable.put("PayPal", "paypal.txt");
        hashtable.put("Uber", "uber.txt");
        hashtable.put("Lyft", "lyft.txt");
        hashtable.put("Airbnb", "airbnb.txt");
        hashtable.put("Snap", "snap.txt");
        hashtable.put("Twitter", "twitter.txt");
        hashtable.put("Pinterest", "pinterest.txt");
        hashtable.put("Square", "square.txt");
        hashtable.put("Zoom", "zoom.txt");
        hashtable.put("Slack", "slack.txt");
        hashtable.put("Dropbox", "dropbox.txt");
        hashtable.put("GitHub", "github.txt");
        hashtable.put("LinkedIn", "linkedin.txt");
        hashtable.put("Reddit", "reddit.txt");
        hashtable.put("Wikipedia", "wikipedia.txt");
        hashtable.put("YouTube", "youtube.txt");
        hashtable.put("TikTok", "tiktok.txt");
        hashtable.put("Instagram", "instagram.txt");
        hashtable.put("WhatsApp", "whatsapp.txt");
        hashtable.put("WeChat", "wechat.txt");
        hashtable.put("Telegram", "telegram.txt");
        hashtable.put("Signal", "signal.txt");
        hashtable.put("Twitter", "twitter.txt");
        hashtable.put("Netflix", "netflix.txt");
        hashtable.put("Tesla", "tesla.txt");
        hashtable.put("General Electric", "ge.txt");
        hashtable.put("Intel", "intel.txt");
        hashtable.put("Oracle", "oracle.txt");
        hashtable.put("IBM", "ibm.txt");
        hashtable.put("Cisco", "cisco.txt");
        hashtable.put("Ford", "ford.txt");
        hashtable.put("Boeing", "boeing.txt");
        hashtable.put("Coca-Cola", "coke.txt");
        hashtable.put("McDonald's", "mcdonalds.txt");
        hashtable.put("PepsiCo", "pepsi.txt");
        hashtable.put("Procter & Gamble", "pg.txt");
        hashtable.put("Johnson & Johnson", "jj.txt");
        hashtable.put("Merck & Co.", "merck.txt");
        hashtable.put("Walmart", "walmart.txt");
        hashtable.put("Home Depot", "homedepot.txt");
        hashtable.put("Visa", "visa.txt");
        hashtable.put("Mastercard", "mastercard.txt");
        hashtable.put("JPMorgan Chase", "jpmorgan.txt");
        hashtable.put("Goldman Sachs", "goldmansachs.txt");
        hashtable.put("Morgan Stanley", "morganstanley.txt");
        hashtable.put("American Express", "amex.txt");
        hashtable.put("ExxonMobil", "exxon.txt");
        hashtable.put("Chevron", "chevron.txt");
        hashtable.put("BP", "bp.txt");
        
        // save the hashtable to a file
        try {
            hashtable.save();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        // Initialize new empty hashtable
        hashtable = new PersistentHashtable();
        
        // get an entry from the hashtable
        String fileName = hashtable.get("Google");
        System.out.println(fileName); // prints "null" because new hashtable is empty
        
        // load the hashtable from the file
        try {
            hashtable.load();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }

        // get an entry from the hashtable
        fileName = hashtable.get("Google");
        System.out.println(fileName); // prints "google.txt"

        // get another entry from the hashtable
        fileName = hashtable.get("Microsoft");
        System.out.println(fileName); // prints "microsoft.txt"
        
        // get another entry from the hashtable
        fileName = hashtable.get("BP");
        System.out.println(fileName); // prints "bp.txt"
        
	     // get another entry from the hashtable
        fileName = hashtable.get("Goldman Sachs");
        System.out.println(fileName); // prints "goldmansachs.txt"
        
        // get another entry from the hashtable
        fileName = hashtable.get("Procter & Gamble");
        System.out.println(fileName); // prints "pg.txt"
    }

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