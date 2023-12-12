//*************************************************************************************************
//
//	Brandon LaPointe & Param Rajguru
//	CSC365 - Professor Doug Lea
//	Loader.java
//

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import com.google.gson.Gson;	// Uses GSON 2.8.2 jar to parse JSON

public class Loader {
	
	// Class Constants & Variables
	private static final int MAX_NUM = 10_000;					// Maximum number of businesses to add to object array
	private static final String[] FILLER_WORDS = {				// List of filler words to remove from reviews		
		"a", "about", "above", "across", "actually", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at",
		"b", "back", "basically", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by",
		"c", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry",
		"d", "de", "describe", "detail", "do", "done", "down", "due", "during",
		"e", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "esp", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except",
		"f", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further",
		"g", "get", "give", "go", "going", "gone",
		"h", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred",
		"i", "i'd", "ie", "if", "i'll", "i'm", "in", "inc", "indeed", "instead", "interest", "into", "is", "it", "its", "it's", "itself", "ive", "i've",
		"j", "just",
		"k", "keep",
		"l", "last", "latter", "latterly", "least", "left", "less", "like", "literally", "ltd",
		"m", "made", "make", "makes", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself",
		"n", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere",
		"o", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own",
		"p", "part", "per", "perhaps", "place", "please", "put",
		"q",
		"r", "rather", "re", "right",
		"s", "said", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system",
		"t", "take", "ten", "than", "that", "that's", "that'll", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "they're", "they've", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two",
		"u", "un", "under", "until", "up", "upon", "us",
		"v", "very", "via",
		"w", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would",
		"x",
		"y", "yet", "you", "your", "yours", "yourself", "yourselves",
		"z"};
	
	// JSON File Names
	private static String businessesAddress = "yelp_dataset_business.json";							// Address of truncated business JSON file used (Truncated Yelp Academic Dataset Business)
	private static String reviewsAddress = "yelp_dataset_review.json";								// Address of truncated review JSON file used (Yelp Academic Dataset Review)
	
	// TFIDF Variables
	private static Business[] businesses;															// Array of business objects
	private static ArrayList<Review> reviews = new ArrayList<Review>();								// Array List of review objects
	private static ArrayList<String> catAryList = new ArrayList<String>();							// Expandable array list containing all of the categories from all businesses
	private static ArrayList<ArrayList<String>> catAryAryList = new ArrayList<ArrayList<String>>();	// Array list containing string arrays of categories from all businesses
	private static ArrayList<String> revAryList = new ArrayList<String>();							// Expandable array list containing all of the reviews from all businesses
	private static ArrayList<ArrayList<String>> revAryAryList = new ArrayList<ArrayList<String>>();	// Array list containing array lists of strings of reviews from all businesses
	
	// BusinessData Folder Variables
	private static String folderName = "BusinessData";												// Name of folder where files for all businesses are stored
	private static File folder = new File(folderName);												// File folder created from String folderName
	
	// K-Means Clustering Variables
    public static HashMap<Integer, List<Point>> cluster_points = new HashMap<Integer, List<Point>>();
    private static int NUM_CLUSTERS = 10;
    private static int MAX_ITER = 1000;
    private static List<Point> points; 
    private static List<Point> clusters;
    
	// Businesses HashMaps
	private static HashMap <String, String> bName_bId = new HashMap <String, String>();								// HashMap of business name to business id
	private static HashMap <String, String> bId_bName = new HashMap <String, String>();								// HashMap of business id to business name
	private static HashMap <String, String[]> bId_category = new HashMap <String, String[]>();						// HashMap of business id to business categories
	private static HashMap <Integer, String> index_bId = new HashMap <Integer, String>();							// HashMap of index to business id
	private static HashMap <String, Integer> bId_index = new HashMap <String, Integer>();							// HashMap of business id to index
	private static HashMap <String, Double> category_tfidf = new HashMap <String, Double>();						// HashMap of categories to tfIdf
	private static HashMap <String, String> bId_address = new HashMap <String, String>();							// HashMap of business id to address
	
	// Reviews HashMaps
	private static HashMap <String, String> reviewId_bId = new HashMap <String, String>();							// HashMap of review id to business id
	private static HashMap <String, ArrayList<String>> bId_reviewIds = new HashMap <String, ArrayList<String>>();	// HashMap of businessId to reviewId ArrayList
	private static HashMap <String, String> reviewId_text = new HashMap <String, String>();							// HashMap of reviewId to reviewText
	private static HashMap <String, Double> reviewWord_tfidf = new HashMap <String, Double>();						// HashMap of reviewWord to tfIdf
	
	// Similarity Metric HashMaps
	private static HashMap<String, String> bId_initCategories = new HashMap<String, String>();						// HashMap of bId to initializer categories
	private static HashMap <String, Double> bId_categorySimilarity = new HashMap <String, Double>();				// HashMap of bId to categorySimilarity
	private static HashMap <String, Double> bId_reviewSimilarity = new HashMap <String, Double>();					// HashMap of bId to reviewSimilarity
	
	// Word Frequency Tables
	static WordFrequencyTable categoryWordFrequencyTable = new WordFrequencyTable();
	static WordFrequencyTable reviewWordFrequencyTable = new WordFrequencyTable();
	
	public static void main(String[] args) {
		
		// Convert JSON file to business object array
		System.out.println("Fetching business json file data...");
		getJsonBusinesses();
		
		// Populate Business HashMaps
		System.out.println("Populating Business Hashmaps...");
		populateBusinessHashmaps();
		
		// Initialize Persistent Hash Table
		System.out.println("Initializing Persistent Hash Table...");
		initializeBusniessIdFileNameHashTable();
		
		// Convert JSON file to reviews object array
		System.out.println("Fetching review json file data...");
		getJsonReviews();
		
		// Populate Review HashMaps
		System.out.println("Populating Review Hashmaps...");
		populateReviewsHashmaps();
		
		// Calculate Similarity Metrics of Categories and Reviews
		calculateSimilarityMetrics();
        
        System.out.println("Gathering Points...");
        points = getPoints();
        
        System.out.println("Clustering Points...");
        clusters = clusterPoints(points, NUM_CLUSTERS, MAX_ITER);

        System.out.println("Creating Clustering Points...");
        createClusterPoints(points);

		System.out.println("Writing business data to files...");
		writeBusinessesToFiles();
		
		System.out.println("Loader Processing Complete");
	}
	public final class Business {		
		// Object Variables
		private String business_id;
		private String name;
		private String address;
		private String categories;
		
		//*********************************************
		// Getters and setters
		public String getId() {
			return business_id;
		}
		public void setId(String business_id) {
			this.business_id = business_id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getAddress() {
			return address;
		}
		public void setAddress(String address) {
			this.address = address;
		}
		public String getCategories() {
			return categories;
		}
		public void setCategories(String categories) {
			this.categories = categories;
		}
	}
	public final class Review {		
		// Object Variables
		private String review_id;
		private String business_id;
		private String text;
		
		//*********************************************
		// Getters and setters
		public String getReviewId() {
			return review_id;
		}
		public void setReviewId(String review_id) {
			this.review_id = review_id;
		}
		public String getBusinessId() {
			return business_id;
		}
		public void setBusinessId(String business_id) {
			this.business_id = business_id;
		}
		public String getText() {
			return text;
		}
		public void setText(String text) {
			this.text = text;
		}
	}
    private static class Point {
        double x;
        double y;
        int clusterID;
        String businessId;
        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }
        public double distance(Point other) {
            double dx = x - other.x;
            double dy = y - other.y;
            return Math.sqrt(dx * dx + dy * dy);
        }
        public void addToCluster(int clusterID){
            this.clusterID = clusterID;
        }
        public void addBusinessId(String businessId){
            this.businessId = businessId;
        }
        @Override
        public String toString() {
            return "(" + x + ", " + y + ")";
        }
    }
	public static class WordFrequencyTable implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		
		//*********************************************
		// Node Class
		static final class Node {
			String key;
			Node next;
			int count;
			Node(String k, int c, Node n) { key = k; count = c; next = n;}
		}
		
		//*********************************************
		// Instantiate Node Array
		static Node[] table = new Node[8]; // Always a power of 2
		static int size = 0;
		
		//*********************************************
		// GetCount Function
		int getCount(String key) {
			int h = key.hashCode();
			int i = h & (table.length - 1);						// i = bitwise AND on h using table.length-1 (like a bit-mask, to return only the low-order bits of h. Basically a super-fast variant of h % table.length)
			for (Node e = table[i]; e != null; e = e.next) {
				if (key.equals(e.key)) {
					return e.count;
				}
			}
		return 0;
		}
		
		//*********************************************
		// Add Function
		void add(String key) {
			int h = key.hashCode();
			int i = h & (table.length - 1);
			for (Node e = table[i]; e != null; e = e.next) {
				if (key.equals(e.key)) {
					++e.count;
					return;
				}
			}
			table[i] = new Node(key, 1, table[i]);
			++size;
			if ((float)size/table.length >= 0.75f) {
				resize();
				//System.out.println("Table resized to size : " + table.length);
			}
		}
		
		//*********************************************
		// Resize Function
		void resize() {
			Node[] oldTable = table;
			int oldCapacity = oldTable.length;
			int newCapacity = oldCapacity << 1;
			Node[] newTable = new Node[newCapacity];
			for (int i = 0; i < oldCapacity; ++i) {
				for (Node e = oldTable[i]; e != null; e = e.next) {
					int h = e.key.hashCode();
					int j = h & (newTable.length - 1);
					newTable[j] = new Node(e.key, e.count, newTable[j]);
				}
			}
			table = newTable;			
		}
		
		//*********************************************
		// Remove Function
		void remove(String key) {
			int h = key.hashCode();
			int i = h & (table.length - 1);
			Node e = table[i], p = null;
			while (e != null) {
				if (key.equals(e.key)) {
					if (p == null) {
						table[i] = e.next;
					}
					else {
						p.next = e.next;
					}
					break;
				}
				p = e;
				e = e.next;
			}
		}
		
		//*********************************************
		// PrintAll Function
		void PrintAll() {
			for (int i = 0; i < table.length; ++i)
				for (Node e = table[i]; e != null; e = e.next) {
					System.out.println("Key : " + e.key + " --- Count : " + e.count);
				}
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
	private static void getJsonBusinesses() {
		
		businesses = new Business[MAX_NUM];		// Array of business objects limited to MAX_NUM class constant
		
		try (BufferedReader reader = new BufferedReader(new FileReader(businessesAddress))) {
			 
			 // Class Variables
			 String line;						// Line of JSON file read in through buffered reader as a string
			 int index = 0;						// Index for adding to array of businesses
			 
			 // While line is not null
			 while ((line = reader.readLine()) != null) {		 
				 // Create new business object from JSON file line taking in business object variables
				 Business business = new Gson().fromJson(line, Business.class);
				 // Insert business information into object array of businesses
				 businesses[index] = business;				 
				 // Increment index counter
				 index++;
			 }
	    } catch(IOException e) {
	    	e.printStackTrace();
	    }
	}
	private static void getJsonReviews() {		
		try (BufferedReader reader = new BufferedReader(new FileReader(reviewsAddress))) {
			 
			 // Class Variables
			 String line;						// Line of JSON file read in through buffered reader as a string
			 int index = 0;						// Index for adding to array of businesses
			 
			 // While line is not null
			 while ((line = reader.readLine()) != null) {		 
				 // Create new review object from JSON file line taking in review object variables
				 Review review = new Gson().fromJson(line, Review.class);
				 // Insert business information into object array of reviews
				 reviews.add(index, review);				 
				 // Increment index counter
				 index++;
			 }
	    } catch(IOException e) {
	    	e.printStackTrace();
	    }
	}
	private static void populateBusinessHashmaps() {
		
		ArrayList<String> categories = new ArrayList<String>();			// Array initialized to hold categories of a single business
		
		// For each business within the array of businesses
		for (int b = 0; b < businesses.length; b++) {
			// If business categories are not null
			if (businesses[b].categories != null) {
				// Create array list of categories
				categories = new ArrayList<String>(Arrays.asList(businesses[b].categories));
				String[] categoriesArray = businesses[b].categories.split(", ");
				for (String category : categoriesArray) {
					// Add to array list containing all categories of all businesses
					catAryList.add(category);
					// Add to category word frequency table
					categoryWordFrequencyTable.add(category);
				}
				// Add to array list containing all array lists of categories of all businesses
				catAryAryList.add(categories);
			    // Put values into hashmaps
				bName_bId.put(businesses[b].name, businesses[b].business_id); 						// Hashmap of name->id
				bId_bName.put(businesses[b].business_id, businesses[b].name); 						// Hashmap of id->name
			    bId_category.put(businesses[b].business_id, businesses[b].categories.split(", ")); 	// Hashmap of id->categories
			    index_bId.put(b, businesses[b].business_id); 										// Hashmap of index->id (helps with accessing business_id from index)
			    bId_index.put(businesses[b].business_id, b); 										// Hashmap of id->index (helps with accessing catAryAryList)
			    bId_address.put(businesses[b].business_id, businesses[b].address);					// Hashmap of id->address
			}
		}
		System.out.println("Calculating Category TFIDFs...");
		// Traverse through populated businesses array to get TFIDF of each category
		for (int b = 0; b < businesses.length; b++) {			
			// If business categories are not null
			if (businesses[b].categories != null) {
				categories = new ArrayList<String>(Arrays.asList(businesses[b].categories));
				String[] categoriesArray = businesses[b].categories.split(", ");
				for (String category : categoriesArray) {
					if (!category_tfidf.containsKey(category)) {
						// Calculate tfidf and apply to category_tfidf hashmap
						category_tfidf.put(category, tfIdf(catAryList, catAryAryList, category, categoryWordFrequencyTable));
					}
				}
			}
		}
	}
	private static void populateReviewsHashmaps() {
		
		ArrayList<String> reviewsArray = new ArrayList<String>();
		HashMap<String, Double> idfValues = new HashMap<String, Double>();

		// Add reviews to hashMaps
		for (int x = 0; x < reviews.size(); x++) {
			// If review text is not null
			if (reviews.get(x).text != null) {
			    // Put values into hashmaps
				if (bId_reviewIds.containsKey(reviews.get(x).business_id))
					bId_reviewIds.get(reviews.get(x).business_id).add(reviews.get(x).review_id);
				else {
					bId_reviewIds.put(reviews.get(x).business_id, new ArrayList<String>());
					bId_reviewIds.get(reviews.get(x).business_id).add(reviews.get(x).review_id);
				}
				reviewId_bId.put(reviews.get(x).review_id, reviews.get(x).business_id);
				reviewId_text.put(reviews.get(x).review_id, reviews.get(x).text);
			}
		}
		System.out.println("Calculating IDF Values...");
	    // Calculate IDF values for all terms in the reviews
	    for (int r = 0; r < reviews.size(); r++) {
	        if (reviews.get(r).text != null && bId_bName.containsKey(reviews.get(r).business_id)) {
	            reviewsArray = removeFillerWords(reviews.get(r).text);
	            for (String word : reviewsArray) {
	                if (!idfValues.containsKey(word)) {
	                    double idfValue = idf(revAryAryList, word, reviewWordFrequencyTable);
	                    idfValues.put(word, idfValue);
	                }
	            }
	        }
	    }

	    System.out.println("Filling Review Array Lists...");
	    // For each review within the array of reviews
	    for (int r = 0; r < reviews.size(); r++) {
	        // If review text is not null & bId_reviewId hashMap contains key value of review business id
	        if (reviews.get(r).text != null && bId_bName.containsKey(reviews.get(r).business_id)) {
	            // Put individual words of review into revAryList
	            reviewsArray = removeFillerWords(reviews.get(r).text);
	            for (String word : reviewsArray) {
	                revAryList.add(word);
	                reviewWordFrequencyTable.add(word);
	            }
	            // Put review array list into revAryAryList
	            revAryAryList.add(reviewsArray);
	        }
	    }

	    System.out.println("Calculating Review TFIDFs...");
	    // Traverse through populated businesses array to get TFIDF of each category
	    for (int r = 0; r < reviews.size(); r++) {
	        // If review text is not null & bId_reviewId hashMap contains key value of review business id
	        if (reviews.get(r).text != null && bId_bName.containsKey(reviews.get(r).business_id)) {
	            reviewsArray = removeFillerWords(reviews.get(r).text);
	            for (String word : reviewsArray) {
	                if (!reviewWord_tfidf.containsKey(word)) {
	                    // Retrieve pre-calculated IDF value from idfValues HashMap
	                    double idfValue = idfValues.get(word);
	                    // Calculate tfIdf and apply to reviewWord_tfidf hashMap
	                    double tfidfValue = tf(reviewsArray, word) * idfValue;
	                    reviewWord_tfidf.put(word, tfidfValue);
	                }
	            }
	        }
	    }
	}
	private static void initializeBusniessIdFileNameHashTable() {
        PersistentHashtable bId_fileName = new PersistentHashtable();
        
        for (Business business : businesses) {
        	bId_fileName.put(business.business_id, folderName + File.separator + business.business_id + ".bin");
        }        
        // save the hashTable to a file
        try {
            bId_fileName.save();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	private static double tf(ArrayList<String> doc, String term) {
        double result = 0;
        for (String word : doc) {
            if (term.equalsIgnoreCase(word))
                result++;
        }
        return result / doc.size();
    }
	private static double idf(ArrayList<ArrayList<String>> docs, String term, WordFrequencyTable freqTable) {
        double n = 0;
        n = freqTable.getCount(term);					// Implementation of word frequency table
        return Math.log(((1 + docs.size()) /(1 + n)) + 1);
    }
	private static double tfIdf(ArrayList<String> doc, ArrayList<ArrayList<String>> docs, String term, WordFrequencyTable freqTable) {
        return tf(doc, term) * idf(docs, term, freqTable);
    }
	private static double cosineSimilarity(ArrayList<String> doc1, ArrayList<String> doc2, Map<String, Double> tfIdfMap) {
		// Create a set of all unique words from both documents
		Set<String> uniqueWords = new HashSet<>();
		uniqueWords.addAll(doc1);
		uniqueWords.addAll(doc2);

		// Calculate dot product and magnitude of vectors for cosine similarity
		double dotProduct = 0;
		double doc1Magnitude = 0;
		double doc2Magnitude = 0;
		// For each word within set of unique words
		for (String word : uniqueWords) {
			// Get TFIDF of word within doc1(business 1 categories) and doc2 (business 2 categories)
			double tfIdf1 = tfIdfMap.getOrDefault(word, 0.0) * countOccurrences(word, doc1);
			double tfIdf2 = tfIdfMap.getOrDefault(word, 0.0) * countOccurrences(word, doc2);
			// Calculate dot product
			dotProduct += tfIdf1 * tfIdf2;
			// Calculate magnitude of vectors
			doc1Magnitude += tfIdf1 * tfIdf1;
			doc2Magnitude += tfIdf2 * tfIdf2;
		}

		// Calculate cosine similarity
		double similarity = dotProduct / (Math.sqrt(doc1Magnitude) * Math.sqrt(doc2Magnitude));
		// Return similarity
		return similarity;
	}
	private static int countOccurrences(String term, ArrayList<String> doc1) {
		int count = 0;
		for (String word : doc1) {
			if (term.equalsIgnoreCase(word)) {
				count++;
				break;
			}
		}
		return count;
	}
	private static ArrayList<String> removeFillerWords(String review) {
		ArrayList<String> shortenedReview = new ArrayList<String>();
		String[] reviewArray = review.split("[\\n\\s~!@#$%^&*()_+`1234567890\\-=;:\\\",<.>/?|\\[\\]]+"); // Removes all counts of spaces, new lines, numbers, and most symbols
		for (String word : reviewArray) {
			if (!Arrays.asList(FILLER_WORDS).contains(word.toLowerCase()) && !word.equals(""))
				shortenedReview.add(word);
		}
		return shortenedReview;
	}
	public static void writeBusinessesToFiles() {
	//********************************************************************************************************************************************************************************************************************************************************************************************
	//
	//                                  FORMAT OF BUSINESS DATA BYTEBUFFER:                                                                                 ______________________                   _________________________
	//                                                                                                                                                     //    *revIdsCount    \\                 //      *reviewCount     \\
	//	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	//  |     INT    ||    INT    ||  STRING  ||     INT    ||  STRING  ||      INT      ||  STRING  ||        INT       ||    STRING    ||     INT     ||     INT     || STRING ||      INT     ||     INT      ||  STRING  ||       DOUBLE       ||      DOUBLE      ||       INT     ||
	//  | totalBytes ||  bIdBytes ||   bId    || bNameBytes ||  bName   || bAddressBytes || bAddress || bCategoriesBytes ||  bCategories || revIdsCount || revIdBytes  || revId  || reviewCount  || reviewBytes  ||  review  || categorySimilarity || reviewSimilarity || clusterNumber ||
	//	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	//
	//********************************************************************************************************************************************************************************************************************************************************************************************
		int totalByteLength;
		int bIdLength;
		int bNameLength;
		int bAddressLength;
		int bCategoriesLength;
		int revIdsByteLength;
		int reviewsByteLength;
		int revIdsCount;
		int reviewCount;
		String bId;
		String bName;
		String bAddress;
		String bCategories;
		double x;
		double y;
		int cluster;
		
		// Create the BusinessData folder if it does not exist
		if (!folder.exists()) {
			folder.mkdir();
		}
        PersistentHashtable hashtable = new PersistentHashtable();
        // load the hashTable from the file
        try {
            hashtable.load();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
		// Write data for each business to separate file
		for (Business biz : businesses) {
			// Create new ArrayLists for revIds and reviews
			ArrayList<String> revIds = new ArrayList<String>();
			ArrayList<ArrayList<String>> reviews = new ArrayList<ArrayList<String>>();
			// Get data needed for file
			bId = biz.business_id;
			bIdLength = bId.getBytes().length;
			bName = biz.name;
			bNameLength = bName.getBytes().length;
			bAddress = biz.address;
			bAddressLength = bAddress.getBytes().length;
			bCategories = biz.categories;
			if (bCategories == null) {
				bCategories = "None";
			}
			bCategoriesLength = bCategories.getBytes().length;
			revIdsByteLength = 0;
			for (String reviewId : bId_reviewIds.get(bId)) {
				revIds.add(reviewId);
				revIdsByteLength += Integer.BYTES + reviewId.getBytes().length;
			}
			for (String reviewId : revIds) {
				reviews.add(removeFillerWords(reviewId_text.get(reviewId)));
			}
			reviewsByteLength = 0;
			for (ArrayList<String> review : reviews) {
				reviewsByteLength += (Integer.BYTES + review.toString().getBytes().length);
			}
			
			revIdsCount = revIds.size();
			reviewCount = revIdsCount;
			
			if (bId_categorySimilarity.get(bId) != null) {
				x = bId_categorySimilarity.get(bId);
			}
			else {
				x = 0.0;
			}
			if (bId_reviewSimilarity.get(bId) != null) {
				y = bId_reviewSimilarity.get(bId);
			}
			else {
				y = 0.0;
			}
			Point bIdPoints = new Point(x, y);
			cluster = 0;
			for (Point point : points) {
				if (x == point.x && y == point.y) {
					cluster = point.clusterID;
				}
			}
			
			totalByteLength = Integer.BYTES + bIdLength + Integer.BYTES + bNameLength + Integer.BYTES + bAddressLength + Integer.BYTES + bCategoriesLength + Integer.BYTES + revIdsByteLength + Integer.BYTES + reviewsByteLength + Double.BYTES + Double.BYTES + Integer.BYTES;
			
			// Allocate ByteBuffer for data
			ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + bIdLength + Integer.BYTES + bNameLength + Integer.BYTES + bAddressLength + Integer.BYTES + bCategoriesLength + Integer.BYTES + revIdsByteLength + Integer.BYTES + reviewsByteLength + Double.BYTES + Double.BYTES + Integer.BYTES);
			
			// Write data to file under current directory/BusinessData/"bId".bin
			try (FileOutputStream out = new FileOutputStream(hashtable.get(bId));
				 FileChannel outChannel = out.getChannel()) {
					 buffer.putInt(totalByteLength);
					 buffer.putInt(bIdLength);
					 buffer.put(bId.getBytes());
					 buffer.putInt(bNameLength);
					 buffer.put(bName.getBytes());
					 buffer.putInt(bAddressLength);
					 buffer.put(bAddress.getBytes());
					 buffer.putInt(bCategoriesLength);
					 buffer.put(bCategories.getBytes());
					 buffer.putInt(revIdsCount);
					 for (String reviewId : revIds) {
						 buffer.putInt(reviewId.getBytes().length);
						 buffer.put(reviewId.getBytes());
					 }
					 buffer.putInt(reviewCount);
					 for (ArrayList<String> review : reviews) {
						 buffer.putInt(review.toString().getBytes().length);
						 buffer.put(review.toString().getBytes());
					 }
					 buffer.putDouble(x);
					 buffer.putDouble(y);
					 buffer.putInt(cluster);
					 buffer.flip();
					 outChannel.write(buffer);
					 buffer.clear();
			} catch (IOException e) {
				System.err.println("Error writing input to file: " + e.getMessage());
				return;
			}
		}		
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
    private static void calculateSimilarityMetrics () {
    	calculateCategorySimilarities();
		calculateReviewSimilarities();
    }
    private static void calculateCategorySimilarities() {
        System.out.println("Calculating Category Similarities...");
        for (Business business : businesses) {
            if (business.categories != null) {
                String[] businessCategories = business.categories.split(", ");
                String initCategory = businessCategories[0];
                bId_initCategories.put(business.business_id, initCategory);
                double cachedSimilarity = bId_categorySimilarity.getOrDefault(business.business_id, -1.0);
                if (cachedSimilarity == -1.0 || !initCategory.equals(bId_initCategories.get(business.business_id))) {
                    double similarity = cosineSimilarity(new ArrayList<String>(Arrays.asList(initCategory)), new ArrayList<String>(Arrays.asList(businessCategories)), category_tfidf);
                    bId_categorySimilarity.put(business.business_id, similarity);
                }
            }
        }
    }
    private static void calculateReviewSimilarities() {
	    System.out.println("Calculating Review Similarities...");
	    
	    // Get initializer reviewWords from single bId reviews
	    
	    ArrayList<String> initReviewWords = new ArrayList<String>();
	    for (String bRevId : bId_reviewIds.get("GRMKMKiNabc9DNwCMNgG0Q")) {
	    	String review = reviewId_text.get(bRevId);
	    	initReviewWords = removeFillerWords(review);
	    }
	    
	    // For each business, calculate cosine similarity of review words compared to initializer review words
	    for (Business business : businesses) {
	        ArrayList<String> reviewWords = new ArrayList<String>();
	        // For each entry of the bId_reviewId hashmap with business id as key
	        for (String reviewId : bId_reviewIds.get(business.business_id)) {
		    	String review = reviewId_text.get(reviewId);
		    	reviewWords = removeFillerWords(review);
	        }
	        // Get cosine similarity
	        double x = cosineSimilarity(initReviewWords, reviewWords, reviewWord_tfidf);
	        if (Double.isNaN(x)) {
		        bId_reviewSimilarity.put(business.business_id, 0.0);
	        }
	        else {
		        bId_reviewSimilarity.put(business.business_id, x);
	        }
	        //System.out.println("Review similarity of " + business.business_id + " : " + bId_reviewSimilarity.get(business.business_id));
	    }
    }
    private static void createClusterPoints(List<Point> points){

        List<Point>[] cluster_list = new ArrayList[NUM_CLUSTERS];
        for (int i = 0; i < NUM_CLUSTERS; i++) {
        	// Create a new array with 10 array lists of point objects
            cluster_list[i] = new ArrayList<Point>();
        }

        for (Point p: points){
            cluster_list[p.clusterID].add(p);
        }
        for (int i = 0; i < NUM_CLUSTERS; i++){
            cluster_points.put(i, cluster_list[i]);
        }
    }
    private static ArrayList<Point> getPoints(){
    	ArrayList<Point> points = new ArrayList<Point>();
         int numPoints = 10000;
         double x;
         double y;
         // Access the similarity metric from the files
         
         // Generate points;
         for (int i = 0; i < numPoints; i++) {
        	 String businessId = businesses[i].business_id;
        	 if (bId_categorySimilarity.get(businessId) != null) {
        		 x = bId_categorySimilarity.get(businessId);
        	 }
        	 else {
        		 x = 0.0;
        	 }
        	 if (bId_reviewSimilarity.get(businessId) != null) {
        		 y = bId_reviewSimilarity.get(businessId); 
        	 }
        	 else {
        		 y = 0.0;
        	 }
        	 Point point = new Point(x, y);
        	 point.addBusinessId(businessId);
        	 points.add(point);
         }
         return points;
    }
	private static List<Point> clusterPoints(List<Point> points, int numClusters, int MAX_ITER) {
        Random rand = new Random();
        // Create clusters
        List<Point> clusters = new ArrayList<>();
        for (int i = 0; i < numClusters; i++) {
            double x = rand.nextDouble();
            double y = rand.nextDouble();
            clusters.add(new Point(x, y));
        }
        // Cluster the points
        int iter = 0;
        while (true) {
            List<List<Point>> clustersList = new ArrayList<>(numClusters);
            for (int i = 0; i < numClusters; i++) {
                clustersList.add(new ArrayList<>());
            }
            for (Point point : points) {
                int nearestCluster = findNearestCluster(point, clusters);
                clustersList.get(nearestCluster).add(point);
            }
            List<Point> newClusters = new ArrayList<>();
            for (List<Point> cluster : clustersList) {
                if (cluster.isEmpty()) {
                    newClusters.add(clusters.get(rand.nextInt(numClusters)));
                } else {
                    newClusters.add(calculateMean(cluster));
                }
            }
            iter++;
            if (newClusters.equals(clusters) || iter > MAX_ITER) {
                break;
            } else {
                clusters = newClusters;
            }
        }
        return clusters;
    }
    private static int findNearestCluster(Point point, List<Point> clusters) {
        double minDist = Double.MAX_VALUE;
        int nearestCluster = -1;
        for (int i = 0; i < clusters.size(); i++) {
            double dist = point.distance(clusters.get(i));
            if (dist < minDist) {
                minDist = dist;
                nearestCluster = i;
            }
        }
        point.addToCluster(nearestCluster);
        return nearestCluster;
    }
    private static Point calculateMean(List<Point> points) {
        double sumX = 0.0;
        double sumY = 0.0;
        for (Point point : points) {
            sumX += point.x;
            sumY += point.y;
        }
        double meanX = sumX / points.size();
        double meanY = sumY / points.size();
        return new Point(meanX, meanY);
    }
}