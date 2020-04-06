package disk_store;


import java.util.ArrayList;
import java.util.List;

/**
 * An ordered index.  Duplicate search key values are allowed,
 * but not duplicate index table entries.  In DB terminology, a
 * search key is not a superkey.
 * 
 * A limitation of this class is that only single integer search
 * keys are supported.
 *
 */


public class OrdIndex implements DBIndex {
	// Solution 3!! 
	private class Entry {
		int key;	// search key 
		ArrayList<BlockCount> blocks;	//list of blockNo and counts
	}
	
	private class BlockCount {
		int blockNo; 
		int count; 
	}
	
	ArrayList<Entry> entries;
	int size = 0; 
	/**
	 * Create an new ordered index.
	 */
	public OrdIndex() {
		entries = new ArrayList<>(); 
	}
	
	@Override
	public List<Integer> lookup(int key) {
		// binary search of entries ArrayList
		// return list of block numbers (no duplicates). 
		// if key not found, return empty list
		ArrayList<Integer> blockNoList = new ArrayList<>(); 
		int left = 0; 
		int right = entries.size()-1; 
		while (left <= right) {
			int middle = (left + right) / 2; 
			if (key == entries.get(middle).key) { //array list of entries
				for (BlockCount blockCount: entries.get(middle).blocks) {
					blockNoList.add(blockCount.blockNo); 	
				}
				return blockNoList; // 
			} else if (entries.get(middle).key < key) {
				left = middle + 1; 
			} else {
				right = middle - 1;
			}
		}
		return blockNoList; // return an empty list 
	}
	
	public Integer lookupIndex(int key) {
		int left = 0; 
		int right = entries.size()-1; 
		while (left <= right) {
			int middle = (left + right) /2; 
			if (key == entries.get(middle).key) { //array list of entries
				return middle; 
			} else if (entries.get(middle).key < key) {
				left = middle + 1; 
			} else {
				right = middle - 1;
			}
		}
		
		return left; 
	}
	@Override
	public void insert(int key, int blockNum) {
		// if key exists
		// 		if blockNo exists, increment count
		// 		if blockNo DNE, create new BlockCount
		// if key DNE
		//		create new entry
		List<Integer> blockNoList = lookup(key);  // one integer - index of entry that matches key/where key should be inserted
		Integer index = lookupIndex(key); 
		if (blockNoList.size() != 0) {
			for (Entry entry: entries) {	//index into arraylist of key that it matches
				if (entry.key == key) {
					for (BlockCount blockCount: entry.blocks) {
						if (blockCount.blockNo == blockNum) {
							blockCount.count ++; 
							size ++; 
							return;
						}
					}
					// blockNo DNE, create new BlockCount;
					BlockCount newBlockCount = new BlockCount(); 
					newBlockCount.count = 1; 
					newBlockCount.blockNo = blockNum; 
					entry.blocks.add(newBlockCount); 
					size ++; 
					return; 
				}
			}
			
		} else {
			Entry newEntry = new Entry(); 
			newEntry.key = key; 
			newEntry.blocks = new ArrayList<>(); 
			BlockCount newBlockCount = new BlockCount(); 
			newBlockCount.count = 1; 
			newBlockCount.blockNo = blockNum; 
			newEntry.blocks.add(newBlockCount);
			entries.add(index, newEntry);
			// add to correct position in arraylist, which is sorted by entry.key
			size ++; 
		}
//		throw new UnsupportedOperationException();
		
		
	}

	@Override
	public void delete(int key, int blockNum) {
		// lookup key
		//		if key not found, should not occur. Ignore it.
		//		else: decrement count for blockNo.
		//			if count is now 0, remove the blockNo.
		//			if there are no block number for this key, remove the key entry.
		List<Integer> blockNoList = lookup(key); 
		if (blockNoList.size() == 0) {
			return; 
		} else {
			for (Entry entry: entries) {
				if (entry.key == key) {
					for (BlockCount blockCount: entry.blocks) {
						// decrement count for blockNo
						if (blockCount.blockNo == blockNum) {
							blockCount.count --; 
							size --; 
							// if count == 0, remove blockNo.	
							if (blockCount.count == 0) {
								entry.blocks.remove(blockCount); 
							}
							if (entry.blocks.size() == 0) {	//remove key if there are no blocks(aka records)
								entries.remove(entry); 
							}
							return; 
						}
					}
					// if there are no block number for this key, remove the key entry
					entries.remove(entry); 
					return;
				}
				
			}
		}
	}
	
	/**
	 * Return the number of entries in the index
	 * @return
	 */
	public int size() {
		// you may find it useful to implement this
		return size;
	}
	
	@Override
	public String toString() {
		String s = new String(); 
		for (Entry entry: entries) {
			s += "key: " + entry.key + "\n"; 
			s += "blocks: \n";  
			for (BlockCount blockCount: entry.blocks) {
				s += "blockNo: " + blockCount.blockNo + "\n"; 
				s += "count: " + blockCount.count + "\n"; 
				
			}
			s += "\n"; 
		}
			
		return s;
						
	}
}
