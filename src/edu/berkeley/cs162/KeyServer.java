 /**
 * Slave Server component of a KeyValue store
 * 
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 *
 * Copyright (c) 2011, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.Serializable;
import java.util.*;

/**
 * This class defines the slave key value servers. Each individual KeyServer 
 * would be a fully functioning Key-Value server. For Project 3, you would 
 * implement this class. For Project 4, you will have a Master Key-Value server 
 * and multiple of these slave Key-Value servers, each of them catering to a 
 * different part of the key namespace.
 *
 * @param <K> Java Generic Type for the Key
 * @param <V> Java Generic Type for the Value
 */
public class KeyServer<K extends Serializable, V extends Serializable> implements KeyValueInterface<K, V> {
	private KVStore<K, V> dataStore = null;
	private KVCache<K, V> dataCache = null;
	
	/**
	 * @param cacheSize number of entries in the data Cache.
	 */
	public KeyServer(int cacheSize) {
	    dataCache = new KVCache(cacheSize);
	    dataStore = new KVStore();
	}
	
	public boolean put(K key, V value) throws KVException {
	    String keyString = KVMessage.marshal(key);
	    String valueString = KVMessage.marshal(value);
		
		//System.out.println("Keyserver key: " + key);
		//System.out.println("Keyserver value: " + value);
		
	    byte[] size = (keyString).getBytes();
	    if (size.length > 256)
		throw new KVException(new KVMessage("resp", keyString, valueString, false, "Over sized key"));
	    if (size.length == 0)
	    	throw new KVException(new KVMessage("resp", keyString, valueString, false, "Empty Key"));
	    size = (valueString).getBytes();
	    if (size.length > 131072)
	    	throw new KVException(new KVMessage("resp", keyString, valueString, false, "Over sized value"));
	    if (size.length == 0)
	    	throw new KVException(new KVMessage("resp", keyString, valueString, false, "Empty Value"));

		boolean store = false;
		boolean cache = false;
		
		try{
		    synchronized (dataStore) {
			store = dataStore.put(key,value);
		    }
		}catch (KVException e) {
			throw new KVException(new KVMessage("resp", null, null, null, "IO Error"));
		}
		cache = dataCache.put(key,value);
		
	    if (store == true)
		return true;
	    if (store == false && cache == false)
		return false;
	    throw new KVException(new KVMessage("resp", keyString, valueString, false, "Unknown error: cache and store not in sync"));
	}
	
	public V get (K key) throws KVException {
		// implement me
		String keyString = KVMessage.marshal(key);
		byte[] size = keyString.getBytes();
		if(size.length > 256)
		throw new KVException(new KVMessage("resp", keyString, null, false, "Over sized key"));
	    if (size.length == 0)
		throw new KVException(new KVMessage("resp", keyString, null, false, "Empty key"));
		
		if(dataCache.get(key) != null){
			return dataCache.get(key);
		}
		try{
		    if (dataStore.get(key) != null) {
			return dataStore.get(key);
		    }
		} catch (KVException e) {
			throw new KVException(new KVMessage("resp", null, null, null, "IO Error"));
		}
		throw new KVException(new KVMessage("resp", null, null, null, "Does Not Exist"));
	}

	@Override
	public void del(K key) throws KVException {
		String keyString = KVMessage.marshal(key);
		
		byte[] size = keyString.getBytes();
		if(size.length > 256)
		throw new KVException(new KVMessage("resp", keyString, null, false, "Over sized key"));
	    if (size.length == 0)
		throw new KVException(new KVMessage("resp", keyString, null, false, "Empty key"));

	    try{
		if(dataCache.get(key) == null && dataStore.get(key) == null) {
		    throw new KVException(new KVMessage("resp", keyString, null, false, "Does not exist"));			
		}
	    }
	    catch (KVException e) {
		throw new KVException(new KVMessage("resp", null, null, null, "IO Error"));
	    }	

	    dataCache.del(key);
	    try{
		synchronized (dataStore) {
		    dataStore.del(key);
		}
	    } catch (KVException e) {
		throw new KVException(new KVMessage("resp", null, null, null, "IO Error"));
	    }
	}
}


