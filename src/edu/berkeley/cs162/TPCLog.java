/**
 * Log for Two-Phase Commit
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 *
 * Copyright (c) 2012, University of California at Berkeley
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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class TPCLog<K extends Serializable, V extends Serializable> {

	private String logPath = null;
	private KeyServer<K, V> keyServer = null;

	// Log entries
	private ArrayList<KVMessage> entries = null;
	
	// Keeps track of the interrupted 2PC operation (There can be at most one, 
	// i.e., when the last 2PC operation before crashing was in READY state)
	private KVMessage interruptedTpcOperation = null;
	
	public TPCLog(String logPath, KeyServer<K, V> keyServer) {
		this.logPath = logPath;
		this.keyServer = keyServer;
		entries = null;
	}

	public ArrayList<KVMessage> getEntries() {
		return entries;
	}

	public boolean empty() {
		return (entries.size() == 0);
	}
	
	public void appendAndFlush(KVMessage entry) {
	    entries.add(entry);
	    flushToDisk();
	}

	/**
	 * Load log from persistent storage
	 */
	@SuppressWarnings("unchecked")
	public void loadFromDisk() {
		ObjectInputStream inputStream = null;
		
		try {
			inputStream = new ObjectInputStream(new FileInputStream(logPath));			
			entries = (ArrayList<KVMessage>) inputStream.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// If log never existed, there are no entries
			if (entries == null) {
				entries = new ArrayList<KVMessage>();
			}

			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Writes log to persistent storage
	 */
	public void flushToDisk() {
		ObjectOutputStream outputStream = null;
		
		try {
			outputStream = new ObjectOutputStream(new FileOutputStream(logPath));
			outputStream.writeObject(entries);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (outputStream != null) {
					outputStream.flush();
					outputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Load log and rebuild by iterating over log entries 
	 * @throws KVException
	 */
	public void rebuildKeyServer() throws KVException {
	    loadFromDisk();
	    for (int i = 0; i < entries.size() ; i++) {
		KVMessage entry = entries.get(i);
		K key = (K) KVMessage.decodeObject(entry.getKey());
		V value = (V) KVMessage.decodeObject(entry.getValue());
		if (entry.getMsgType() == "putreq") 
		    keyServer.put(key, value);
		else if (entry.getMsgType() == "delreq")
		    keyServer.del(key);
	    }
	}
	
	/**
	 * 
	 * @return Interrupted 2PC operation, if any 
	 */
	public KVMessage getInterruptedTpcOperation() { 
		KVMessage logEntry = interruptedTpcOperation; 
		interruptedTpcOperation = null; 
		return logEntry; 
	}
	
	/**
	 * 
	 * @return True if TPCLog contains an interrupted 2PC operation
	 */
	public boolean hasInterruptedTpcOperation() {
		return interruptedTpcOperation != null;
	}
}
