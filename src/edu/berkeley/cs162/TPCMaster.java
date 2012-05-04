/**
 * Master for Two-Phase Commits
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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.TreeSet;

import sun.misc.Lock;

public class TPCMaster<K extends Serializable, V extends Serializable>  {
	
	/**
	 * Implements NetworkHandler to handle registration requests from 
	 * SlaveServers.
	 * 
	 */
	
	ArrayList<SlaveInfo> slaves = new ArrayList<SlaveInfo>();
	
	private class TPCRegistrationHandler implements NetworkHandler {

		private ThreadPool threadpool = null;

		public TPCRegistrationHandler() {
			// Call the other constructor
			this(1);	
		}

		public TPCRegistrationHandler(int connections) {
			threadpool = new ThreadPool(connections);	
		}
		
		public class runReg implements Runnable{
			Socket regClient = null;
			
			public runReg(Socket client){
				this.regClient = client;
			}
			
			public void run(){
				KVMessage msg = null;
				
				try{
					msg = new KVMessage(regClient.getInputStream());
				} catch (KVException e1){
					
				} catch (IOException e2){
					
				}
				
				if (msg.getMsgType().equals("register")){
					SlaveInfo slave = null;
					try{
						slave = new SlaveInfo(msg.getMessage());
					} catch (KVException e){
						
					}
					
					slaves.add(slave);
					
					FilterOutputStream fos = null;

					//try-catch this
					try {
						fos = new FilterOutputStream(regClient.getOutputStream());
						fos.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					

					KVMessage regACK = new KVMessage("resp", "Successfully registered " + slave.slaveID+"@"+slave.getHostName()+":"+slave.port);
					String xml = regACK.toXML();
					System.out.println("REGISTRATION ACK: " + xml);
					
					//try-catch this
					byte [] xmlBytes = xml.getBytes();
					try {
						fos.write(xmlBytes);
						fos.flush();
						fos.close();
						regClient.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
			}
		}
		@Override
		public void handle(Socket client) throws IOException {
			// implement me
			
			Runnable r = new runReg(client);
			try{
				threadpool.addToQueue(r);
			}catch (InterruptedException e){
				
			}
		}
	}
	
	/**
	 *  Data structure to maintain information about SlaveServers
	 *
	 */
	private class SlaveInfo {
		// 64-bit globally unique ID of the SlaveServer
		private long slaveID = -1;
		// Name of the host this SlaveServer is running on
		private String hostName = null;
		// Port which SlaveServer is listening to
		private int port = -1;
		
		// Variables to be used to maintain connection with this SlaveServer
		private KVClient<K, V> kvClient = null;
		private Socket kvSocket = null;

		/**
		 * 
		 * @param slaveInfo as "SlaveServerID@HostName:Port"
		 * @throws KVException
		 */
		public SlaveInfo(String slaveInfo) throws KVException {
			// implement me
			slaveID = Long.valueOf(slaveInfo.substring(0, slaveInfo.indexOf('@')));
			hostName = slaveInfo.substring(slaveInfo.indexOf('@')+1, slaveInfo.indexOf(':'));
			port = Integer.valueOf(slaveInfo.substring(slaveInfo.indexOf(':')+1));
			
			//kvClient = new KVClient(hostName, port);
			
			//how to initialize kvSocket?
			//We don't need to, just use the setKvSocket method
		}
		
		public long getSlaveID() {
			return slaveID;
		}
		
		public String getHostName(){
			return hostName;
		}

		public KVClient<K, V> getKvClient() {
			return kvClient;
		}

		public Socket getKvSocket() {
			return kvSocket;
		}

		public void setKvSocket(Socket kvSocket) {
			this.kvSocket = kvSocket;
		}
	}
	
	// Timeout value used during 2PC operations
	private static final int TIMEOUT_MILLISECONDS = 5000;
	
	// Cache stored in the Master/Coordinator Server
	private KVCache<K, V> masterCache = new KVCache<K, V>(1000);
	
	// Registration server that uses TPCRegistrationHandler
	private SocketServer regServer = null;
	
	// ID of the next 2PC operation
	private Long tpcOpId = 0L;
	
	/**
	 * Creates TPCMaster using SlaveInfo provided as arguments and SlaveServers 
	 * actually register to let TPCMaster know their presence
	 * 
	 * @param listOfSlaves list of SlaveServers in "SlaveServerID@HostName:Port" format
	 * @throws Exception
	 */
	public TPCMaster(String[] listOfSlaves) throws Exception {
		// implement me
		for (int i =0; i<listOfSlaves.length; i++){
			slaves.add(new SlaveInfo(listOfSlaves[i]));
		}


		// Create registration server
		regServer = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 9090);
	}
	
	/**
	 * Calculates tpcOpId to be used for an operation. In this implementation
	 * it is a long variable that increases by one for each 2PC operation. 
	 * 
	 * @return 
	 */
	private String getNextTpcOpId() {
		tpcOpId++;
		return tpcOpId.toString();		
	}
	
	/**
	 * Start registration server in a separate thread
	 */
	public void run() {
		Socket s = null;
		try{
			s = regServer.server.accept();
		} catch(IOException e){

		}

		TPCRegistrationHandler regHandler = new TPCRegistrationHandler();

		try{
			regHandler.handle(s);
		} catch (IOException e){
		
		}
	}
	
	/**
	 * Converts Strings to 64-bit longs
	 * Borrowed from http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
	 * Adapted from String.hashCode()
	 * @param string String to hash to 64-bit
	 * @return
	 */
	private long hashTo64bit(String string) {
		// Take a large prime
		long h = 1125899906842597L; 
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31*h + string.charAt(i);
		}
		return h;
	}
	
	/**
	 * Compares two longs as if they were unsigned (Java doesn't have unsigned data types except for char)
	 * Borrowed from http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 * @param n1 First long
	 * @param n2 Second long
	 * @return is unsigned n1 less than unsigned n2
	 */
	private boolean isLessThanUnsigned(long n1, long n2) {
		return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
	}
	
	private boolean isLessThanEqualUnsigned(long n1, long n2) {
		return isLessThanUnsigned(n1, n2) || n1 == n2;
	}	

	/**
	 * Find first/primary replica location
	 * @param key
	 * @return
	 */
	private SlaveInfo findFirstReplica(K key) {
		// 64-bit hash of the key
		long hashedKey = hashTo64bit(key.toString());

		// implement me
		long min = Long.MAX_VALUE;

		SlaveInfo slave = slaves.get(0);

		for(int i = 0; i < slaves.size(); i++){
			if(hashedKey < slaves.get(i).getSlaveID()){
				if(slaves.get(i).getSlaveID() < min){
					min = slaves.get(i).getSlaveID();
					slave = slaves.get(i);
				}
			}
		}

		return slave;
	}
	
	/**
	 * Find the successor of firstReplica to put the second replica
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {
		// implement me
		int temp = 0;

		for(int i = 0; i < slaves.size(); i++){
			if(firstReplica.getSlaveID() == slaves.get(i).getSlaveID()){
				temp = i;
			}
		}

		if(temp != slaves.size()){
			return slaves.get(temp+1);
		}
		else{
			return slaves.get(0);
		}

	}
	
	/**
	 * Synchronized method to perform 2PC operations one after another
	 * 
	 * @param msg
	 * @param isPutReq
	 * @return True if the TPC operation has succeeded
	 * @throws KVException
	 */
	static Lock writeLock = new Lock();
	public synchronized boolean performTPCOperation(KVMessage msg, boolean isPutReq) throws KVException {
		// the following is pseudocode. write it up as you go along
		
		try {
			//aquires the writelock
			writeLock.lock();
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		//key = get msg’s key;
		String key = msg.getKey();
		
		//firstSocket = get the kvSocket of findFirstReplica(key);
		SlaveInfo slave = findFirstReplica((K)key);
		Socket firstSocket = slave.getKvSocket();
		
		//secondSocket = get the kvSocket of findSuccessor(findFirstReplica(key));
		SlaveInfo slave1 = findSuccessor(slave);
		Socket secondSocket = slave1.getKvSocket();
		
		//req = a new TPCMessage with the msgType of msg and applicable fields;
		//not finished yet(if u notice the allcaps message
		KVMessage req = new KVMessage("msg", "WTF IS THIS MESSAGE SUPPOSED TO BE");
		
		
		//THE FOLLOWING TWO LINES STILL NEED TO BE IMPLEMENTED FUUUUUUUUUUUUUUUUUUUUUU...
		//stream req to firstSocket;
		//stream req to secondSocket;
		
		//set timeout for firstSocket, secondSocket;
		try {
			firstSocket.setSoTimeout(5000);
			secondSocket.setSoTimeout(5000);
		} catch (SocketException e) {
			System.out.print("Got a Socket Exception line 259");
			System.out.print(e.getMessage());
			e.printStackTrace();
		}
		
		OutputStream firstMsg;
		
		try {
			firstMsg = firstSocket.getOutputStream();
		} catch (IOException e) {
			System.out.println("IOException line 335");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		/*

		try to receive TPCMessages from firstSocket and secondSocket:
		if either socket times out:
		stream abort message to both firstSocket and secondSocket;
		return false;
		if both sockets send ready messages:
		stream ready message to both firstSocket and secondSocket;

				get writeLock of KVCache;
		update KVCache with operation;
		get exclusive lock on AccessList;
		release writeLock;
		update AccessList;
		release lock on Accesslist;

		release writeLock;

		return true;*/
		return false;
	}

	/**
	 * Perform GET operation in the following manner:
	 * - Try to GET from first/primary replica
	 * - If primary succeeded, return Value
	 * - If primary failed, try to GET from the other replica
	 * - If secondary succeeded, return Value
	 * - If secondary failed, return KVExceptions from both replicas
	 * 
	 * @param msg Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */
	public V handleGet(KVMessage msg) throws KVException {
		// implement me
		return null;
	}
}
