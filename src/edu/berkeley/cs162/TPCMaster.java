/**
 * Master for Two-Phase Commits
 *
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 *
 * Copyright (c) 2012, University of California at Berkeley All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. * Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. * Neither the name of University of California,
 * Berkeley nor the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written
 * permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.TreeSet;

import sun.misc.Lock;

public class TPCMaster<K extends Serializable, V extends Serializable> {

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

		public class runReg implements Runnable {

			Socket regClient = null;

			public runReg(Socket client) {
				this.regClient = client;
			}

			public void run() {
				KVMessage msg = null;

				try {
					msg = new KVMessage(regClient.getInputStream());
				} catch (KVException e1) {
				} catch (IOException e2) {
				}

				if (msg.getMsgType().equals("register")) {
					SlaveInfo slave = null;
					try {
						slave = new SlaveInfo(msg.getMessage());
						Socket slaveSocket = new Socket(slave.getHostName(), slave.getPort());
						slave.setKvSocket(slaveSocket);
					} catch (KVException e) {
					} catch (IOException e) {
						e.printStackTrace();
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


					KVMessage regACK = new KVMessage("resp", "Successfully registered " + slave.slaveID + "@" + slave.getHostName() + ":" + slave.port);
					String xml = null;
					try {
						xml = regACK.toXML();
					} catch (KVException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					System.out.println("REGISTRATION ACK: " + xml);

					//try-catch this
					byte[] xmlBytes = xml.getBytes();
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
			try {
				threadpool.addToQueue(r);
			} catch (InterruptedException e) {
			}
		}
	}

	/**
	 * Data structure to maintain information about SlaveServers
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
			hostName = slaveInfo.substring(slaveInfo.indexOf('@') + 1, slaveInfo.indexOf(':'));
			port = Integer.valueOf(slaveInfo.substring(slaveInfo.indexOf(':') + 1));

			//kvClient = new KVClient(hostName, port);

			//how to initialize kvSocket?
			//We don't need to, just use the setKvSocket method
		}

		public long getSlaveID() {
			return slaveID;
		}

		public String getHostName() {
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

		public int getPort() {
			return this.port;
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
	 * Creates TPCMaster using SlaveInfo provided as arguments and
	 * SlaveServers actually register to let TPCMaster know their presence
	 *
	 * @param listOfSlaves list of SlaveServers in
	 * "SlaveServerID@HostName:Port" format
	 * @throws Exception
	 */
	public TPCMaster(String[] listOfSlaves) throws Exception {
		// implement me
		for (int i = 0; i < listOfSlaves.length; i++) {
			slaves.add(new SlaveInfo(listOfSlaves[i]));
		}


		// Create registration server
		regServer = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 9090);
	}

	/**
	 * Calculates tpcOpId to be used for an operation. In this
	 * implementation it is a long variable that increases by one for each
	 * 2PC operation.
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
		try {
			s = regServer.server.accept();
		} catch (IOException e) {
		}

		TPCRegistrationHandler regHandler = new TPCRegistrationHandler();

		try {
			regHandler.handle(s);
		} catch (IOException e) {
		}
	}

	/**
	 * Converts Strings to 64-bit longs Borrowed from
	 * http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
	 * Adapted from String.hashCode()
	 *
	 * @param string String to hash to 64-bit
	 * @return
	 */
	private long hashTo64bit(String string) {
		// Take a large prime
		long h = 1125899906842597L;
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31 * h + string.charAt(i);
		}
		return h;
	}

	/**
	 * Compares two longs as if they were unsigned (Java doesn't have
	 * unsigned data types except for char) Borrowed from
	 * http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 *
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
	 *
	 * @param key
	 * @return
	 */
	private SlaveInfo findFirstReplica(K key) {
		// 64-bit hash of the key
		long hashedKey = hashTo64bit(key.toString());

		// implement me
		long min = Long.MAX_VALUE;

		SlaveInfo slave = slaves.get(0);

		for (int i = 0; i < slaves.size(); i++) {
			if (hashedKey < slaves.get(i).getSlaveID()) {
				if (slaves.get(i).getSlaveID() < min) {
					min = slaves.get(i).getSlaveID();
					slave = slaves.get(i);
				}
			}
		}

		return slave;
	}

	/**
	 * Find the successor of firstReplica to put the second replica
	 *
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {
		// implement me
		int temp = 0;

		for (int i = 0; i < slaves.size(); i++) {
			if (firstReplica.getSlaveID() == slaves.get(i).getSlaveID()) {
				temp = i;
			}
		}

		if (temp != slaves.size()) {
			return slaves.get(temp + 1);
		} else {
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
	static Lock readLock = new Lock();
	static Lock KVCacheLock = new Lock();

	public synchronized boolean performTPCOperation(KVMessage msg, boolean isPutReq) throws KVException {
		// the following is pseudocode. write it up as you go along

		try {
			//acquires the writelock
			writeLock.lock();
			readLock.lock();
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		//key = get msg�s key;
		String key = msg.getKey();

		//firstSocket = get the kvSocket of findFirstReplica(key);
		SlaveInfo slave = findFirstReplica((K) key);
		//Socket firstSocket = slave.getKvSocket();

		//secondSocket = get the kvSocket of findSuccessor(findFirstReplica(key));
		SlaveInfo slave1 = findSuccessor(slave);
		//Socket secondSocket = slave1.getKvSocket();

		//THE FOLLOWING HUGE CHUNK OF CODE IS TO SEND A PREPARE MESSAGE TO SLAVE SERVERS

		//req = a new TPCMessage with the msgType of msg and applicable fields;
		KVMessage req = new KVMessage(msg.getMsgType(), msg.getKey(), msg.getValue());
		req.setTpcOpId(tpcOpId.toString());
		getNextTpcOpId();

		//xml is the xml version of request
		String xml = null;

		//create the sockets to establish connection
		Socket firstSocket = null;
		Socket secondSocket = null;
		try {
			firstSocket = new Socket(slave.getHostName(), slave.getPort());
			secondSocket = new Socket(slave1.getHostName(), slave1.getPort());
		} catch (UnknownHostException e1) {
			System.out.println("exception line 394");
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("exception line 397");
			e1.printStackTrace();
		}

		//create the filteroutputstreams
		FilterOutputStream fos = null;
		FilterOutputStream fos1 = null;
		try {
			fos = new FilterOutputStream(firstSocket.getOutputStream());
			fos1 = new FilterOutputStream(secondSocket.getOutputStream());
			fos.flush();
			fos1.flush();
		} catch (IOException e) {
			System.out.println("exception line 409");
			e.printStackTrace();
		}

		try {
			xml = req.toXML();
			//System.out.println("XML RESPONSE: " + xml);
		} catch (KVException e1) {
			System.out.println("exception line 419");
			System.out.println(e1.getMessage());
		}
		byte[] xmlBytes = xml.getBytes();
		try {
			fos.write(xmlBytes);
			fos.flush();
			fos1.write(xmlBytes);
			fos1.flush();
		} catch (IOException e) {
			System.out.println("IO Error line 427");
			System.out.println(e.getMessage());
		}

		//END OF THE PREPARE MESSAGE PHASE

		//START OF THE RECEIVING MESSAGE FROM SLAVES PHASE

		KVMessage decision = null;

		//set timeout for firstSocket, secondSocket;
		try {
			firstSocket.setSoTimeout(5000);
			secondSocket.setSoTimeout(5000);
		} catch (SocketException e) {
		}

		/*
		 * try to receive TPCMessages from firstSocket and secondSocket:
		 * if either socket times out: stream abort message to both
		 * firstSocket and secondSocket; return false; if both sockets
		 * send ready messages: stream ready message to both firstSocket
		 * and secondSocket;
		 */
		InputStream firstMsg = null;
		InputStream secondMsg = null;

		try {
			firstMsg = firstSocket.getInputStream();
			secondMsg = secondSocket.getInputStream();
		} catch (IOException e) {
			System.out.println("IOException line 335");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		KVMessage inMsg1, inMsg2;
		try {
			inMsg1 = new KVMessage(firstMsg);
			inMsg2 = new KVMessage(secondMsg);
		} catch (SocketTimeoutException e) {
			decision = new KVMessage("abort");
			KVMessage error = new KVMessage("resp", "Timeout Error: SlaveServer <slaveID> has timed out during the first phase of 2PC");
			try {
				firstSocket.close();
				secondSocket.close();
			} catch (IOException e1) {
				System.out.println("exception line 449");
				System.out.println(e1.getMessage());
				e1.printStackTrace();
			}
			try {
				firstSocket = new Socket(slave.getHostName(), slave.getPort());
				secondSocket = new Socket(slave1.getHostName(), slave1.getPort());
			} catch (UnknownHostException e1) {
				System.out.println("exception line 457");
				e1.printStackTrace();
			} catch (IOException e1) {
				System.out.println("exception line 460");
				e1.printStackTrace();
			}
			try {
				fos = new FilterOutputStream(firstSocket.getOutputStream());
				fos1 = new FilterOutputStream(secondSocket.getOutputStream());
				fos.flush();
				fos1.flush();
			} catch (IOException e1) {
				System.out.println("exception line 469");
				e1.printStackTrace();
			}

			try {
				xml = decision.toXML();
				//System.out.println("XML RESPONSE: " + xml);
			} catch (KVException e1) {
				System.out.println("exception line 477");
				System.out.println(e1.getMessage());
			}
			byte[] xmlBytes1 = xml.getBytes();
			try {
				fos.write(xmlBytes1);
				fos.flush();
				fos1.write(xmlBytes1);
				fos1.flush();
			} catch (IOException e1) {
				System.out.println("IO Error line 490");
				System.out.println(e1.getMessage());
			}

			boolean ackReceived = false;
			while (!ackReceived) {
				try {
					//Wait for an ACK
					firstSocket.setSoTimeout(5000);
					secondSocket.setSoTimeout(5000);
					//ACK received (hopefully)
					KVMessage respMessage = null;
					KVMessage respMessage1 = null;
					InputStream is = null, is1 = null;

					try {
						is = firstSocket.getInputStream();
						is1 = secondSocket.getInputStream();
					} catch (IOException e1) {
					}

					try {
						respMessage = new KVMessage(is);
						respMessage1 = new KVMessage(is1);
					} catch (SocketTimeoutException e1) {
						//ACK not received, try sending the message again
						try {
							fos = new FilterOutputStream(firstSocket.getOutputStream());
							fos1 = new FilterOutputStream(secondSocket.getOutputStream());
						} catch (IOException e3) {
							// TODO Auto-generated catch block
							e3.printStackTrace();
						}
						try {
							fos.flush();
							fos1.flush();
						} catch (IOException e3) {
							// TODO Auto-generated catch block
							e3.printStackTrace();
						}

						try {
							xml = decision.toXML();
							//System.out.println("XML RESPONSE: " + xml);
						} catch (KVException e2) {
							System.out.println("exception line 535");
							System.out.println(e2.getMessage());
						}
						xmlBytes1 = xml.getBytes();
						try {
							fos.write(xmlBytes1);
							fos.flush();
							fos1.write(xmlBytes1);
							fos1.flush();
						} catch (IOException e2) {
							System.out.println("IO Error line 545");
							System.out.println(e2.getMessage());
						}
					}
					if (respMessage.getMsgType().equals("ack") && respMessage1.getMsgType().equals("ack")) {
						ackReceived = true;
					}
				} catch (SocketException e1) {
				}
			}

			//SEND BACK THE ERROR KVMessage THIS IS NOT FINISHED

			//then unlock and return false
			writeLock.unlock();
			readLock.unlock();
			return false;
		}
		System.out.println("line 403 inMsg1.getMsgType() is " + inMsg1.getMsgType());
		System.out.println("line 404 inMsg2.getMsgType() is " + inMsg2.getMsgType());
		if (inMsg1.getMsgType() != "Ready" || inMsg2.getMsgType() != "Ready") {
			//send abort messages

			decision = new KVMessage("abort");
			try {
				firstSocket.close();
				secondSocket.close();
			} catch (IOException e1) {
				System.out.println("exception line 524");
				System.out.println(e1.getMessage());
				e1.printStackTrace();
			}
			try {
				firstSocket = new Socket(slave.getHostName(), slave.getPort());
				secondSocket = new Socket(slave1.getHostName(), slave1.getPort());
			} catch (UnknownHostException e1) {
				System.out.println("exception line 532");
				e1.printStackTrace();
			} catch (IOException e1) {
				System.out.println("exception line 535");
				e1.printStackTrace();
			}
			try {
				fos = new FilterOutputStream(firstSocket.getOutputStream());
				fos1 = new FilterOutputStream(secondSocket.getOutputStream());
				fos.flush();
				fos1.flush();
			} catch (IOException e1) {
				System.out.println("exception line 545");
				e1.printStackTrace();
			}

			try {
				xml = decision.toXML();
				//System.out.println("XML RESPONSE: " + xml);
			} catch (KVException e1) {
				System.out.println("exception line 477");
				System.out.println(e1.getMessage());
			}
			byte[] xmlBytes1 = xml.getBytes();
			try {
				fos.write(xmlBytes1);
				fos.flush();
			} catch (IOException e1) {
				System.out.println("IO Error line 487");
				System.out.println(e1.getMessage());
			}

			boolean ackReceived = false;
			while (!ackReceived) {
				try {
					//Wait for an ACK
					firstSocket.setSoTimeout(5000);
					secondSocket.setSoTimeout(5000);

				} catch (SocketException e1) {
				}
				//ACK received (hopefully)
				KVMessage respMessage = null;
				KVMessage respMessage1 = null;

				InputStream is = null, is1 = null;
				try {
					is = firstSocket.getInputStream();
					is1 = secondSocket.getInputStream();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				try {
					respMessage = new KVMessage(is);
					respMessage1 = new KVMessage(is1);
				} catch (SocketTimeoutException e1) {
					//ACK not received, try sending the message again
					try {
						fos = new FilterOutputStream(firstSocket.getOutputStream());
						fos1 = new FilterOutputStream(secondSocket.getOutputStream());
					} catch (IOException e3) {
						// TODO Auto-generated catch block
						e3.printStackTrace();
					}
					try {
						fos.flush();
						fos1.flush();
					} catch (IOException e3) {
						// TODO Auto-generated catch block
						e3.printStackTrace();
					}

					try {
						xml = decision.toXML();
						//System.out.println("XML RESPONSE: " + xml);
					} catch (KVException e2) {
						System.out.println("exception line 666");
						System.out.println(e2.getMessage());
					}
					xmlBytes1 = xml.getBytes();
					try {
						fos.write(xmlBytes1);
						fos.flush();
						fos1.write(xmlBytes1);
						fos1.flush();
					} catch (IOException e2) {
						System.out.println("IO Error line 676");
						System.out.println(e2.getMessage());
					}
				}
				if (respMessage.getMsgType().equals("ack") && respMessage1.getMsgType().equals("ack")) {
					ackReceived = true;
				}
			}

			//then return false
			readLock.unlock();
			writeLock.unlock();
			return false;
		} else {
			//send commit messages

			decision = new KVMessage("commit");
			try {
				firstSocket.close();
				secondSocket.close();
			} catch (IOException e1) {
				System.out.println("exception line 700");
				System.out.println(e1.getMessage());
				e1.printStackTrace();
			}
			try {
				firstSocket = new Socket(slave.getHostName(), slave.getPort());
				secondSocket = new Socket(slave1.getHostName(), slave1.getPort());
			} catch (UnknownHostException e1) {
				System.out.println("exception line 708");
				e1.printStackTrace();
			} catch (IOException e1) {
				System.out.println("exception line 711");
				e1.printStackTrace();
			}
			try {
				fos = new FilterOutputStream(firstSocket.getOutputStream());
				fos1 = new FilterOutputStream(secondSocket.getOutputStream());
				fos.flush();
				fos1.flush();
			} catch (IOException e1) {
				System.out.println("exception line 720");
				e1.printStackTrace();
			}

			try {
				xml = decision.toXML();
				//System.out.println("XML RESPONSE: " + xml);
			} catch (KVException e1) {
				System.out.println("exception line 728");
				System.out.println(e1.getMessage());
			}
			byte[] xmlBytes1 = xml.getBytes();
			try {
				fos.write(xmlBytes1);
				fos.flush();
			} catch (IOException e1) {
				System.out.println("IO Error line 736");
				System.out.println(e1.getMessage());
			}

		}

		boolean ackReceived = false;
		while (!ackReceived) {
			try {
				//Wait for an ACK
				firstSocket.setSoTimeout(5000);
				secondSocket.setSoTimeout(5000);

			} catch (SocketException e1) {
			}
			//ACK received (hopefully)
			KVMessage respMessage = null;
			KVMessage respMessage1 = null;
			InputStream is = null, is1 = null;
			try {
				is = firstSocket.getInputStream();
				is1 = secondSocket.getInputStream();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {
				respMessage = new KVMessage(is);
				respMessage1 = new KVMessage(is1);
			} catch (SocketTimeoutException e1) {
				//ACK not received, try sending the message again
				try {
					fos = new FilterOutputStream(firstSocket.getOutputStream());
					fos1 = new FilterOutputStream(secondSocket.getOutputStream());
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
				try {
					fos.flush();
					fos1.flush();
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}

				try {
					xml = decision.toXML();
					//System.out.println("XML RESPONSE: " + xml);
				} catch (KVException e2) {
					System.out.println("exception line 782");
					System.out.println(e2.getMessage());
				}
				byte[] xmlBytes1 = xml.getBytes();
				try {
					fos.write(xmlBytes1);
					fos.flush();
					fos1.write(xmlBytes1);
					fos1.flush();
				} catch (IOException e2) {
					System.out.println("IO Error line 792");
					System.out.println(e2.getMessage());
				}
			}
			if (respMessage.getMsgType().equals("ack") && respMessage1.getMsgType().equals("ack")) {
				ackReceived = true;
			}
		}

		//get writeLock of KVCache(this is the access list)
		try {
			KVCacheLock.lock();
		} catch (InterruptedException e) {
			System.out.println("excetpion line 802");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		//update KVCache with operation;
		if (isPutReq) {
			masterCache.put((K) (msg.getKey()), (V) (msg.getValue()));
		} else {
			masterCache.del((K) (msg.getKey()));
		}

		//I HAVEN'T DONE THE FOLLOWING COMMENT
		//get exclusive lock on AccessList

		//release writeLock
		KVCacheLock.unlock();
		writeLock.unlock();
		/*
		 * update AccessList; release lock on Accesslist;
		 *
		 * release writeLock;
		 *
		 * return true;
		 */
		return true;
	}

	/**
	 * Perform GET operation in the following manner: - Try to GET from
	 * first/primary replica - If primary succeeded, return Value - If
	 * primary failed, try to GET from the other replica - If secondary
	 * succeeded, return Value - If secondary failed, return KVExceptions
	 * from both replicas
	 *
	 * @param msg Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */
	public V handleGet(KVMessage msg) throws KVException {
		// implement me

		try {
			readLock.lock();
		} catch (InterruptedException e) {
			System.out.println("exception line 847");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		V value = masterCache.get((K) msg.getKey());
		if (value != null) {
			return value;
		}

		SlaveInfo first = findFirstReplica((K) msg.getKey());
		V firstValue = first.getKvClient().get((K) msg.getKey());

		//if (firstValue success)
		if (firstValue != null) {
			readLock.unlock();
			return firstValue;
		}

		SlaveInfo secondary = findSuccessor(first);
		V secondaryValue = secondary.getKvClient().get((K) msg.getKey());

		//if (secondaryValue success)
		if (secondaryValue != null) {
			readLock.unlock();
			return secondaryValue;
		}

		throw new KVException(new KVMessage("resp", null, null, false, "Get operation failed for both replicas line 875"));
	}
}
