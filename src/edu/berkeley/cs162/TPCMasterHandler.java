/**
 * Handle TPC connections over a socket interface
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
import java.io.Serializable;
import java.net.Socket;
import java.util.Dictionary;
import java.util.Hashtable;


/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 *
 */
public class TPCMasterHandler<K extends Serializable, V extends Serializable> implements NetworkHandler {
	private KeyServer<K, V> keyserver = null;
	private ThreadPool threadpool = null;
	private TPCLog<K, V> tpcLog = null;
	
	//Saves state across connections
	private Dictionary<String, KVMessage> opIdToOperation = null; 
	
	private boolean ignoreNext = false;

	public TPCMasterHandler(KeyServer<K, V> keyserver) {
		this(keyserver, 1);
	}

	public TPCMasterHandler(KeyServer<K, V> keyserver, int connections) {
		this.keyserver = keyserver;
		threadpool = new ThreadPool(connections);
		opIdToOperation = new Hashtable<String, KVMessage>();
	}

	@Override
	public void handle(Socket client) throws IOException {
		// implement me
		System.out.println("handle called");
		ConnectionHandler newTask = new ConnectionHandler(client);
		System.out.println("Adding to threadpool");
		if(newTask.failed == false){
			try {
				threadpool.addToQueue(newTask);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private class ConnectionHandler implements Runnable {
		
		Socket s1;
		KVMessage message;  //Message sent by the master
		public boolean failed = false; /*If we fail to get the message from the master, 
		 								 we do not add it to the thread pool		*/
		
		public ConnectionHandler(Socket client) throws IOException{
			this.s1 = client;
			KVMessage response = new KVMessage("resp", null, null);; //If there's an error getting the message, send this back
			try {
				message = new KVMessage(s1.getInputStream());
			} catch (KVException e) {
				FilterOutputStream fos = new FilterOutputStream(s1.getOutputStream());
				fos.flush();
				response.setMessage(e.getMsg().getMessage());
				String xml = null;
				try {
					xml = response.toXML();
				} catch (KVException e1) {
					e1.printStackTrace();
				}
				byte[] xmlBytes = xml.getBytes();
				fos.write(xmlBytes);
				fos.flush();
				s1.close();
				failed = true;
			}
		}
		
		public void run(){
			System.out.println("Calling Run");
			FilterOutputStream fos = null;
			try {
				fos = new FilterOutputStream(s1.getOutputStream());
				fos.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			KVMessage response = null;
			String xml;
			
			
			//TODO: Need to send back a "Response" (READY/ABORT) message
			if(message.getMsgType().equals("getreq")) {
				V value = null;
				try {
					value = (V)KVMessage.decodeObject((String)keyserver.get((K)message.getKey()));
					response = new KVMessage("resp" , message.getKey(), value, null, "Success");
				} catch (KVException e) {
					response = new KVMessage("resp", null, 
							null, null, e.getMsg().getMessage());	
				} finally {
					try {
						xml = response.toXML();
						byte[] xmlBytes = xml.getBytes();
						fos.write(xmlBytes);
						fos.flush();
						s1.shutdownOutput();
						s1.close();
					} catch (IOException e){
						e.printStackTrace();
					} catch (KVException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}//End of GET
			
			//Is part of the "prepare" message from coordinator in the 2PC Diagram
			//TODO: Need to send back a "Response" (READY/ABORT) message
			if(message.getMsgType().equals("putreq")){
				opIdToOperation.put(message.getTpcOpId(), message);
				response = new KVMessage("Ready");
				tpcLog.appendAndFlush(response);
				response.setTpcOpId(message.getTpcOpId());
				try {
					xml = response.toXML();
					byte[] xmlBytes = xml.getBytes();
					fos.write(xmlBytes);
					fos.flush();
					s1.shutdownOutput();
					s1.close();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (KVException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}//End of PUT
			
			//Is part of the "Prepare" message from coordinator in 2PC diagram
			//TODO: Need to send back a "Response" (READY/ABORT) message
			if(message.getMsgType().equals("delreq")){
				opIdToOperation.put(message.getTpcOpId(), message);
				response = new KVMessage("Ready");
				tpcLog.appendAndFlush(response);
				response.setTpcOpId(message.getTpcOpId());
				try {
					xml = response.toXML();
					byte[] xmlBytes = xml.getBytes();
					fos.write(xmlBytes);
					fos.flush();
					s1.shutdownOutput();
					s1.close();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (KVException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}//End of DEL
			
			//Is part of the "Decision" message from coordinator in the 2PC diagram
			//Send an ACK back to the coordinator
			if(message.getMsgType().equals("commit")){
				//Perform the operation
			    tpcLog.appendAndFlush(message);
			    response = new KVMessage("ack");
			    response.setTpcOpId(message.getTpcOpId());
			    KVMessage commitOp = opIdToOperation.get(message.getTpcOpId());	
			    if(commitOp.getMsgType().equals("putreq")){
				try {
				    keyserver.put((K)commitOp.getKey(),(V)commitOp.getValue());	
				} catch (KVException e) {
				    response = new KVMessage("resp", null, 
							     null, null, e.getMsg().getMessage());		
				}
			    } else if(commitOp.getMsgType().equals("delreq")){
				try {
				    keyserver.del((K)commitOp.getKey());
				} catch (KVException e) {
				    response = new KVMessage("resp", null, 
							     null, null, e.getMsg().getMessage());		
				}
			    }
				
			    //Respond with ACK or KVException to the coordinator
			    try {
			    	xml = response.toXML();
					byte[] xmlBytes = xml.getBytes();
					fos.write(xmlBytes);
					fos.flush();
					s1.shutdownOutput();
					s1.close();
			    } catch (IOException e) {
				e.printStackTrace();
			    } catch (KVException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}//End Commit
			
			//Is part of the "Decision" message from coordinator in the 2PC diagram
			//TODO: Send an ACK back to the coordinator
			if(message.getMsgType().equals("abort")){
			    tpcLog.appendAndFlush(message);
			    //Respond with ACK to the coordinator
			    response = new KVMessage("ack");
			    response.setTpcOpId(message.getTpcOpId());
			    try {
			    	xml = response.toXML();
				    byte[] xmlBytes = xml.getBytes();
			    	fos.write(xmlBytes);
			    	fos.flush();
			    	s1.shutdownOutput();
			    	s1.close();
			    } catch (IOException e) {
			    	e.printStackTrace();
			    } catch (KVException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}//End ACK
			
		}
	}

	/**
	 * Set TPCLog after it has been rebuilt
	 * @param tpcLog
	 */
	public void setTPCLog(TPCLog<K, V> tpcLog) {
		this.tpcLog  = tpcLog;
	}

}
