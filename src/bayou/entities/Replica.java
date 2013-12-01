package bayou.entities;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import bayou.types.PlayList;
import bayou.types.PlayListOperation;


public class Replica extends Process {
	List<ProcessId> replicas;
	ProcessId primary;
	PlayList playList;
	List<PlayListOperation> ops;
	Map<String, PlayListOperation> uniqueCopies;
	PrintWriter writer;
	int entropyThreshold = 2;
	boolean isPrimary;
	long commitSeq;
	Map<ProcessId, Long> versionVector;
	public Replica(Env env, ProcessId me, ProcessId primary) {
		this.env = env;
		this.me = me;
		this.primary = primary;
		replicas = new ArrayList<ProcessId>();
		ops = new ArrayList<PlayListOperation>();
		try {
			String name = "";
			String [] names = this.me.toString().split(":");
			for (int i = 0; i < names.length; i++) { 
				name += names[i];
			}
			writer = new PrintWriter(name+".txt", "UTF-8");
		} catch (Exception e) { 
			System.out.println(e);
		}
		playList = new PlayList();
		isPrimary = false;
		if (me.equals(primary)) { 
			isPrimary = true;
		}
		versionVector = new HashMap<ProcessId,Long>();
		uniqueCopies = new HashMap<String, PlayListOperation>();
		commitSeq = 0;
		env.addProc(me, this);
	}
	public void setReplicas(List<ProcessId> replicas) {
		this.replicas = replicas;
	}
	@Override
	void body() {
		long timeStamp = 0; 
		if (isPrimary) {
			writer.println("I am primary");
			
			
			for (;;) {
				BayouMessage msg = getNextMessage();
				if (msg instanceof CommitRequestMessage) { 
					CommitRequestMessage req = (CommitRequestMessage)msg;
					writer.println("Received commit request from "+req.src+" with commit number "+req.senderCommitNumber);
					req.op.commitNumber = commitSeq;
					//Saving the copy of an operation here. 
					ops.add(new PlayListOperation(req.op));
					ProcessId receiver = msg.src; 
					CommitResponseMessage response = new CommitResponseMessage(req.op);
					response.src = this.me; 
					response.dest = receiver; 
					response.commitNumber = commitSeq; 
					sendMessage(receiver, response);
					writer.println("Primary replica assigning commit number to "+ req.op.serialize()+" assigned "+commitSeq +" received from "+ response.dest);
					writer.flush();
					commitSeq++;
					for (int i = 0; 
							i < commitSeq ; i ++) {
						EntropyResponseMessage entropyResponse = new EntropyResponseMessage();
						entropyResponse.setOp(ops.get(i));
						entropyResponse.src = this.me; 
						entropyResponse.dest = receiver;
						writer.println("Sending entropy message "+ ops.get(i).serialize()+" to  "+ entropyResponse.dest);
						sendMessage(receiver, entropyResponse);
					}
					
				}
			}
		} else {
			for (;;) {
				BayouMessage msg = getNextMessage();
				if (msg instanceof RequestMessage) {
					RequestMessage message = (RequestMessage)msg;
					String cmd = message.op;
					PlayListOperation operation = PlayListOperation.getOperation(cmd);
					operation.id = this.me+":"+operation.id;
					operation.execStamp = timeStamp; 
					ops.add(operation);
					uniqueCopies.put(operation.id, operation);
					operation.operate(playList);
					if (ops.size() % entropyThreshold == 0) {
						for (PlayListOperation tempOp : ops) { 
							if (tempOp.commitNumber != -1) 
								continue;
							CommitRequestMessage commitReq = new CommitRequestMessage(new PlayListOperation(tempOp)); 
							commitReq.src = this.me; 
							commitReq.dest = primary; 
							commitReq.senderCommitNumber = this.commitSeq;
							sendMessage(primary, commitReq);
						}
						for (ProcessId replica : replicas) {
							if (this.me.equals(replica)) 
								continue;
							List<PlayListOperation> tempOps = new ArrayList<PlayListOperation>(ops);
							AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, tempOps, this.commitSeq);
						}
					}
					System.out.println(this.me +" executed "+ operation.serialize());
					timeStamp++;
				} else if (msg instanceof EntropyInitMessage) { 
					EntropyRequestMessage message = new EntropyRequestMessage(); 
					message.versionVector = versionVector;
					message.commitSeq = this.commitSeq;
					message.src = this.me; 
					message.dest = ((EntropyInitMessage)msg).src;
					sendMessage(message.dest, message);
				} else if ( msg instanceof EntropyResponseMessage) { 
					EntropyResponseMessage message = (EntropyResponseMessage)msg;
					if (!uniqueCopies.containsKey(message.op)) {
						ops.add(message.op);
						writer.println("Receiving entropy response message from "+message.src +" "+message.op.serialize());
						writer.flush();
						this.commitSeq = (this.commitSeq < message.op.commitNumber) ? message.op.commitNumber : this.commitSeq;
						uniqueCopies.put(message.op.id, message.op);
						System.out.println(this.me +" received "+ message.op.serialize());
					}
					versionVector.put(message.op.execServer, message.op.execStamp);
				} else if ( msg instanceof CommitResponseMessage) { 
					/*
					 * The operation that you passed was passed by reference and not by copy so you are good to go.
					 * Otherwise you would loop through the playlist operation and then set the commit sequence numbers
					 */
					CommitResponseMessage commitResponse = (CommitResponseMessage)msg;
					writer.println("Receiving commited message from "+commitResponse.src +" "+ commitResponse.commitNumber+" "+commitResponse.op.serialize());
					writer.flush();
					this.commitSeq = (this.commitSeq < commitResponse.commitNumber) ? commitResponse.commitNumber : this.commitSeq;
					if (uniqueCopies.containsKey(commitResponse.op.id)) { 
						uniqueCopies.get(commitResponse.op.id).commitNumber = commitResponse.op.commitNumber;
					} else { 
						PlayListOperation copy = new PlayListOperation(commitResponse.op);
						ops.add(copy);
						uniqueCopies.put(copy.id,copy);
					}
				} else if (msg instanceof PrintLogRequestMessage) { 
					PrintLogRequestMessage logRequest = (PrintLogRequestMessage)msg;
					System.out.println(this.me +" printing logs");
					if (logRequest.showCommitSeq) {
						writer.println("###########################");
						writer.println("Commit Seq " + this.commitSeq);
						writer.println("###########################");
					}
					if (logRequest.showCommitedOps) {
						writer.println("Showing commited ops");
						writer.println("###########################");
						for (PlayListOperation op : ops){ 
							if (op.commitNumber != -1) { 
								writer.println(op.serialize());
							}
						}
						writer.println("###########################");
					}
					if (logRequest.showTentativeWrites) { 
						writer.println("Showing Tentative writes");
						writer.println("###########################");
						for (PlayListOperation op : ops){ 
							if (op.commitNumber == -1) { 
								writer.println(op.serialize());
							}
						}
						writer.println("###########################");
					}
					writer.flush();
				}
			}
		}
	}
}
