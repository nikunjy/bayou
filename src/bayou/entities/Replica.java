package bayou.entities;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import bayou.types.PlayList;
import bayou.types.PlayListOperation;


public class Replica extends Process {
	List<ProcessId> replicas;
	List<ProcessId> connectedReplicas;
	ProcessId primary;
	PlayList playList;
	PlayList commitedPlayList;
	List<PlayListOperation> ops;
	List<PlayListOperation> commitedOps;
	Map<String, PlayListOperation> uniqueCopies;
	PrintWriter writer;
	int entropyThreshold = 1;
	boolean isPrimary;
	long commitSeq;
	Map<ProcessId, Long> versionVector;
	Long timeStamp;
	public Replica(Env env, ProcessId me, ProcessId primary) {
		this.env = env;
		this.me = me;
		this.primary = primary;
		//this.connectedReplicas = neigh;
		replicas = new ArrayList<ProcessId>();
		ops = new ArrayList<PlayListOperation>();
		commitedOps = new ArrayList<PlayListOperation>();
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
		commitedPlayList = new PlayList();
		isPrimary = false;
		commitSeq = -1;
		if (me.equals(primary)) { 
			isPrimary = true;
			this.commitSeq = 0;
		}
		versionVector = new HashMap<ProcessId,Long>();
		uniqueCopies = new HashMap<String, PlayListOperation>();
		this.timeStamp = (long)0;
		env.addProc(me, this);
	}
	public void setConnectedList(List<ProcessId> neigh) {
		this.connectedReplicas = neigh;
	}
	public void setReplicas(List<ProcessId> replicas) {
		this.replicas = replicas;
	}
	public void removeFromConnectedList(ProcessId p) {
		this.connectedReplicas.remove(p);
	}
	public void addToConnectedList(ProcessId p) {
		this.connectedReplicas.add(p);
	}
	public void setTimeStamp(long t) {
		timeStamp = (long)t;
	}
	@Override
	void body() {
		
		if (isPrimary) {
			writer.println("I am primary");
			for (;;) {
				BayouMessage msg = getNextMessage();
				if (msg instanceof CommitRequestMessage) { 
					CommitRequestMessage req = (CommitRequestMessage)msg;
					writer.println("Received commit request from "+req.src+" with commit number "+req.senderCommitNumber);
					req.op.commitNumber = commitSeq; 
					ops.add(new PlayListOperation(req.op));
					ProcessId receiver = msg.src; 
					writer.println("Primary replica assigning commit number to "+ req.op.serialize()+" assigned "+commitSeq +" received from "+ receiver);
					commitSeq++;
					for (int i = 0; i < commitSeq ; i ++) {
						CommitResponseMessage commitedMessage = new CommitResponseMessage(new PlayListOperation(ops.get(i)));
						commitedMessage.commitNumber = ops.get(i).commitNumber;
						commitedMessage.src = this.me; 
						commitedMessage.dest = receiver;
						writer.println("Sending commited message "+ ops.get(i).serialize()+" to  "+ commitedMessage.dest);
						sendMessage(receiver, commitedMessage);
					}
					writer.flush();
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
					operation.execServer = this.me;
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
						for (ProcessId replica : connectedReplicas) {
							if (this.me.equals(replica)) 
								continue;
							AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
									this.commitSeq);
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
					if (!uniqueCopies.containsKey(message.op.id)) {
						ops.add(message.op);
						
						writer.println("Receiving entropy response message from "+message.src +" "+message.op.serialize());
						writer.flush();
						this.commitSeq = (this.commitSeq < message.op.commitNumber) ? message.op.commitNumber : this.commitSeq;
						/*Perform the op*/
						if(message.op.isCreateOp()) {
							//Will wait for it to get committed and then perform it//
							/*ProcessId ret = new ProcessId(message.op.execStamp.toString()+":"+message.op.execServer);
							System.out.println(this.me+": Got to know about creation of: "+ret.toString());
							replicas.add(ret);*/
						}
						else if (message.op.isRetireOp()) {
							//Will wait for it to get committed and then perform it...
							/*ProcessId ret = message.op.execServer;
							System.out.println(this.me+": Got to know about retirement of: "+ret.toString());
							replicas.remove(ret);
							if(connectedReplicas.contains(ret))
								connectedReplicas.remove(ret);*/
						}
						else if(message.op.isWriteOp()) {
							if(message.op.commitNumber == -1) //should always be true..
								message.op.operate(playList);
						}
						uniqueCopies.put(message.op.id, message.op);
					}
					versionVector.put(message.op.execServer, message.op.execStamp);
				} else if ( msg instanceof CommitResponseMessage) { 
					CommitResponseMessage commitResponse = (CommitResponseMessage)msg;
					if(this.commitSeq > commitResponse.commitNumber)
						continue;
					writer.println("Receiving commited message from "+commitResponse.src +" "+ commitResponse.commitNumber+" "+commitResponse.op.serialize());
					this.commitSeq = (this.commitSeq < commitResponse.commitNumber) ? commitResponse.commitNumber : this.commitSeq;
					writer.println(this.me+" setting commit sequence number "+commitSeq);					
					Iterator<PlayListOperation> it = ops.iterator();
					PlayListOperation commitedOperation = commitResponse.op;
					while (it.hasNext()) { 
						PlayListOperation op = it.next(); 
						if (op.id.equals(commitedOperation.id)) { 
							it.remove();
						}
					}
					boolean found = false; 
					for (PlayListOperation op : commitedOps) {
						if (op.id.equals(commitedOperation.id)) { 
							found = true; 
							break;
						}
					}
					if (!found) { 
						commitedOps.add(commitedOperation);
						if(commitedOperation.isCreateOp()) {
							ProcessId ret = new ProcessId(commitedOperation.execStamp.toString()+":"+commitedOperation.execServer);
							System.out.println(this.me+": Got to know about creation of: "+ret.toString());
							replicas.add(ret);
						}
						else if (commitedOperation.isRetireOp()) {
							ProcessId ret = commitedOperation.execServer;
							System.out.println(this.me+": Got to know about retirement of: "+ret.toString());
							replicas.remove(ret);
							if(connectedReplicas.contains(ret))
								connectedReplicas.remove(ret);
						}
						else if(commitedOperation.isWriteOp()) {
							
							commitedOperation.operate(commitedPlayList);
						}
						
						writer.println("Executing commited operation"+commitedOperation.serialize());
					}
					uniqueCopies.put(commitedOperation.id, commitedOperation);
					writer.flush();
				} else if (msg instanceof UserEntropyInitMessage) { 
					UserEntropyInitMessage initRequest = (UserEntropyInitMessage)msg;
					ProcessId replica = initRequest.receiver;
					AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
							this.commitSeq);
					
				} else if (msg instanceof PrintLogRequestMessage) { 
					PrintLogRequestMessage logRequest = (PrintLogRequestMessage)msg;
					if (logRequest.showCommitSeq) {
						writer.println("###########################");
						writer.println("Commit Seq " + this.commitSeq);
						writer.println("###########################");
					}
					if (logRequest.showCommitedOps) {
						writer.println("Showing commited ops");
						writer.println("###########################");
						for (PlayListOperation op : commitedOps){ 
							writer.println(op.serialize());
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
					writer.println("PlayList");
					writer.println("###########################");
					System.out.println(playList.toString());
					writer.println("Commited PlayList");
					writer.println("###########################");
					System.out.println(commitedPlayList.toString());
					writer.flush();
				} else if(msg instanceof CreationMessage) {
					Integer indexOfReplica = ((CreationMessage) msg).indexOfReplica;
					ProcessId ret = new ProcessId(timeStamp.toString()+":"+this.me);
					System.out.println(this.me + ": Received creation write for replica:"+indexOfReplica+"with assigned ID:"+ret.toString());
					replicas.add(ret);
					connectedReplicas.add(ret);
					env.replicas.add(ret);
					env.addIdToProc(indexOfReplica, ret);
					Replica repl = new Replica(env, ret, primary);
					repl.setTimeStamp(timeStamp+1);
					repl.setReplicas(replicas);
					List<ProcessId> list = new ArrayList<ProcessId>();
					list.add(me);
					list.add(ret);
					repl.setConnectedList(list);
					//Need to append to write log
					PlayListOperation op = new PlayListOperation();
					op.op = PlayListOperation.OperationTypes.CREATE.value();
					op.execServer = this.me;
					op.execStamp = timeStamp;
					ops.add(op);
					uniqueCopies.put(op.id, op);
					//TODO: need to ensure this write is executed..
					//TODO: do we need to put in version vector??
					//versionVector.put(ret, timeStamp);
					System.out.println(this.me + " got to know about, replica:"+indexOfReplica+"with ID:"+ret.toString());
					timeStamp++;
				} else if(msg instanceof RetirementMessage) { 
					/*I am supposed to retire*/
					System.out.println(this.me+" will be retiring...");
					int neigh = ((RetirementMessage) msg).neigh;
					ProcessId nhbr = env.idToProc.get(neigh);
					System.out.println("Perform Anti-entropy with:"+nhbr.toString());
					PlayListOperation op = new PlayListOperation();
					op.op = PlayListOperation.OperationTypes.RETIRE.value();
					op.execServer = this.me;
					op.execStamp = timeStamp;
					ops.add(op);
					uniqueCopies.put(op.id, op);
					AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+nhbr),this.me, nhbr, ops, commitedOps,
							this.commitSeq);
					connectedReplicas.clear();
					timeStamp++;
				} else if(msg instanceof BreakConnectionMessage) {
					connectedReplicas.remove(env.idToProc.get(((BreakConnectionMessage) msg).getNeigh()));
				} else if(msg instanceof RecoverConnectionMessage) {
					connectedReplicas.add(env.idToProc.get(((RecoverConnectionMessage) msg).getNeigh()));
				}
			}
		}
	}
}
