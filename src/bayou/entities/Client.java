package bayou.entities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bayou.entities.Env.UserCommandTypes;
import bayou.types.PlayList;
import bayou.types.PlayListOperation;

public class Client extends Process{
	public ProcessId pid;
	public int clientId;
	Integer assignedReplica;
	ProcessId replicaId;
	Map<String, PlayListOperation> writeSet;
	public Client(Env env,ProcessId pid, int clientId, int assignedReplica) {
		this.env = env; 
		this.clientId = clientId;
		this.pid = pid;
		this.me = pid;
		this.assignedReplica = assignedReplica;
		this.replicaId = env.idToProc.get(assignedReplica);
		writeSet = new HashMap<String,PlayListOperation>();
		env.addProc(pid, this);
	}
	public enum UserCommandTypes {
		QUERY("query"),ADD("add"),EDIT("edit"),DELETE("delete"),DISCONNECT("remove"),CONNECT("attach");
		public String message;
		public String value() { 
			return message;
		}
		UserCommandTypes(String message) {
			this.message = message;
		}
	}
	public void body() { 
		try {
			//BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			while(true) {
				BayouMessage msg = getNextMessageWait();
				if(msg instanceof ClientMessage) {
					ClientMessage message = (ClientMessage)msg;
					//System.out.println("Enter your Command for client"+clientId+": ");
					String input  = message.query;
					for (int i=0;i<UserCommandTypes.values().length;i++) { 
						UserCommandTypes type = UserCommandTypes.values()[i];
						if (input.contains(type.value())) {
							if (type.equals(UserCommandTypes.QUERY)) { 
								System.out.println(this.me+" received query message");
								String s = input.substring(input.indexOf(":")+1);
								PlayListOperation op = new PlayListOperation();
								op.op = PlayListOperation.OperationTypes.QUERY.value();
								op.name = s;
								op.url = "";
								/*Check if RYW guarantee is satisfied*/
								Replica repl = (Replica) env.procs.get(replicaId);
								boolean satisfied = true;
								for(String id : writeSet.keySet()) {
									if(!repl.uniqueCopies.containsKey(id)) 
										satisfied = false;
								}
								if(!satisfied) {
									System.out.println("Currently connected replica:"+assignedReplica.toString()+" cannot satisfy RYW");
									System.out.println("Switch me to a different replica...");
								}
								else {
									sendMessage(replicaId,
											new RequestMessage(pid, replicaId, op.serialize()));
									System.out.println("Result will be printed by assigned replica..");
								}

							} else if (type.equals(UserCommandTypes.ADD)) {
								System.out.println(this.me+" received add message");
								String s = input.substring(input.indexOf(":")+1);
								String[] subs = s.split(",");
								PlayListOperation op = new PlayListOperation();
								op.op = PlayListOperation.OperationTypes.ADD.value();
								op.name = subs[0];
								op.url = subs[1];
								String opId = replicaId+":"+op.id; //this is what replica will put as id of op
								writeSet.put(opId , op);
								sendMessage(replicaId,
										new RequestMessage(pid, replicaId, op.serialize()));
							} else if (type.equals(UserCommandTypes.EDIT)) {
								System.out.println(this.me+" received edit message");
								String s = input.substring(input.indexOf(":")+1);
								String[] subs = s.split(",");
								PlayListOperation op = new PlayListOperation();
								op.op = PlayListOperation.OperationTypes.EDIT.value();
								op.name = subs[0];
								op.url = subs[1];
								String opId = replicaId+":"+op.id; //this is what replica will put as id of op
								writeSet.put(opId , op);
								sendMessage(replicaId,
										new RequestMessage(pid, replicaId, op.serialize()));
							} else if (type.equals(UserCommandTypes.DELETE)) {
								System.out.println(this.me+" received delete message");
								String s = input.substring(input.indexOf(":")+1);
								//String[] subs = s.split(",");
								PlayListOperation op = new PlayListOperation();
								op.op = PlayListOperation.OperationTypes.DELETE.value();
								op.name = s;
								op.url ="";
								//op.url = subs[1];
								String opId = replicaId+":"+op.id; //this is what replica will put as id of op
								writeSet.put(opId , op);
								sendMessage(replicaId,
										new RequestMessage(pid, replicaId, op.serialize()));
							} else if (type.equals(UserCommandTypes.DISCONNECT)) {
								System.out.println(this.me+" received disconnect message");
								assignedReplica = 0;
								replicaId = env.idToProc.get(assignedReplica);

							} else if (type.equals(UserCommandTypes.CONNECT)) {
								System.out.println(this.me+" received connect message");
								String s = input.substring(input.indexOf(":")+1);
								assignedReplica = Integer.parseInt(s);
								replicaId = env.idToProc.get(assignedReplica);
							}
						}
					}
				}
			}


		} catch(Exception e) {
		}finally {
		}
	}

}
