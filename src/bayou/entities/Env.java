package bayou.entities;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bayou.types.PlayListOperation;

public class Env {
	Map<ProcessId, Process> procs = new HashMap<ProcessId, Process>();
	Map<Integer,ProcessId> idToProc = new HashMap<Integer, ProcessId>();
	List<ProcessId> replicas;
	Map<Integer, ProcessId> clientToProc = new HashMap<Integer, ProcessId>();
	ProcessId primary;
	public static int nReplicas = 2;
	private int numClients;
	class BlackList {
		String process1; 
		String process2;
		public BlackList(String p1, String p2)  {
			this.process1 = p1; 
			this.process2 = p2;
		}
	};
	public List<ProcessId> isolatedNodes = new ArrayList<ProcessId>();
	public List<BlackList> blackList = new ArrayList<BlackList>();
	public Env() { 
		try {
			BufferedReader br = new BufferedReader(new FileReader("BlackList.txt"));
			String line = "";
			while ((line = br.readLine()) != null) {
				String p[] = line.split(" ");
				System.out.println("Adding to blacklist "+p[0]+" "+p[1]);
				blackList.add(new BlackList(p[0],p[1]));
			}
		} catch(Exception e) { 
			
		}
	}
	boolean isBlackListMessage(ProcessId dst, BayouMessage msg) {
		if (msg.src == null || dst == null) { 
			return false;
		}
		for (ProcessId isolatedNode : isolatedNodes) { 
			if (dst.equals(isolatedNode) || dst.name.contains(isolatedNode.name)) {
				return true;
			}
		}
		for (BlackList item : blackList) { 
			if (dst.name.contains(item.process1) && msg.src.name.contains(item.process2)) { 
				return true;
			}
			if (dst.name.contains(item.process2) && msg.src.name.contains(item.process1)) { 
				return true;
			}
		}
		return false;
	}
	synchronized void sendMessage(ProcessId dst, BayouMessage msg){
		if (isBlackListMessage(dst,msg)) { 
			return;
		}
		Process p = procs.get(dst);
		if (p != null) {
			p.deliver(msg);
		}
	}
	synchronized void addProc(ProcessId pid, Process proc){
		procs.put(pid, proc);
		proc.start();
	}
	synchronized void removeProc(ProcessId pid){
		if (procs.containsKey(pid)) { 
			procs.remove(pid);
			return;
		}
		for (ProcessId id : procs.keySet()) {
			if (id.name.equalsIgnoreCase(pid.name)) {
				procs.remove(id);
				System.out.println("Killed "+id);
				for (ProcessId tid : procs.keySet()) {
					System.out.println(tid);
				}
				break;
			}
		}
	}
	synchronized void addIdToProc(int id, ProcessId p) {
		idToProc.put(id, p);
	}
	synchronized void removeIdFromProc(int id) {
		idToProc.remove(id);
	}
	public enum UserCommandTypes {
		PRINTLOG("printLog:"),INITENTROPY("initEntropy:"),PRINTALL("printAll:"),ISOLATE("isolate:"),
		RECONNECT("reconnect:"),PAUSE("pause:"),RESUME("resume:"),KILLPROCESS("killProcess:"),
		DELPARTITION("deletePartition:"),ADDBLACKLIST("add:"),COMMAND("command:"),JOIN("join:"),LEAVE("leave:"),
		BREAK("break:"),RECOVER("recover:"),ENTROPY("entropy"),CLIENT("client:");
		public String message;
		public String value() { 
			return message;
		}
		UserCommandTypes(String message) {
			this.message = message;
		}
	}
	class UserReader extends Process {
		public UserReader(Env env,ProcessId pid) { 
			this.env = env;
			this.me = pid;
			env.addProc(pid, this);	
		}
		public void body() { 
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				//Thread.sleep(7000);
				while(true) {
				System.out.println("Enter your Command : ");
				String input  = br.readLine();
				for (int i=0;i<UserCommandTypes.values().length;i++) { 
					UserCommandTypes type = UserCommandTypes.values()[i];
					if (input.contains(type.value())) {
						if (type.equals(UserCommandTypes.PRINTLOG)) { 
							String s = input.substring(input.indexOf(":")+1);
							String[] subs = s.split(",");
							int replicaId = Integer.parseInt(subs[0]);
							int printLogLevel = Integer.parseInt(subs[1]);
							PrintLogRequestMessage logMessage = new PrintLogRequestMessage(printLogLevel); 
							sendMessage(env.idToProc.get(replicaId), logMessage);
						} else if (type.equals(UserCommandTypes.PRINTALL)) { 
							int logLevel = Integer.parseInt(input.substring(input.indexOf(":")+1));
							for (ProcessId replica : replicas) {
								PrintLogRequestMessage logMessage = new PrintLogRequestMessage(logLevel); 
								sendMessage(replica, logMessage);
							}
						} else if (type.equals(UserCommandTypes.INITENTROPY)) {
							String s = input.substring(input.indexOf(":")+1);
							String[] subs = s.split(",");
							ProcessId sender = idToProc.get(Integer.parseInt(subs[0]));
							ProcessId receiver = idToProc.get(Integer.parseInt(subs[1]));
							UserEntropyInitMessage initRequest = new UserEntropyInitMessage(receiver);
							initRequest.dest = sender;
							sendMessage(sender, initRequest);
						} else if (type.equals(UserCommandTypes.ISOLATE)) {
							int replicaId = Integer.parseInt(input.substring(input.indexOf(":")+1));
							isolatedNodes.add(idToProc.get(replicaId));
						} else if (type.equals(UserCommandTypes.RECONNECT)) {
							int replicaId = Integer.parseInt(input.substring(input.indexOf(":")+1));
							isolatedNodes.remove(idToProc.get(replicaId));
						} else if (type.equals(UserCommandTypes.PAUSE)) {						
							String s = input.substring(input.indexOf(":")+1);
							if (s.isEmpty() || s.equals(" ")) {
								System.out.println("Pausing all processes..");
								for (ProcessId id : procs.keySet()) { 
									if (!id.equals(this.me)) {
										procs.get(id).suspend();
									}
								}
							} else { 
								ProcessId replica = idToProc.get(Integer.parseInt(s));
								procs.get(replica).wait();
							}
						} else if (type.equals(UserCommandTypes.RESUME)) { 
							String s = input.substring(input.indexOf(":")+1);
							if (s.isEmpty() || s.equals(" ")) {
								for (ProcessId id : procs.keySet()) { 
									if (!id.equals(this.me)) {
										procs.get(id).resume();
									}
								}
							} else { 
								ProcessId replica = idToProc.get(Integer.parseInt(s));
								procs.get(replica).notify();
							}
						} else if (type.equals(UserCommandTypes.KILLPROCESS)) {
							String id = input.substring(input.indexOf(":")+1);
							System.out.println(id);
							env.removeProc(new ProcessId(id));
						} else if (type.equals(UserCommandTypes.DELPARTITION))  {
							env.blackList.clear();
						} else if(type.equals(UserCommandTypes.ADDBLACKLIST)) {
							
							
						} else if (type.equals(UserCommandTypes.COMMAND)) { 
							
						} else if (type.equals(UserCommandTypes.JOIN)) {
							//join:i,j, such that 'i' will send creation write to 'j'
							String s = input.substring(input.indexOf(":")+1);
							String[] subs = s.split(",");
							Integer newReplica = Integer.parseInt(subs[0]);
							ProcessId receiver = idToProc.get(Integer.parseInt(subs[1]));
							CreationMessage msg = new CreationMessage();
							msg.setIndex(newReplica);
							sendMessage(receiver,msg);
							//receiver will decide ProcessID for this replica and add it to Env.replicas, idToProc and procs
							nReplicas++;
						} else if (type.equals(UserCommandTypes.LEAVE)) {
							//leave:i,j such that 'i' will perform last anti-entropy with 'j'
							String s = input.substring(input.indexOf(":")+1);
							String[] subs = s.split(",");
							System.out.println(subs[0]+" is retiring from the system");
							RetirementMessage msg = new RetirementMessage();
							ProcessId receiver = idToProc.get(Integer.parseInt(subs[0]));
							msg.setNeigh(Integer.parseInt(subs[1]));
							sendMessage(receiver,msg);
							replicas.remove(receiver);
							removeProc(receiver);
							removeIdFromProc(Integer.parseInt(subs[0]));
							nReplicas--;
						} else if (type.equals(UserCommandTypes.BREAK)) {
							String s = input.substring(input.indexOf(":")+1);
							String[] subs = s.split(",");
							int src = Integer.parseInt(subs[0]);
							int dst = Integer.parseInt(subs[1]);
							System.out.println("Breaking connection between: "+src+" and "+dst);
							BreakConnectionMessage msg = new BreakConnectionMessage();
							msg.setNeigh(dst);
							ProcessId receiver = idToProc.get(src);
							sendMessage(receiver, msg);
							BreakConnectionMessage msg1 = new BreakConnectionMessage();
							receiver = idToProc.get(dst);
							msg1.setNeigh(src);
							sendMessage(receiver,msg1);							
						} else if (type.equals(UserCommandTypes.RECOVER)) {
							String s = input.substring(input.indexOf(":")+1);
							String[] subs = s.split(",");
							int src = Integer.parseInt(subs[0]);
							int dst = Integer.parseInt(subs[1]);
							RecoverConnectionMessage msg = new RecoverConnectionMessage();
							msg.setNeigh(dst);
							ProcessId receiver = idToProc.get(src);
							sendMessage(receiver, msg);
							RecoverConnectionMessage msg1 = new RecoverConnectionMessage();
							receiver = idToProc.get(dst);
							msg1.setNeigh(src);
							sendMessage(receiver,msg1);
						} else if (type.equals(UserCommandTypes.CLIENT)) {
							String s = input.substring(input.indexOf(":")+1);
							String[] subs = s.split(";");
							int id = Integer.parseInt(subs[0]);
							if(clientToProc.containsKey(id)) {
								ProcessId clientId = clientToProc.get(id);
								ClientMessage msg = new ClientMessage();
								msg.setQuery(subs[1]);
								sendMessage(clientId,msg);
							}
						}
					}
				}
				}
			}catch(Exception e) { 
				System.out.println(e);
			}
		}
	}
	/*class Client extends Process {
		public ProcessId pid;
		public int clientId;
		public Client(Env env,ProcessId pid, int clientId) {
			this.env = env; 
			this.clientId = clientId;
			this.pid = pid;
			this.me = pid;
			env.addProc(pid, this);
		}
		public void body() { 
			try {
				BufferedReader br = new BufferedReader(new FileReader(clientId+".txt"));
				String line = "";
				while ((line = br.readLine()) != null) {
					System.out.println("Client "+clientId +" executing "+line);
					if (line.contains("delay:")) {
						line = line.substring(line.indexOf(":")+1);
						long sleepTime = Long.parseLong(line);  
						Thread.sleep(sleepTime);
					} else {
						for (int r = 0; r < nReplicas; r++) {
							sendMessage(env.replicas.get(r),new RequestMessage(this.me, env.replicas.get(r), line));
						}	
					}
				}
			} catch(Exception e) {
			}finally {
			}
		}
	};*/
	void run(String[] args){
		replicas = new ArrayList<ProcessId>();
		primary = new ProcessId("primary-replica");
		/*Providing 0 as id to primary*/
		idToProc.put(0, primary);
		replicas.add(primary); // primary is also a replica..
		
		for (int i = 1; i <= nReplicas; i++) {
			replicas.add(new ProcessId("replica:" + i));
			idToProc.put(i, replicas.get(i));
		}
		List<ProcessId> connectedList = new ArrayList<ProcessId>(replicas);
		//connectedList.add(primary);
		/*Initially, everyone is connected to everyone else*/
		Replica primaryReplica = new Replica(this, primary, primary);
		primaryReplica.setReplicas(replicas);
		primaryReplica.setConnectedList(connectedList);
		
		for (int i = 1; i <= nReplicas; i++) {
			Replica repl = new Replica(this, replicas.get(i), primary);
			repl.setReplicas(replicas);
			repl.setConnectedList(connectedList);
		}
		if (numClients == 1) {
			for (int i = 1; i < 3; i++) {
				ProcessId pid = new ProcessId("client:" + i);
				clientToProc.put(i, pid);
				PlayListOperation op = new PlayListOperation();
				op.op = PlayListOperation.OperationTypes.ADD.value();
				op.name = "Blah"+i;
				op.url = "http://blah";
				sendMessage(replicas.get(i%replicas.size()),
							new RequestMessage(pid, replicas.get(i%replicas.size()), op.serialize()));
				
			}
		} else { 
			for (int i = 1; i <= numClients ; i++) { 
				ProcessId pid = new ProcessId("client:" + i);
				clientToProc.put(i, pid);
				System.out.println("starting client"+i);
				Client c = new Client(this,pid,i,i); //Assigning client 'i' to replica 'i'
			}
		}
		UserReader ucmd = new UserReader(this,new ProcessId("usercmd:"));
	}

	public static void main(String[] args){
		Env obj = new Env();
		if (args.length>0) { 
			obj.numClients = Integer.parseInt(args[0]);
		} else { 
			obj.numClients = 2;
		}
		obj.run(args);
	}
}

