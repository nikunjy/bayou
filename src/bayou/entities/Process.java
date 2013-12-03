package bayou.entities;

public abstract class Process extends Thread {
	ProcessId me;
	Queue<BayouMessage> inbox = new Queue<BayouMessage>();
	Env env;

	abstract void body();

	public void run(){
		body();
		env.removeProc(me);
	}
	BayouMessage getNextMessageWait() {
		return inbox.bdequeue();
	}
	BayouMessage getNextMessage(){
		return inbox.bdequeue(2000);
	}
	BayouMessage getPingMessage(long timeOut) { 
		return inbox.bdequeue(timeOut);
	}
	void sendMessage(ProcessId dst, BayouMessage msg){
		env.sendMessage(dst, msg);
	}

	void deliver(BayouMessage msg){
		inbox.enqueue(msg);
	}
}
