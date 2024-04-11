/**
 * 
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class MutualExclusion {
		
	private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
	/** lock variables */
	private boolean CS_BUSY = false;						// indicate to be in critical section (accessing a shared resource) 
	private boolean WANTS_TO_ENTER_CS = false;				// indicate to want to enter CS
	private List<Message> queueack; 						// queue for acknowledged messages
	private List<Message> mutexqueue;						// queue for storing process that are denied permission. We really don't need this for quorum-protocol
	
	private LamportClock clock;								// lamport clock
	private Node node;
	
	public MutualExclusion(Node node) throws RemoteException {
		this.node = node;
		
		clock = new LamportClock();
		queueack = new ArrayList<Message>();
		mutexqueue = new ArrayList<Message>();
	}
	
	public synchronized void acquireLock() {
		CS_BUSY = true;
	}
	
	public void releaseLocks() {
		WANTS_TO_ENTER_CS = false;
		CS_BUSY = false;
	}

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {
		
		logger.info(node.nodename + " wants to access CS");
		// clear the queueack before requesting for votes
		queueack.clear();
		// clear the mutexqueue
		mutexqueue.clear();
		// increment clock
		clock.increment();
		// adjust the clock on the message, by calling the setClock on the message
		message.setClock(clock.getClock());
		// wants to access resource - set the appropriate lock variable
		WANTS_TO_ENTER_CS = true;
		
		// start MutualExclusion algorithm
		
			// first, call removeDuplicatePeersBeforeVoting. A peer can hold/contain 2 replicas of a file. This peer will appear twice
		List<Message> uniquePeers = removeDuplicatePeersBeforeVoting();
			// multicast the message to activenodes (hint: use multicastMessage)
		multicastMessage(message, uniquePeers);
			// check that all replicas have replied (permission) - areAllMessagesReturned(int numvoters)?
		if (areAllMessagesReturned(uniquePeers.size())) {
			// Acquire lock
			acquireLock();

			// Send updates to all replicas
			node.broadcastUpdatetoPeers(updates);

			// Clear the mutexqueue
			mutexqueue.clear();

			// Return permission
			return true;
		}
		
		return false;
	}
	
	// multicast message to other processes including self
	private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
		logger.info("Number of peers to vote = " + activenodes.size());

		// Iterate over the active nodes
		for (Message peer : activenodes) {
			try {
				// Obtain a stub for each node from the registry
				NodeInterface nodeStub = Util.getProcessStub(peer.getNodeName(), peer.getPort());

				// Call onMutexRequestReceived on the node's stub
				nodeStub.onMutexRequestReceived(message);
			} catch (Exception e) {
				logger.error("Error multicasting message to " + peer.getNodeName() + ": " + e.getMessage());
			}
		}
	}


	public void onMutexRequestReceived(Message message) throws RemoteException {
		
		// increment the local clock
		clock.increment();
		// if message is from self, acknowledge, and call onMutexAcknowledgementReceived()
			if(message.getNodeName().equals(node.nodename)){
				logger.info("Received mutex request from self. Acknowledging...");
				onMutexAcknowledgementReceived(message);
				return;
			}
		int caseid = -1;

		if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
			// Case 0: Receiver is not accessing shared resource and does not want to
			caseid = 0;
		} else if (CS_BUSY || WANTS_TO_ENTER_CS) {
			// Case 1: Receiver already has access to the resource
			caseid = 1;
		} else {
			// Case 2: Receiver wants to access resource but is yet to
			// Compare own message clock to received message's clock
			if (clock.getClock() < message.getClock() || (clock.getClock() == message.getClock() && node.nodename.compareTo(message.getNodeName()) < 0)) {
				caseid = 2;
			} else {
				// Queue the request if receiver's clock is ahead or equal but has a higher node name
				mutexqueue.add(message);
				logger.info("Added request from " + message.getNodeName() + " to mutex queue.");
				return;
			}
		}

		// Call decision algorithm
		doDecisionAlgorithm(message, mutexqueue, caseid);
	}

	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
		String procName = message.getNodeName();
		int port = message.getPort();

		switch (condition) {
			// Case 0: Receiver is not accessing shared resource and does not want to
			case 0: {
				// Get a stub for the sender from the registry
				NodeInterface senderStub = Util.getProcessStub(procName, port);

				// Acknowledge message
				logger.info("Acknowledging mutex request from " + procName);
				senderStub.onMutexAcknowledgementReceived(message);

				break;
			}
			// Case 1: Receiver already has access to the resource
			case 1: {
				// Queue this message
				queue.add(message);
				logger.info("Queued mutex request from " + procName);
				break;
			}
			// Case 2: Receiver wants to access resource but is yet to
			case 2: {
				// Check the clock of the sending process (in the received message)
				int senderClock = message.getClock();

				// Own clock of the receiver (in the node's message)
				int receiverClock = clock.getClock();

				// Compare clocks
				if (senderClock < receiverClock || (senderClock == receiverClock && procName.compareTo(node.nodename) < 0)) {
					// Acknowledge the message
					logger.info("Acknowledging mutex request from " + procName);
					NodeInterface senderStub = Util.getProcessStub(procName, port);
					senderStub.onMutexAcknowledgementReceived(message);
				} else {
					// Queue the message
					queue.add(message);
					logger.info("Queued mutex request from " + procName);
				}
				break;
			}
			default:
				break;
		}
	}


	public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
		// Add message to queueack
		queueack.add(message);
		logger.info("Acknowledgement received from " + message.getNodeName());
	}


	// multicast release locks message to other processes including self
	public void multicastReleaseLocks(Set<Message> activenodes) {
		logger.info("Releasing locks from = " + activenodes.size());

		// Iterate over the active nodes
		for (Message node : activenodes) {
			try {
				// Obtain a stub for each node from the registry
				NodeInterface nodeStub = Util.getProcessStub(node.getNodeName(), node.getPort());

				// Call releaseLocks on the node's stub
				nodeStub.releaseLocks();
				logger.info("Locks released for " + node.getNodeName());
			} catch (Exception e) {
				logger.error("Error releasing locks for " + node.getNodeName() + ": " + e.getMessage());
			}
		}
	}


	private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
		logger.info(node.getNodeName()+": size of queueack = "+queueack.size());

		// check if the size of the queueack is the same as the numvoters
		// clear the queueack
		// return true if yes and false if no
		if(queueack.size() == numvoters){
			queueack.clear();
			return true;
		}
		return false;
	}


	private List<Message> removeDuplicatePeersBeforeVoting() {
		
		List<Message> uniquepeer = new ArrayList<Message>();
		for(Message p : node.activenodesforfile) {
			boolean found = false;
			for(Message p1 : uniquepeer) {
				if(p.getNodeName().equals(p1.getNodeName())) {
					found = true;
					break;
				}
			}
			if(!found)
				uniquepeer.add(p);
		}		
		return uniquepeer;
	}
}
