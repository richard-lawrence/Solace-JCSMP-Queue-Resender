//
// JCSMP example to read messages from one queue (e.g. a Dead Message Queue) and re-send them to a different queue
//
// The tool uses a transacted session, if all messages cannot be read/sent or an error is encountered the transaction is rolled back.
//
// When resending messages from a DMQ, if the messages read were sent directly to a queue this original queue is checked that it matches the
// configured -toQ, if the queue names dont match the operation is rolled back (unless the -force option is specified).
// If the read messages were attracted to its original queue via a topic subscription it's not possible to check the original queue,
// so it's up to the user to ensure the -toQ parameter is correct.
//
// CAUTION use of this tool is at user's own risk. It is recommended to run the tool with the -nop option to check behaviour first.
//
// Note; logging maybe set in log4j.properties, with debug enabled the content of read messages is logged.
//

package com.solace;

import org.apache.log4j.Logger;

import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.transaction.*;


public class JcsmpQueueResender
{

    public void usage()
    {
        System.out.println("\nUsage: java JcsmpQueueResender [options]");
	System.out.println("");
        System.out.println("   where options are:");
        System.out.println("");
        System.out.println("  -url    	<URL>			- Solace broker URL (default localhost:55555)");
        System.out.println("  -username <Username>		- Solace broker usnername (default admin)");
        System.out.println("  -password	<Password>		- Solace broker password (default admin)");
        System.out.println("  -vpn	<VPN Name>		- Solace broker VPN name (default: default )");
        System.out.println("  -fromQ	<Queue Name>		- Queue to read from (default #DEAD_MSG_QUEUE)");
        System.out.println("  -toQ	<Queue Name>		- Queue to re-send to");
        System.out.println("  -count	<Number of messages>	- Number of messages to read and re-send (default 1)");
        System.out.println("  -msgTTL	<msecs>			- Set TimeToLive in millisecs on messages resent (default 0 - no expiry)");
        System.out.println("  -msgDMQ	<true|false>		- Set DMQ Eligible on messages resent (default true)");
        System.out.println("  -force				- Force resend, ignoring any warnings");
        System.out.println("  -nop				- Force rollback, do not commit transaction");
        System.exit(0);
    }

    protected String m_url = "localhost:55555";
    protected String m_username = "admin";
    protected String m_password = "admin";
    protected String m_vpn  =  "default";
    protected String m_fromQName  =  "#DEAD_MSG_QUEUE";
    protected String m_toQName  =  null;
    protected boolean m_force = false;
    protected boolean m_nop = false;
    protected boolean m_msgDMQ = true;
    protected long m_msgTTL = 0;
    protected int m_count = 1;


    protected JCSMPSession m_session = null;
    protected TransactedSession m_txSession = null;
    protected FlowReceiver m_conFlow = null;
    protected XMLMessageProducer m_producer = null;

    protected Queue m_fromQ = null;
    protected Queue m_toQ = null;
 
    protected static final Logger log = Logger.getLogger(JcsmpQueueResender.class);
 

    public JcsmpQueueResender()
    {
    }

    public void init()
    {
	try
	{
	    // create session (connection to broker)
	    JCSMPProperties properties = new JCSMPProperties();
	    properties.setProperty(JCSMPProperties.HOST, m_url);
	    properties.setProperty(JCSMPProperties.USERNAME, m_username);
	    properties.setProperty(JCSMPProperties.PASSWORD, m_password);
	    properties.setProperty(JCSMPProperties.VPN_NAME,  m_vpn);
	    properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false);  // enables the use of smfs://  without specifying a trust store


	    // Override the default reconnect params
	    JCSMPChannelProperties cp = (JCSMPChannelProperties) properties
		    .getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
	        
	    // Set number of reconnect retries (default is 3)
	    cp.setReconnectRetries(120);

	    // Set number of reconnect retry wait (default is 3000 millis)
	    cp.setReconnectRetryWaitInMillis(5000);

	    log.info("init: creating transacted session to broker: "+m_url);

	    m_session = JCSMPFactory.onlyInstance().createSession(properties);
	    m_session.connect();

	    m_txSession = m_session.createTransactedSession();


	    m_fromQ = JCSMPFactory.onlyInstance().createQueue(m_fromQName);
	    m_toQ = JCSMPFactory.onlyInstance().createQueue(m_toQName);

	    // bind to Q, blocking until we are the one and only active consumer

	    log.info("init: binding to from Q: "+m_fromQName);

	    ConsumerFlowProperties conFlowProps = new ConsumerFlowProperties();
	    conFlowProps.setEndpoint(m_fromQ);
	    conFlowProps.setStartState(true);
	    conFlowProps.setActiveFlowIndication(true);
	    EndpointProperties endpointProps = new EndpointProperties();
            endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
            endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
	    m_conFlow = m_txSession.createFlow(null, conFlowProps, endpointProps, new QueueEventHandler());

	    // Note; we cant create a producer in the transacted session until we get the default producer!!
	    m_session.getMessageProducer(new PublishEventHandler());

	    ProducerFlowProperties prodFlowProps = new ProducerFlowProperties();
            m_producer = m_txSession.createProducer(prodFlowProps, new PublishEventHandler());
 

	}
	catch(JCSMPException ie)
	{
	    log.error("init: Exception: "+ie.getMessage());
	    cleanUp();
	    System.exit(0);
	}
    }
    public void run()
    {
	try
	{
	    for (int i=0;i < m_count; i++)
	    {
		log.info("run: Reading message "+(i+1)+" from Q: "+m_fromQName);

		BytesXMLMessage msg = m_conFlow.receiveNoWait();

		if (msg == null)
		{
		    log.error("run: No message read from Q, rolling back transaction.. ");
		    cleanUp();
		    System.exit(0);
		}

		// If the message was sent directly to a queue check this is same as the queue to re-send to.
		// If the message was sent to a topic we cant check which queue this message came from..

		if (msg.getDestination() instanceof Queue)
		{
		    String origDestName = msg.getDestination().getName();
		    if (!origDestName.equals(m_toQName))
		    {
			if (!m_force)
			{
			    log.error("run: Original queue: "+origDestName+" differs to re-send queue: "+m_toQName+" rolling back transaction (use -force option to override");
			    cleanUp();
			    System.exit(0);
			}
			else
			{
			    log.warn("run: Original queue: "+origDestName+" differs to re-send queue: "+m_toQName+" forcing send to specified re-send queue");
			}
		    }
		}

		log.debug("Message dump:\n"+ msg.dump());

		log.info("run: Sending message "+(i+1)+" to queue: "+m_toQName);

		// Create clone of message to send
		BytesXMLMessage newMsg = JCSMPFactory.onlyInstance().createMessage(msg);
		newMsg.setDeliveryMode(DeliveryMode.PERSISTENT);

		// Note, if the messages are read from a DMQ, the DMQ Eligible flag will now be false, so reset if required.
		newMsg.setDMQEligible(m_msgDMQ);
		newMsg.setTimeToLive(m_msgTTL);

		// Send the message, note this does not reset the sender's timestamp, this will be as in the original message
		m_producer.send(newMsg, m_toQ);

	
	    }

	    if (m_nop)
	    {
		log.info("run: Resent "+m_count+" messages, NOP specified, rolling back transaction..");
		m_txSession.rollback();
	    }
	    else
	    {
		log.info("run: Resent "+m_count+" messages, committing transaction..");
		m_txSession.commit();
	    }

	    log.info("run: Operation successfully completed.");
	}
	catch(Exception ie)
	{
	    log.error("run: Exception: "+ie.toString());
	    log.info("run: Operation failed.");
	}

	cleanUp();
   }

    public class QueueEventHandler implements FlowEventHandler
    {
	public QueueEventHandler ()
	{
	}
	public void handleEvent(Object o, FlowEventArgs args)
	{
	    log.info("QueueEventHandler: "+ args.getEvent());

	    if (args.getEvent() != FlowEvent.FLOW_ACTIVE)
	    {
		log.error("QueueEventHandler: unexpected flow event for queue: "+m_fromQName+" rolling back transaction");
		cleanUp();
		System.exit(0);
	    }
	}
    }

    public class PublishEventHandler implements JCSMPStreamingPublishEventHandler 
    {

        // As TransactedSession does not have message acknowledgement, this method will not be called.
        public void handleError(String messageID, JCSMPException cause,
                long timestamp) {
	    log.warn("handleError: should not get here!");
            // Do Nothing
        }
        
        // As TransactedSession does not have message acknowledgement, this method will not be called.
        public void responseReceived(String messageID) {
	    log.warn("responseReceived: should not get here!");
            // Do Nothing
        }
    }
 


    public void cleanUp()
    {
	if (m_conFlow != null)
	    m_conFlow.close();
	m_conFlow = null;

	if (m_producer != null)
	    m_producer.close();
	m_producer = null;

	if (m_txSession != null)
	    m_txSession.close();

	if (m_session != null)
	    m_session.closeSession();
    }

    public static void main(String[] args) {
        JcsmpQueueResender r = new JcsmpQueueResender();
	try
	{
	    r.parseModuleArgs(args);
	    r.init();
	    r.run();
	}
	catch(Exception ie)
	{
	    log.error("Main: Exception: "+ie.getMessage());
	}
    }

    public int convertStringToInt(String value)
    {
        if(value != null && !value.equals(""))
	try
	{
	    return Integer.valueOf(value).intValue();
	}
	catch(NumberFormatException e)
	{   
	    log.warn("convertStringToInt: Exception: "+e.getMessage());
	}
        return 0;
    }


    public void parseModuleArgs(String[] args)
    {
	int i=0;

        while(i < args.length)
        {
            if (args[i].compareTo("-force")==0)
            {
                m_force = true;
		i += 1;
            }
            else
            if (args[i].compareTo("-nop")==0)
            {
                m_nop = true;
		i += 1;
            }
            else
            if (args[i].compareTo("-url")==0)
            {
                if ((i+1) >= args.length) usage();
                m_url = args[i+1];
		i += 2;
            }
            else
            if (args[i].compareTo("-username")==0)
            {
                if ((i+1) >= args.length) usage();
                m_username = args[i+1];
		i += 2;
            }
            else
            if (args[i].compareTo("-password")==0)
            {
                if ((i+1) >= args.length) usage();
                m_password = args[i+1];
		i += 2;
            }
            else
            if (args[i].compareTo("-vpn")==0)
            {
                if ((i+1) >= args.length) usage();
                m_vpn = args[i+1];
		i += 2;
            }
            else
            if (args[i].compareTo("-fromQ")==0)
            {
                if ((i+1) >= args.length) usage();
                m_fromQName = args[i+1];
		i += 2;
            }
            else
            if (args[i].compareTo("-toQ")==0)
            {
                if ((i+1) >= args.length) usage();
                m_toQName = args[i+1];
		i += 2;
            }
	    else
            if (args[i].compareTo("-msgDMQ")==0)
            {
                if ((i+1) >= args.length) usage();
                String b = args[i+1];
                if (b.equalsIgnoreCase("true"))
		    m_msgDMQ = true;
		else
                if (b.equalsIgnoreCase("false"))
		    m_msgDMQ = false;
		else
		    log.warn("parseModuleArgs: Invalid msgDMQ: "+args[i+1]+" using: true");
		i += 2;
            }
	    else
            if (args[i].compareTo("-msgTTL")==0)
            {
                if ((i+1) >= args.length) usage();
		int s = convertStringToInt(args[i+1]);
                if (s >= 0)
		    m_msgTTL = s;
		else
		    log.warn("parseModuleArgs: Invalid msgTTl: "+args[i+1]+" using: 0");
		i += 2;
            }
	    else
            if (args[i].compareTo("-count")==0)
            {
                if ((i+1) >= args.length) usage();
		int s = convertStringToInt(args[i+1]);
                if (s > 0)
		    m_count = s;
		else
		    log.warn("parseModuleArgs: Invalid count: "+args[i+1]+" using: 1");
		i += 2;
            }
	    else
            {
                usage();
            }
        }
	if (m_toQName == null)
	{
	    log.error("parseModuleArgs: -toQ must be specified");
	    usage();
	}
    }
}
