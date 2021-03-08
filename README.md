# Solace JCSMP Queue Resender

JCSMP example to read messages from one Solace PubSub+ queue (e.g. a Dead Message Queue) and re-send them to a different queue

The tool uses a transacted session, if all messages cannot be read/sent or an error is encountered the transaction is rolled back.

When resending messages from a DMQ, if the messages read were sent directly to a queue this original queue is checked that it matches the
configured -toQ, if the queue names dont match the operation is rolled back (unless the -force option is specified).
If the read messages were attracted to its original queue via a topic subscription it's not possible to check the original queue,
so it's up to the user to ensure the -toQ parameter is correct.

CAUTION: Use of this tool is at user's own risk. It is recommended to run the tool with the -nop option to check behaviour first.

Note; logging maybe set in log4j.properties, with debug enabled the content of read messages is logged.


## Building
```
mvn clean dependency:copy-dependencies package
```

## Running

Usage: java JcsmpQueueResender [options]

Where options are:

```
	-url    	<URL>			- Solace broker URL (default localhost:55555)
	-username 	<Username>		- Solace broker usnername (default admin)
	-password	<Password>		- Solace broker password (default admin)
	-vpn		<VPN Name>		- Solace broker VPN name (default: default)
	-fromQ		<Queue Name>		- Queue to read from (default #DEAD_MSG_QUEUE)
	-toQ		<Queue Name>		- Queue to re-send to
	-count		<Number of messages>	- Number of messages to read and re-send (default 1)
	-msgTTL		<msecs>			- Set TimeToLive in millisecs on messages resent (default 0 - no expiry)
	-msgDMQ		<true|false>		- Set DMQ Eligible on messages resent (default true)
	-force					- Force resend, ignoring any warnings
	-nop					- Force rollback, do not commit transaction
```
